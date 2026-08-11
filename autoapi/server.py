"""
FastAPI 服务层模块喵~

职责：把 HTTP 世界和编排层对接起来 —— 收下客户端请求、扒出原始要素交给 proxy，
     再把 proxy 给出的 ProxyOutcome 转成真正的 HTTP 响应喵。

三个路由：
    GET  /healthz            健康检查，顺手报告当前有几个候选在冻结中
    GET  /v1/models          列出所有虚拟模型（很多客户端启动时会拉这个列表来填下拉框）
    ANY  /{full_path:path}   通配透传路由，真正干活的就是它

并发能力：
    httpx 的连接池在 lifespan 里创建、全程复用，池子开到 200 条keep-alive 连接。
    每条请求是一个协程，60rpm（也就是 1 req/s）对异步模型来说非常轻松，真正占资源的是
    「很多条流式长连接同时挂着」，而这恰好是 asyncio 最擅长的场景喵。

边界条件：通配路由只接受带 JSON 体的方法（POST/PUT/PATCH），GET/DELETE 之类没有 model
        字段可读，会被 proxy 判成 400 并给出明确提示喵。
"""

# 引入注解特性喵
from __future__ import annotations

# asyncio 用来跑配置热重载的后台任务喵
import asyncio
# logging 用来输出服务层日志喵
import logging
# time 用来记录流式请求完整生命周期的总时长喵
import time
# contextlib 的 asynccontextmanager 用来写 FastAPI 的 lifespan 喵
from contextlib import asynccontextmanager
# pathlib 用于热重载时检查配置文件的修改时间喵
from pathlib import Path

# httpx 提供异步客户端喵
import httpx
# FastAPI 相关的类型和响应类喵
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response, StreamingResponse

# 引入配置加载相关喵
from .config import AppConfig, ConfigError, load_config
# 引入编排层喵
from .proxy import handle_request, STATUS_DROP_CONNECTION
# 引入运行时状态喵
from .state import RuntimeState
# 引入把流式结果变成字节流的工具喵
from .upstream import iter_upstream_bytes

# 服务层的日志器喵
logger = logging.getLogger("autoapi.server")


# 热重载被关掉（reload_poll_interval 设为 0）时，每隔这么多秒回头看一眼这个开关有没有
# 被重新打开，单位：秒。这样主人在 REPL 里敲 set reload_poll_interval 2 就能当场启用，
# 不用重启代理喵
DISABLED_RECHECK_SECONDS = 5.0


async def _config_reload_loop(state: RuntimeState, path: Path) -> None:
    """
    配置文件热重载的后台任务喵~

    思路：每隔一段时间看一眼配置文件的修改时间，变了就重新加载并整体替换掉内存里的配置。
         用轮询 mtime 而不是引入 watchdog 依赖，是因为这个需求太简单了，多一个 C 扩展
         依赖不值得，而且轮询在 Windows 上行为更可预测喵。

    关键点：轮询间隔是每一轮都从 state 现读的，不是启动时定死的参数。这样主人改了
           reload_poll_interval（无论是手改文件还是在 REPL 里 set）都会当场生效；
           而且设成 0 关掉热重载之后，这个任务并不退出，只是转成每 5 秒瞄一眼开关，
           所以之后还能再打开，不需要重启代理喵。

    边界条件：新配置有语法错或校验不通过时，保留旧配置继续服务、只打一条错误日志。
            绝不能因为编辑器保存了一个写坏的配置就让整个代理停摆喵。
    """
    # 记下上次看到的修改时间，初始为 0 表示还没看过喵
    last_mtime = 0.0
    # 首次进来先取一次当前修改时间，避免启动瞬间白重载一次喵
    try:
        last_mtime = path.stat().st_mtime
    # 喵~防御：取不到就保持 0，下一轮循环会当成「变了」而重载一次，无害喵
    except OSError:
        pass
    # 无限循环，直到任务被取消（进程退出时 lifespan 会取消它）喵
    while True:
        # 每一轮都现读间隔配置，所以改了这个值当场就生效喵
        interval = state.config.server.reload_poll_interval
        # 间隔为 0 表示主人关掉了热重载喵
        if interval <= 0:
            # 睡一会儿再回来看这个开关有没有被重新打开，期间不碰配置文件喵
            await asyncio.sleep(DISABLED_RECHECK_SECONDS)
            # 关掉期间文件可能被改过，把基准时间同步成当前值，
            # 这样重新打开后不会立刻触发一次「补重载」，语义更符合直觉喵
            try:
                last_mtime = path.stat().st_mtime
            # 喵~防御：取不到就保持原值，无害喵
            except OSError:
                pass
            # 回到循环顶部继续等喵
            continue
        # 先睡够间隔时间，睡眠期间不占任何 CPU 喵
        # 喵~防御：间隔压一个 0.5 秒的下限，防止被配成 0.001 导致每秒疯狂 stat 磁盘喵
        await asyncio.sleep(max(0.5, interval))
        # 取当前修改时间喵
        try:
            mtime = path.stat().st_mtime
        # 喵~防御：文件被临时删掉或改名时跳过这一轮，等它回来再说喵
        except OSError:
            continue
        # 修改时间没变说明文件没动，什么都不做喵
        if mtime == last_mtime:
            continue
        # 先更新记录的时间，这样即使下面加载失败也不会每轮都重试同一个坏文件喵
        last_mtime = mtime
        # 尝试加载新配置喵
        try:
            new_config = load_config(path)
        # 喵~防御：新配置有问题时保留旧配置继续服务，只记错误日志喵
        except ConfigError as exc:
            logger.error("配置热重载失败，继续使用旧配置喵：%s", exc)
            continue
        # 加载成功，整体替换掉内存里的配置喵
        state.replace_config(new_config)
        # 记一条成功日志，写明现在有几个虚拟模型几条规则喵
        logger.info(
            "配置已热重载喵：%d 个虚拟模型，%d 条规则",
            len(new_config.virtual_models),
            len(new_config.rules),
        )
        # 喵~防御：新配置里如果用了已退役的配置项，重载时也要提醒一次。
        # 只在启动时提醒是不够的 —— 主人很可能是在运行中编辑配置时才写进去的喵
        for warning in new_config.warnings:
            logger.warning("配置提醒喵：%s", warning)


def create_app(state: RuntimeState) -> FastAPI:
    """
    创建 FastAPI 应用喵~

    输入：已经装好初始配置的运行时状态
    输出：配置好路由和生命周期的 FastAPI 应用
    """

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        """管理 httpx 客户端和热重载任务的生命周期喵~"""
        # 取当前配置，用来读连接池设置和热重载开关喵
        config = state.config
        # 这里给客户端设的超时只是个兜底默认值喵。
        # 真正生效的超时是每条请求在 upstream.py 里现算现传的（见 build_timeout），
        # 这样主人在运行中改了超时配置、或者给某个节点配了专属超时，都能当场生效，
        # 而不会被这个启动时定死的值压住喵。
        timeout = httpx.Timeout(
            # 兜底用两个总预算里更大的那个，免得这个默认值反而成了最紧的约束喵
            timeout=max(config.server.stream_timeout, config.server.nonstream_timeout),
            # 建立连接的超时单独设置，连不上要快速失败好换下一个候选喵
            connect=config.server.connect_timeout,
        )
        # 配置连接池上限，支撑多条并发长连接喵
        limits = httpx.Limits(
            # 总连接数上限，60rpm 场景下远远够用，留足余量给突发喵
            max_connections=200,
            # 保持活跃的连接数，复用连接能省掉每次的 TLS 握手，显著降延迟喵
            max_keepalive_connections=50,
        )
        # 创建全程复用的异步客户端喵
        # follow_redirects 打开，因为部分中转站会用 301 把 /v1 重定向到实际路径喵
        async with httpx.AsyncClient(timeout=timeout, limits=limits, follow_redirects=True) as client:
            # 把客户端挂到 app.state 上，路由函数通过它取用喵
            app.state.http_client = client
            # 热重载任务的句柄，默认没有喵
            reload_task: asyncio.Task | None = None
            # 只要知道配置文件在哪就把这个任务起起来喵。
            # 注意这里不再判断 reload_poll_interval 是否大于 0 —— 那个判断挪进循环内部了，
            # 因为如果在这里判断，启动时设成 0 的话任务压根不起，之后主人就再也没法
            # 在不重启的情况下打开热重载了喵。
            if config.source_path is not None:
                # 创建后台任务，间隔由它自己每轮现读喵
                reload_task = asyncio.create_task(_config_reload_loop(state, config.source_path))
            # 到这里服务就绪，把控制权交给 uvicorn 开始接请求喵
            try:
                yield
            # 退出时清理后台任务喵
            finally:
                # 有热重载任务就取消它喵
                if reload_task is not None:
                    # 发出取消信号喵
                    reload_task.cancel()
                    # 喵~防御：等它真正结束，并吞掉预期内的 CancelledError 喵
                    try:
                        await reload_task
                    except asyncio.CancelledError:
                        pass

    # 创建应用实例，关掉自动生成的文档路由以免和通配路由抢路径喵
    app = FastAPI(
        # 应用标题喵
        title="autoapi 故障转移代理",
        # 关掉 Swagger UI，通配路由会把 /docs 也吃掉，留着反而混淆喵
        docs_url=None,
        # 关掉 ReDoc，同理喵
        redoc_url=None,
        # 关掉 OpenAPI schema 端点喵
        openapi_url=None,
        # 绑定上面定义的生命周期管理器喵
        lifespan=lifespan,
    )
    # 把路由注册上去喵
    _register_routes(app, state)
    # 返回应用喵
    return app


def _register_routes(app: FastAPI, state: RuntimeState) -> None:
    """
    注册所有路由喵~

    注册顺序很重要：FastAPI 按注册顺序匹配路由，所以 /healthz 和 /v1/models 这两个
    具体路径必须先注册，否则会被后面的通配路由抢先吃掉喵。
    """

    @app.get("/healthz")
    async def healthz() -> JSONResponse:
        """健康检查，顺手报告冻结情况喵~"""
        # 取当前冻结列表喵
        freezes = state.list_freezes()
        # 返回一个简单的状态摘要喵
        return JSONResponse(
            {
                # 服务活着喵
                "status": "ok",
                # 当前配了几个虚拟模型喵
                "virtual_models": len(state.list_virtual_models()),
                # 当前有几个候选在冻结中喵
                "frozen_candidates": len(freezes),
                # 累计处理了多少条请求喵
                "total_requests": state.total_requests,
                # 累计有多少条请求把整条链都用尽了喵
                "total_exhausted": state.total_exhausted,
            }
        )

    @app.get("/v1/models")
    async def list_models() -> JSONResponse:
        """
        列出所有虚拟模型喵~

        为什么需要这个接口：绝大多数客户端（各种 GUI 客户端、SDK）启动时会先拉一次
        /v1/models 来填模型下拉框。如果让它落进通配路由，会因为 GET 请求没有 body、
        取不到 model 字段而回 400，客户端就会显示「连接失败」。所以单独实现，
        直接把虚拟模型表当成模型列表返回，格式对齐 OpenAI 的规范喵。
        """
        # 按 OpenAI 的 /v1/models 响应格式组装喵
        return JSONResponse(
            {
                # 固定的列表类型标记喵
                "object": "list",
                # 每个虚拟模型转成一个模型条目喵
                "data": [
                    {
                        # 模型 id 就是虚拟模型名，客户端之后会用它当 model 参数喵
                        "id": name,
                        # 固定的对象类型标记喵
                        "object": "model",
                        # 归属方写成 autoapi，表明这是代理提供的虚拟模型喵
                        "owned_by": "autoapi",
                    }
                    # 遍历所有虚拟模型名喵
                    for name in state.list_virtual_models()
                ],
            }
        )

    @app.api_route("/{full_path:path}", methods=["POST", "PUT", "PATCH", "GET", "DELETE"])
    async def catch_all(request: Request, full_path: str) -> Response:
        """
        通配透传路由，真正干活的就是它喵~

        客户端的路径、查询串、请求头、请求体全部原样往上游转发，只有三样东西会被替换：
        base_url、api_key、以及请求体顶层的 model 字段喵。
        """
        # 记录服务端收到客户端请求的时刻，平均耗时覆盖完整服务端生命周期喵
        request_started_at = time.monotonic()
        # 把客户端的请求体完整读出来，之后要解析出虚拟模型名喵
        raw_body = await request.body()
        # 把请求头转成普通字典，交给编排层处理喵
        headers = dict(request.headers)
        # 交给编排层走完整条候选链喵
        outcome = await handle_request(
            # 复用的 HTTP 客户端喵
            request.app.state.http_client,
            # 运行时状态喵
            state,
            # 客户端原始请求方法喵
            request.method,
            # 客户端原始请求路径喵
            full_path,
            # 客户端原始查询串喵
            str(request.url.query or ""),
            # 客户端原始请求头喵
            headers,
            # 客户端原始请求体喵
            raw_body,
            # 服务端收到客户端请求的起始时刻喵
            request_started_at,
        )
        # 喵~防御：目标模式超时且配置为断开连接时，直接抛异常中断响应喵
        if not outcome.success and outcome.status == STATUS_DROP_CONNECTION:
            logger.warning("目标模式超时，断开连接不返回响应喵")
            raise RuntimeError("目标模式超时，主动断开连接喵")
        # 编排失败（400 或 502），把错误体作为 JSON 回给客户端喵
        if not outcome.success or outcome.attempt is None:
            return JSONResponse(outcome.error_body, status_code=outcome.status)
        # 取出这次成功（或 passthrough）的尝试结果喵
        attempt = outcome.attempt
        # 流式路径：用 StreamingResponse 边收边吹给客户端喵
        if outcome.is_stream:
            # 复制一份上游响应头，再补上几个流式必需的头喵
            stream_headers = dict(attempt.headers)
            # 把流式总时长、尾包 usage 与 TPM 补写收拢到生成器结束后的 finally 喵
            async def observed_stream():
                # 记录从服务端收到客户端请求到流响应生命周期结束的全程耗时喵
                stream_started_at = request_started_at
                # 只有上游迭代器自然耗尽才算请求正常完成喵
                stream_completed_normally = False
                try:
                    # 逐块透传并观察尾包 usage 喵
                    async for chunk in iter_upstream_bytes(attempt):
                        yield chunk
                    # 只有上游字节迭代器自然结束才允许进入成功耗时统计喵
                    stream_completed_normally = attempt.stream_completed_normally
                finally:
                    # 无论流如何结束都计算客户端视角的完整生命周期耗时供日志使用喵
                    total_ms = (time.monotonic() - stream_started_at) * 1000
                    # 流结束后统一创建 RPM/TPM 事件，保持既有成功流速率统计语义喵
                    if attempt.virtual_model is not None:
                        attempt.rate_event = state.record_rate_event(
                            attempt.virtual_model,
                            attempt.usage_tokens,
                        )
                    # 只有正常结束的流才补写完整耗时，异常流不污染平均值喵
                    if stream_completed_normally and attempt.rate_event is not None:
                        state.attach_elapsed_ms(attempt.rate_event, total_ms)
                    logger.info(
                        "流式请求结束 虚拟模型=%s 返回请求耗时=%.0fms 正常完成=%s usage_tokens=%s 喵",
                        attempt.virtual_model or "未知",
                        total_ms,
                        stream_completed_normally,
                        attempt.usage_tokens if attempt.usage_tokens is not None else "未上报",
                    )
                    # 尾包 usage 已在 iterator 结束时观察完成，统计事件直接使用最终值喵
                    # 没有 usage 时保持 None，正常 RPM 已记录但 TPM 仍显示未完整上报喵
            # 禁止缓存，否则中间层可能把 SSE 缓存起来导致客户端收不到增量喵
            stream_headers["Cache-Control"] = "no-cache"
            # 关掉 nginx 一类反向代理的缓冲，不加这个头会导致流被攒成一大坨才下发喵
            stream_headers["X-Accel-Buffering"] = "no"
            # 喵~防御：content-type 由 media_type 参数单独指定，headers 里留着会重复设置喵
            stream_headers.pop("content-type", None)
            stream_headers.pop("Content-Type", None)
            # 返回流式响应，字节来源是「已缓冲前缀 + 上游后续字节」喵
            return StreamingResponse(
                # 字节生成器喵
                observed_stream(),
                # 状态码固定 200，因为只有 200 才会走到这里喵
                status_code=200,
                # 过滤后的上游响应头喵
                headers=stream_headers,
                # SSE 的标准 content-type，上游没给就用这个兜底喵
                media_type=attempt.media_type or "text/event-stream",
            )
        # 非流式路径：响应体已经完整读出来了，直接回喵
        body_headers = dict(attempt.headers)
        # 喵~防御：content-type 交给 media_type 参数处理，避免重复设置头喵
        body_headers.pop("content-type", None)
        body_headers.pop("Content-Type", None)
        # 返回完整响应，状态码用上游的真实状态码（passthrough 时可能是 400）喵
        return Response(
            # 上游的完整响应体，取不到时用空字节兜底喵
            content=attempt.body or b"",
            # 上游的真实状态码喵
            status_code=attempt.status,
            # 过滤后的上游响应头喵
            headers=body_headers,
            # 上游声明的 content-type 喵
            media_type=attempt.media_type,
        )
