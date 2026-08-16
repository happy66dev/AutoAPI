"""
候选链编排模块喵~

这是把所有零件串起来的核心，一条客户端请求进来之后的完整旅程都在这里喵：

    1. 从请求体里取出虚拟模型名，查候选链；查不到就回 400 并列出可用的虚拟模型喵
    2. 按严格优先级从链首往下走，跳过还在冻结中的候选喵
    3. 对每个候选调用 upstream.try_candidate 打一次
    4. 成功就立刻返回（流式的话把连接交给客户端继续读）
    5. 失败就问规则引擎该怎么办：
         retry       原地退避重试，次数用尽才换下一个
         next        立刻换下一个
         freeze      全局冻结这个候选，然后换下一个
         passthrough 不再转移，把上游的响应原样回传给客户端
    6. 整条链都走完还没成功，回 502 并附上每个候选各自的失败原因喵

设计要点：严格优先级意味着「链首就是你最想用的那个」，只有它挂了才降级。配合冻结机制，
        撞了额度墙的候选会被自动跳过一段时间，所以不会每条请求都去撞一次墙喵。
"""

# 引入注解特性喵
from __future__ import annotations

# asyncio 用来做退避等待喵
import asyncio
# time 用来记录请求性能耗时，使用单调时钟避免系统时间变化影响结果喵
import time
# json 用来解析客户端请求体喵
import json
# logging 用来输出转发过程的日志喵
import logging
# secrets 用来生成请求 ID，用它而不是 random 是因为它天生线程安全喵
import secrets
# dataclass 用来定义编排结果容器喵
from dataclasses import dataclass, field
# Any 用于标注解析出来的请求体喵
from typing import Any

# httpx 提供复用的异步客户端类型喵
import httpx

# 引入候选和服务器配置类型喵
from .config import Candidate, ServerConfig
# 引入规则引擎喵
from .rules import decide
# 引入运行时状态喵
from .state import RuntimeState
# 引入上游调用相关的东西喵
from .upstream import AttemptResult, try_candidate

# 本模块的日志器，名字会显示在日志行里方便定位喵
logger = logging.getLogger("autoapi.proxy")


def _is_context_limit_error(status: int, error_text: str) -> bool:
    """判断是否为上游报告的上下文超限错误喵~"""
    # 只把明确的 400 上下文窗口错误视为用户侧问题喵
    if status != 400:
        return False
    # 兼容常见 OpenAI、Anthropic 和中转站错误关键词喵
    normalized_error = error_text.lower()
    return any(
        marker in normalized_error
        for marker in ("context_length_exceeded", "maximum context length", "context window")
    )


# count_tokens 只做 token 计数，不代表 LLM 输出能力喵~
def _is_ignored_error_endpoint(server: ServerConfig, method: str, path: str) -> bool:
    """判断请求是否命中配置的接口错误忽略列表喵~"""
    # 统一方法大小写和首尾空白，与配置解析后的格式保持一致喵
    normalized_method = method.strip().upper()
    # 只去掉多余前导斜杠，保留尾斜杠以实现路径精确匹配喵
    normalized_path = "/" + path.strip().lstrip("/")
    # method 与 path 必须同时存在于配置集合中喵
    return (normalized_method, normalized_path) in server.ignored_error_endpoints


def _is_count_tokens_request(method: str, path: str) -> bool:
    """判断请求是否为默认的 Anthropic token 计数接口喵~"""
    # 使用旧特例的标准化方式保留测试和内部调用兼容性喵
    normalized_method = method.upper().strip()
    # 保留尾斜杠差异以符合接口精确匹配语义喵
    normalized_path = "/" + path.strip().lstrip("/")
    # 仅精确匹配 POST /v1/messages/count_tokens，避免误伤其他接口喵
    return normalized_method == "POST" and normalized_path == "/v1/messages/count_tokens"


@dataclass
class ProxyOutcome:
    """
    编排的最终结果，交给 server 模块转成真正的 HTTP 响应喵~

    三种形态：
        success=True 且 attempt 是流式  → server 用 StreamingResponse 往下吹
        success=True 且 attempt 是非流式 → server 用 Response 直接回
        success=False                  → server 按 status 和 error_body 回错误
    """

    # 整体是否成功喵
    success: bool
    # 成功时（或 passthrough 时）对应的那次尝试结果喵
    attempt: AttemptResult | None = None
    # 失败时要回给客户端的状态码喵
    status: int = 502
    # 失败时要回给客户端的错误体（已经是 JSON 可序列化的字典）喵
    error_body: dict[str, Any] = field(default_factory=dict)
    # 是不是流式请求，server 据此决定用哪种 Response 类喵
    is_stream: bool = False


def parse_client_body(raw: bytes) -> dict[str, Any]:
    """
    把客户端的原始请求体解析成字典喵~

    输入：原始请求体字节
    输出：解析后的字典
    边界条件：空请求体、非 JSON、JSON 顶层不是字典，全部抛 ValueError 并带中文说明，
            因为这三种情况下我们都无法知道客户端想用哪个虚拟模型喵。
    """
    # 喵~防御：空请求体没法取 model 字段，直接报错让客户端知道喵
    if not raw:
        raise ValueError("请求体为空，无法确定要使用哪个虚拟模型喵")
    # 尝试解析 JSON 喵
    try:
        obj = json.loads(raw.decode("utf-8", errors="replace"))
    # 喵~防御：JSON 语法错误时给出明确说明喵
    except (json.JSONDecodeError, ValueError) as exc:
        raise ValueError(f"请求体不是合法的 JSON：{exc} 喵") from exc
    # 喵~防御：顶层必须是字典，数组或裸值都不符合两种协议的规范喵
    if not isinstance(obj, dict):
        raise ValueError("请求体的 JSON 顶层必须是对象喵")
    # 返回解析结果喵
    return obj


def detect_stream_flag(body_obj: dict[str, Any]) -> bool:
    """
    判断这是不是一个流式请求喵~

    两种协议都用请求体顶层的 stream 字段表示，所以看这一个字段就够了。
    只有显式为布尔真值才算流式，字符串 "true" 这种非标准写法也一并兼容喵。
    """
    # 取出 stream 字段喵
    value = body_obj.get("stream")
    # 标准写法：布尔值喵
    if isinstance(value, bool):
        return value
    # 喵~防御：兼容个别客户端把 stream 写成字符串 "true" 的情况喵
    if isinstance(value, str):
        return value.strip().lower() == "true"
    # 其他情况（缺失、None、数字）都按非流式处理喵
    return False


def _pick_usable_candidates(
    state: RuntimeState,
    chain: list[Candidate],
) -> tuple[list[Candidate], list[str]]:
    """
    从候选链里挑出当前可用的候选喵~

    输入：运行时状态、完整的候选链
    输出：(可用候选列表, 被跳过的候选说明列表)
    说明：严格保持原始顺序，只是把还在冻结中的滤掉。被跳过的会记下剩余冻结秒数，
         万一整条链都不可用，这些说明会出现在 502 的错误体里，方便排查喵。
    """
    # 可用候选列表喵
    usable: list[Candidate] = []
    # 被跳过的候选说明列表喵
    skipped: list[str] = []
    # 按原顺序遍历整条链喵
    for candidate in chain:
        # 查这个候选还剩多少秒冻结，0 表示可用喵
        remaining = state.is_frozen(candidate)
        # 还在冻结中就跳过，并记下原因喵
        if remaining > 0:
            skipped.append(f"{candidate.label} 冻结中，还剩 {remaining:.0f} 秒")
        # 没被冻结就收进可用列表喵
        else:
            usable.append(candidate)
    # 返回两个列表喵
    return usable, skipped


def new_request_id() -> str:
    """
    给每条客户端请求生成一个短 ID 喵~

    为什么需要：并发时多条请求的日志会交错在一起，光看「失败了、换候选了」根本认不出
    哪几行属于同一条请求。给每行日志都带上同一个 ID，就能用它把一条请求的完整旅程
    grep 出来了喵。

    用 6 位十六进制（约 1600 万种），同一时刻在飞的请求撞 ID 的概率低到可以忽略，
    而且短到不会把日志行撑长喵。
    """
    # 取 3 个随机字节转成 6 位十六进制喵
    return secrets.token_hex(3)


async def _run_one_candidate(
    client: httpx.AsyncClient,
    state: RuntimeState,
    candidate: Candidate,
    method: str,
    path: str,
    query: str,
    headers: dict[str, str],
    body_obj: dict[str, Any],
    is_stream: bool,
    req_id: str,
    virtual_model: str,
    request_started_at: float,
    ignored_error_endpoint: bool,
) -> tuple[str, AttemptResult]:
    """
    在一个候选上尝试到底（含该候选内部的退避重试）喵~

    输入：一个候选和这次请求的全部要素
    输出：(结论, 最后一次的尝试结果)，结论取值：
            ok          成功了，可以返回给客户端
            next        这个候选不行，请换下一个
            passthrough 规则要求把上游响应原样回传，不要再转移
    副作用：会更新统计，必要时把候选写进冻结表喵
    """
    # 取一份当前配置快照，整个候选的尝试过程都用这一份，避免中途热重载导致行为不一致喵
    config = state.config
    # 当前是第几次尝试，从 1 开始数喵
    attempt_no = 1
    # 无限循环，靠内部的 return 退出；退避重试就在这个循环里打转喵
    while True:
        # 每次 try_candidate 都代表一次真实发往上游的尝试，先记下其统一起始时刻喵
        attempt_started_at = time.monotonic()
        # 用这个候选打一次上游喵
        result = await try_candidate(
            # 复用的 HTTP 客户端喵
            client,
            # 当前候选喵
            candidate,
            # 原样转发的方法喵
            method,
            # 原样转发的路径喵
            path,
            # 原样转发的查询串喵
            query,
            # 客户端原始请求头喵
            headers,
            # 客户端原始请求体喵
            body_obj,
            # 是否流式喵
            is_stream,
            # 超时配置喵
            config.server,
            # 是否抑制上游模块针对忽略接口的假成功 warning 喵
            suppress_error_warnings=ignored_error_endpoint,
        )
        # 成功了，记一笔成功（顺带会自动解冻这个候选）然后返回喵
        if result.ok:
            # 流式此刻只有探测成功，终态资源事件留给 server 消费完整流时写入喵
            if not is_stream:
                # 非流完整响应已读完，耗时从上游请求起点计算喵
                upstream_elapsed_ms = (
                    (time.monotonic() - result.started_at) * 1000
                    if result.started_at is not None
                    else (time.monotonic() - attempt_started_at) * 1000
                )
                # 每次非流成功尝试只写一条候选资源终态事件喵
                state.record_candidate_health(
                    candidate,
                    True,
                    result.usage_tokens,
                    upstream_elapsed_ms,
                    result.input_tokens,
                    result.cached_tokens,
                    result.started_at or attempt_started_at,
                )
            # 更新统计并解冻喵
            state.record_success(
                candidate,
                result.usage_tokens,
                (time.monotonic() - request_started_at) * 1000 if not is_stream else None,
            )
            # 非流式完整请求按虚拟模型记录一次最终成功喵
            if not is_stream and not ignored_error_endpoint:
                state.record_virtual_model_health(
                    virtual_model,
                    True,
                    result.usage_tokens,
                    (time.monotonic() - request_started_at) * 1000,
                    result.input_tokens,
                    result.cached_tokens,
                )
            # 记录 RPM/TPM 事件：非流式完整响应已经结束，立即统一上报喵
            # 流式必须等整条流结束后再由 server 统一上报，放行时不能提前增加 RPM 喵
            if not is_stream:
                result.rate_event = state.record_rate_event(
                    virtual_model,
                    result.usage_tokens,
                    result.input_tokens,
                    result.cached_tokens,
                )
            # 保存虚拟模型名，流结束时 server 需要用它补写尾包 usage 喵
            result.virtual_model = virtual_model
            # 非流式请求此刻已经完整结束，立即补上完整耗时；流式要等生成器结束后再补喵
            if not is_stream and result.rate_event is not None:
                state.attach_elapsed_ms(result.rate_event, (time.monotonic() - request_started_at) * 1000)
            # 计算从服务端接收客户端请求到成功响应准备完成的全程耗时喵
            elapsed_ms = (time.monotonic() - request_started_at) * 1000
            # 流式在此刻只是确认健康并放行，完整总时长要等流结束后才知道喵
            if is_stream:
                # 计算从本次上游节点请求开始到第一个非空字节到达的节点耗时喵
                first_byte_ms = (
                    (result.first_byte_at - result.started_at) * 1000
                    if result.first_byte_at is not None and result.started_at is not None
                    else 0.0
                )
                logger.info(
                    "[%s] 成功 %s（第 %d 次尝试）喵 流 首字=%.0fms 请求首字=%.0fms",
                    req_id, candidate.name, attempt_no, first_byte_ms, elapsed_ms,
                )
            # 非流式此时完整响应已读完，记录服务端返回响应前的全程耗时喵
            else:
                logger.info(
                    "[%s] 成功 %s（第 %d 次尝试）喵 非流 返回请求耗时=%.0fms",
                    req_id, candidate.name, attempt_no, elapsed_ms,
                )
            # 返回成功结论喵
            return "ok", result
        # 记录候选失败，但上下文超限属于用户侧问题，不计入上游模型错误统计喵
        # 失败尝试仍属于真实上游调用，因此保留一条不计平均耗时的候选资源事件喵
        state.record_candidate_health(
            candidate,
            False,
            result.usage_tokens,
            None,
            result.input_tokens,
            result.cached_tokens,
            result.started_at or attempt_started_at,
        )
        # 记录候选失败，但上下文超限属于用户侧问题，不计入上游模型错误统计喵
        if _is_context_limit_error(result.status, result.error_text):
            hedge_hits = 0
        # 被忽略接口不参与自动避险失败累计，避免接口缺失冻结正常候选喵
        elif ignored_error_endpoint:
            hedge_hits = 0
        # 普通接口继续按原逻辑累计自动避险失败喵
        else:
            # 阈值从配置里取，所以主人改了 auto_hedge_threshold 能立即生效喵
            hedge_hits = state.record_failure(
                candidate, result.error_text, config.server.auto_hedge_threshold,
                count_health=not _is_context_limit_error(result.status, result.error_text),
            )
        # 达到连续失败阈值，自动把这个节点冻结起来避险喵。
        # 注意这一步和下面规则引擎的决策是独立的两件事：
        #   规则引擎决定「这一次请求接下来怎么走」（重试 / 换候选 / 回传）
        #   自动避险决定「这个节点接下来一段时间还要不要用」
        # 所以即使规则说「原地重试」，节点也可能因为攒够失败次数而被冻结，
        # 那么重试时它已经不可用、会自然跳到下一个候选，这正是我们想要的行为喵
        if hedge_hits > 0:
            # 把分钟换算成秒喵
            hedge_seconds = config.server.auto_hedge_minutes * 60.0
            # 写入冻结表，原因里写清楚是自动避险而不是上游告知的额度限制喵
            state.freeze(
                candidate,
                hedge_seconds,
                f"自动避险：连续失败 {hedge_hits} 次（最近一次：{result.error_text[:120]}）",
            )
            # 普通接口才输出自动避险警告，忽略接口保持静默喵
            if not ignored_error_endpoint:
                logger.warning(
                    "[%s] 自动避险 %s：连续失败 %d 次达到阈值，冻结 %.0f 分钟喵",
                    req_id,
                    candidate.label,
                    hedge_hits,
                    config.server.auto_hedge_minutes,
                )
        # 问规则引擎该怎么办喵
        decision = decide(config.rules, result.status, result.error_text, result.retry_after)
        # 普通接口输出候选失败警告，忽略接口不升级为 warning 喵
        if not ignored_error_endpoint:
            logger.warning(
                "[%s] 失败 %s 状态=%s 决策=%s 依据=%s 原因=%s 喵",
                req_id,
                candidate.label,
                result.status,
                decision.action,
                decision.matched_by,
                result.error_text[:200],
            )
        # 规则要求原样回传上游响应，不再做任何转移喵
        if decision.action == "passthrough":
            return "passthrough", result
        # 规则要求冻结这个候选喵
        if decision.action == "freeze":
            # 写入冻结表，时长由规则引擎算好（可能是从上游消息里抽出来的）喵
            state.freeze(candidate, decision.freeze_seconds, result.error_text)
            # 规则冻结动作仍然执行，但忽略接口不输出候选级 warning 喵
            if not ignored_error_endpoint:
                logger.warning(
                    "[%s] 冻结 %s 共 %.0f 秒喵", req_id, candidate.label, decision.freeze_seconds
                )
            # 冻结之后换下一个候选喵
            return "next", result
        # 喵~防御：如果这个节点刚刚被自动避险冻结了，就别再原地重试它了。
        # 不加这个判断的话，规则说「重试」时我们会继续打一个已经被判定为不可用的节点，
        # 白白浪费一次尝试和等待时间；直接换下一个候选才对喵
        if hedge_hits > 0:
            return "next", result
        # 规则要求原地重试，且重试次数还没用完喵
        if decision.action == "retry" and attempt_no < decision.max_attempts:
            # 计算这次要等多久：退避基数的 attempt_no 次方，等待时间随次数指数增长喵
            delay = decision.backoff_base ** attempt_no
            # 喵~防御：等待时间上限压到 30 秒，防止把 backoff_base 配大后一等好几分钟喵
            delay = min(delay, 30.0)
            # 记一条重试日志喵
            logger.info(
                "[%s] 退避重试 %s，等待 %.1f 秒后进行第 %d/%d 次尝试喵",
                req_id,
                candidate.name,
                delay,
                attempt_no + 1,
                decision.max_attempts,
            )
            # 异步等待，不阻塞事件循环，其他请求照常处理喵
            await asyncio.sleep(delay)
            # 尝试次数加一，然后回到循环顶部再打一次喵
            attempt_no += 1
            continue
        # 其余情况（decision 是 next，或者 retry 但次数已用完）都是换下一个候选喵
        return "next", result


# 特殊状态码：表示目标模式超时后需要断开连接喵
STATUS_DROP_CONNECTION = -5


async def handle_request(
    client: httpx.AsyncClient,
    state: RuntimeState,
    method: str,
    path: str,
    query: str,
    headers: dict[str, str],
    raw_body: bytes,
    request_started_at: float | None = None,
) -> ProxyOutcome:
    """
    处理一条客户端请求，走完整条候选链喵~ 这是本模块唯一的公开入口喵。

    输入：复用的 HTTP 客户端、运行时状态、以及客户端请求的全部原始要素
    输出：ProxyOutcome，由 server 模块转成真正的 HTTP 响应
    边界条件：请求体非法 → 400；虚拟模型不存在 → 400；整条链用尽 → 502。
            任何情况下都返回 ProxyOutcome，绝不把异常抛给 FastAPI 层喵。
    """
    # 测试或其他直接调用未提供起点时，从进入编排层开始计时喵
    if request_started_at is None:
        request_started_at = time.monotonic()
    # 给这条请求生成一个短 ID，它会出现在这条请求产生的每一行日志上，
    # 这样并发时也能用它把一条请求的完整转移过程 grep 出来喵
    req_id = new_request_id()
    # 累计请求数加一，用于 REPL 的 stats 展示喵
    state.total_requests += 1
    # 解析客户端请求体喵
    try:
        body_obj = parse_client_body(raw_body)
    # 喵~防御：请求体非法时回 400，并把具体原因告诉客户端喵
    except ValueError as exc:
        return ProxyOutcome(
            success=False,
            status=400,
            error_body={"error": {"message": str(exc), "type": "invalid_request_error"}},
        )
    # 取出客户端想用的虚拟模型名喵
    virtual_model = body_obj.get("model")
    # 喵~防御：model 字段缺失或不是字符串时回 400，并列出可用的虚拟模型喵
    if not isinstance(virtual_model, str) or not virtual_model.strip():
        return ProxyOutcome(
            success=False,
            status=400,
            error_body={
                "error": {
                    "message": "请求体缺少 model 字段喵",
                    "type": "invalid_request_error",
                    "available_models": state.list_virtual_models(),
                }
            },
        )
    # 去掉首尾空白，容忍客户端配置里带了空格喵
    virtual_model = virtual_model.strip()
    # 查这个虚拟模型对应的候选链喵
    chain = state.get_chain(virtual_model)
    # 喵~防御：虚拟模型没配过就回 400，并把所有可用的名字列出来帮用户改配置喵
    if chain is None:
        # 记一条日志，方便发现客户端配错了模型名喵
        logger.warning("[%s] 客户端请求了未配置的虚拟模型 %r 喵", req_id, virtual_model)
        # 返回 400 并附上可用列表喵
        return ProxyOutcome(
            success=False,
            status=400,
            error_body={
                "error": {
                    "message": f"虚拟模型 {virtual_model!r} 未在 config.yaml 的 virtual_models 里配置喵",
                    "type": "invalid_request_error",
                    "available_models": state.list_virtual_models(),
                }
            },
        )
    # 判断这是流式还是非流式请求喵
    is_stream = detect_stream_flag(body_obj)
    # 判断目标模式是否开启，开关只从内存读取，不接触 config.yaml 喵
    target_mode = state.target_mode_enabled
    # 取一份配置快照，用于读取目标模式的各项配置喵
    config = state.config
    # 判断请求是否命中配置的接口错误忽略列表喵
    ignored_error_endpoint = _is_ignored_error_endpoint(config.server, method, path)
    # 目标模式请求的截止时刻，使用单调时钟避免系统时间跳变喵
    target_deadline = (
        time.monotonic() + config.server.target_mode_max_wait_seconds if target_mode else None
    )
    # 目标模式已经循环了多少轮喵
    target_rounds = 0
    # 目标模式开始时记录日志；忽略接口不会进入目标模式喵
    if target_mode and not ignored_error_endpoint:
        logger.warning(
            "[%s] 目标模式已接管虚拟模型 %s：链路全失效后每 %.0f 秒重试，最长 %.0f 分钟喵",
            req_id, virtual_model, config.server.target_mode_round_interval_seconds,
            config.server.target_mode_max_wait_seconds / 60,
        )
    # 目标模式外层循环：正常模式只执行一轮，目标模式整轮失败后回到链首喵
    while True:
        # 每一轮重新读取候选链和冻结状态，允许冻结到期或热重载在下一轮生效喵
        current_chain = state.get_chain(virtual_model) or chain
        usable, skipped = _pick_usable_candidates(state, current_chain)
        # 当前轮数加一喵
        target_rounds += 1
        # 记一条开始处理的日志，写明虚拟模型、是否流式、可用候选数和当前轮次喵
        logger.info(
            "[%s] 处理请求 虚拟模型=%s 流式=%s 可用候选=%d/%d 第%d轮喵",
            req_id, virtual_model, is_stream, len(usable), len(current_chain), target_rounds,
        )
        # 收集这一轮失败原因喵
        failures: list[str] = list(skipped)
        # 按严格优先级依次尝试当前轮可用候选喵
        for candidate in usable:
            # 在这个候选上尝试到底（含内部退避重试）喵
            verdict, result = await _run_one_candidate(
                client, state, candidate, method, path, query, headers, body_obj,
                is_stream, req_id, virtual_model, request_started_at, ignored_error_endpoint,
            )
            # 成功了，直接返回，后面的候选不再尝试喵
            if verdict == "ok":
                return ProxyOutcome(success=True, attempt=result, is_stream=is_stream)
            # 规则要求原样回传上游响应（比如 400），绝不能被目标模式重试喵
            if verdict == "passthrough":
                # 忽略接口不纳入虚拟模型健康统计，普通接口把客户端可见错误记为失败喵
                if not ignored_error_endpoint:
                    state.record_virtual_model_health(virtual_model, False)
                return ProxyOutcome(success=True, attempt=result, is_stream=False)
            # 这个候选不行，记下原因然后试下一个喵
            failures.append(f"{candidate.label} → 状态 {result.status}：{result.error_text[:150]}")
        # 整条候选链本轮都走完了，目标模式可能还会继续重试喵
        # total_exhausted 和虚拟模型失败只在下面真正终态返回时结算一次喵
        # 忽略接口链耗尽后直接返回普通 502，只输出一条 info，不进入目标模式喵
        if ignored_error_endpoint:
            # 被忽略接口的耗尽仍保留原有总耗尽计数，但不进入虚拟模型健康统计喵
            state.total_exhausted += 1
            # 记录单条最终信息，保持忽略接口的日志静默语义喵
            logger.info(
                "[%s] 虚拟模型 %s 的接口 %s 全部不可用喵",
                req_id,
                virtual_model,
                path,
            )
            # 返回忽略接口的普通 502 结果喵
            return ProxyOutcome(
                success=False,
                status=502,
                error_body={"error": {
                    "message": f"虚拟模型 {virtual_model} 的所有候选都不可用喵",
                    "type": "upstream_all_failed",
                    "virtual_model": virtual_model,
                    "attempts": failures,
                }},
            )
        # 普通接口未开启目标模式时回传 502，开启时才继续后续目标模式逻辑喵
        if not target_mode:
            # 普通接口最终失败才增加整条链耗尽计数喵
            state.total_exhausted += 1
            # 非忽略接口在真正终态返回前只记录一次虚拟模型失败喵
            state.record_virtual_model_health(virtual_model, False)
            logger.error(
                "[%s] 虚拟模型 %s 的所有候选都失败了喵：%s", req_id, virtual_model, " | ".join(failures)
            )
            return ProxyOutcome(
                success=False,
                status=502,
                error_body={"error": {
                    "message": f"虚拟模型 {virtual_model} 的所有候选都不可用喵",
                    "type": "upstream_all_failed",
                    "virtual_model": virtual_model,
                    "attempts": failures,
                }},
            )
        # 目标模式下，截止时间到了也要让本轮完整结束后才根据配置行为返回喵
        now = time.monotonic()
        if target_deadline is not None and now >= target_deadline:
            waited_seconds = config.server.target_mode_max_wait_seconds
            # 根据配置的超时行为决定返回什么喵
            action = config.server.target_mode_timeout_action
            logger.error(
                "[%s] 目标模式结束 虚拟模型=%s 已尝试%d轮、等待%.0f秒仍无成功响应，行为=%s 喵：%s",
                req_id, virtual_model, target_rounds, waited_seconds, action, " | ".join(failures),
            )
            # drop_connection：断开连接不返回任何响应，客户端会感知为网络超时喵
            if action == "drop_connection":
                # 目标模式最终失败时才增加整条链耗尽计数喵
                state.total_exhausted += 1
                state.record_virtual_model_health(virtual_model, False)
                return ProxyOutcome(
                    success=False,
                    status=STATUS_DROP_CONNECTION,
                    error_body={"error": {
                        "message": "目标模式超时，断开连接喵",
                        "type": "target_mode_drop_connection",
                        "virtual_model": virtual_model,
                        "rounds": target_rounds,
                        "waited_seconds": waited_seconds,
                    }},
                )
            # return_504：返回 504 Gateway Timeout，标准的网关超时状态码喵
            elif action == "return_504":
                status_code = 504
                error_type = "target_mode_gateway_timeout"
                message = "目标模式超时：所有链路等待超时喵"
            # return_429：返回 429 Too Many Requests，表示限流喵
            elif action == "return_429":
                status_code = 429
                error_type = "target_mode_rate_limit"
                message = "目标模式超时：所有链路全部不可用喵"
            # return_502：返回 502 Bad Gateway，伪装成上游故障喵
            elif action == "return_502":
                status_code = 502
                error_type = "target_mode_bad_gateway"
                message = "目标模式超时：所有链路返回错误喵"
            # 喵~防御：未知行为（理论上配置加载时已经挡住了），兜底用 504 喵
            else:
                status_code = 504
                error_type = "target_mode_timeout"
                message = f"目标模式超时（未知行为 {action}）喵"
            # 返回对应的错误响应喵
            # 目标模式最终失败时才增加整条链耗尽计数喵
            state.total_exhausted += 1
            # 目标模式最终失败只结算一次虚拟模型失败喵
            state.record_virtual_model_health(virtual_model, False)
            return ProxyOutcome(
                success=False,
                status=status_code,
                error_body={"error": {
                    "message": message,
                    "type": error_type,
                    "virtual_model": virtual_model,
                    "target_mode": True,
                    "rounds": target_rounds,
                    "waited_seconds": waited_seconds,
                    "attempts": failures,
                }},
            )
        # 还没到截止时间，等待配置的间隔后从链首开始下一轮喵
        # 目标模式下的等待日志保留 warning，普通接口行为不变喵
        logger.warning(
            "[%s] 目标模式第%d轮链路全部不可用，%.0f秒后从链首重试喵：%s",
            req_id, target_rounds, config.server.target_mode_round_interval_seconds, " | ".join(failures),
        )
        await asyncio.sleep(config.server.target_mode_round_interval_seconds)
