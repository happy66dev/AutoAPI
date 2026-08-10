"""
端到端冒烟测试喵~

和单元测试的区别：这里不用任何 mock，而是起三个真的 HTTP 服务，用真的 socket 通信：
    坏上游（端口 9101）  一律返回带恢复时间的 429，用来触发冻结
    好上游（端口 9102）  返回正常的非流式响应和正常的 SSE 流
    autoapi（端口 9100） 被测的代理本体
然后用真的 httpx 客户端打 autoapi，验证故障转移真的发生了、流真的能收到内容喵。

用法喵：
    python smoke_test.py
"""

# 引入注解特性喵
from __future__ import annotations

# asyncio 用来并发跑三个服务和发请求喵
import asyncio
# json 用来构造和解析响应喵
import json
# sys 用来控制退出码喵
import sys
# tempfile 用来放临时配置文件喵
import tempfile
# pathlib 用来处理临时文件路径喵
from pathlib import Path

# httpx 用作测试客户端喵
import httpx
# uvicorn 用来跑三个服务喵
import uvicorn
# FastAPI 用来搭两个假上游喵
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, StreamingResponse

# 引入被测的代理喵
from autoapi.config import load_config
from autoapi.server import create_app
from autoapi.state import RuntimeState

# 三个服务的端口，选一段不常用的避免和别的程序撞车喵
PORT_PROXY = 9100
PORT_BAD = 9101
PORT_GOOD = 9102
# 卡流上游的端口：返回 200 并吐几个字，然后一直挂着不动喵
PORT_STALL = 9103


def make_bad_upstream() -> FastAPI:
    """
    造一个一律返回额度用尽的坏上游喵~

    返回的错误消息刻意做成真实世界的样子，用来验证规则引擎的正则能抽出「6 分钟」喵。
    """
    # 创建应用喵
    app = FastAPI()

    @app.post("/{path:path}")
    async def always_429(path: str) -> JSONResponse:
        """一律返回 429 和带恢复时间的错误消息喵~"""
        # 返回真实世界那种额度用尽的错误喵
        return JSONResponse(
            status_code=429,
            content={
                "error": {
                    "message": "key sk-3ce*** has reached its rolling 1h usage quota; refreshes in 6 minutes"
                }
            },
        )

    # 返回应用喵
    return app


def make_stalling_upstream() -> FastAPI:
    """
    造一个「卡流」上游喵~

    行为：返回 200 并开始吐 SSE，先吐两个字符（不够放行门槛），然后就一直挂着不动、
         连接也不断。这是主人描述的那种最阴险的坏上游，专门用来验证卡流检测喵。
    """
    # 创建应用喵
    app = FastAPI()

    @app.post("/{path:path}")
    async def stall(path: str):
        """吐两个字就卡住喵~"""

        async def gen():
            """先吐一点内容，然后永远挂着喵~"""
            # 吐一个只有 role 的占位首包，模拟真实上游喵
            yield f'data: {json.dumps({"choices": [{"delta": {"role": "assistant", "content": ""}}]})}\n\n'.encode()
            # 再吐两个字符，不够 10 个门槛喵
            yield f'data: {json.dumps({"choices": [{"delta": {"content": "嗯嗯"}}]}, ensure_ascii=False)}\n\n'.encode()
            # 然后一直挂着，代理的探测超时会先到并主动放弃这个候选喵
            await asyncio.sleep(3600)

        # 返回这条永远不会正常结束的流喵
        return StreamingResponse(gen(), media_type="text/event-stream")

    # 返回应用喵
    return app


def make_good_upstream() -> FastAPI:
    """造一个正常工作的好上游，同时支持非流式和流式喵~"""
    # 创建应用喵
    app = FastAPI()

    @app.post("/{path:path}")
    async def chat(request: Request, path: str):
        """按请求体里的 stream 字段决定回非流式还是流式喵~"""
        # 解析请求体喵
        body = await request.json()
        # 非流式：直接回一个完整的响应喵
        if not body.get("stream"):
            # 把收到的 model 名回显出来，这样测试能验证 model 真的被替换了喵
            return JSONResponse(
                {
                    "id": "smoke-1",
                    "model": body.get("model"),
                    "choices": [{"index": 0, "message": {"role": "assistant", "content": "来自好上游的回答"}}],
                }
            )

        async def gen():
            """吐一条正常的 SSE 流喵~"""
            # 先吐一个只有 role 的占位首包，模拟真实上游的行为喵
            first = {"choices": [{"delta": {"role": "assistant", "content": ""}}]}
            # 按 SSE 格式吐出喵
            yield f"data: {json.dumps(first)}\n\n".encode()
            # 逐个吐出内容块，中间加一点延迟模拟真实的生成速度喵
            for word in ["好上游", "正在", "流式", "回答"]:
                # 构造内容增量喵
                chunk = {"choices": [{"delta": {"content": word}}]}
                # 吐出这一块喵
                yield f"data: {json.dumps(chunk, ensure_ascii=False)}\n\n".encode()
                # 稍微等一下，模拟真实的逐字生成喵
                await asyncio.sleep(0.02)
            # 最后吐结束标记喵
            yield b"data: [DONE]\n\n"

        # 返回流式响应喵
        return StreamingResponse(gen(), media_type="text/event-stream")

    # 返回应用喵
    return app


def write_temp_config(directory: Path) -> Path:
    """
    在临时目录里写一份指向两个假上游的配置喵~

    候选链故意把坏上游放在链首、好上游放在第二位，这样只要能成功就证明发生了故障转移喵。
    """
    # 配置文本，坏上游在前好上游在后喵
    text = f"""
server:
  host: 127.0.0.1
  port: {PORT_PROXY}
  # 超时设得比较小，让卡流检测能在几秒内触发，冒烟测试才跑得快喵
  # 静默上限：那个假上游吐两个字就彻底不动了，所以 3 秒静默就足够判定卡流喵
  stall_timeout: 3
  # 流式和非流式的总预算也压小，免得万一哪个检查卡住会等很久喵
  stream_timeout: 30
  nonstream_timeout: 30
  # 动态 RPM/TPM/平均耗时的统计窗口，单位：分钟。冒烟测试里显式写出，验证模板字段可用喵
  metrics_window_minutes: 30
  min_content_chars: 10
  connect_timeout: 5
  reload_poll_interval: 0

virtual_models:
  auto-smoke:
    - name: 坏上游
      base_url: http://127.0.0.1:{PORT_BAD}
      api_key: sk-bad-key-for-smoke-test
      model: 坏模型
      auth_style: bearer
    - name: 好上游
      base_url: http://127.0.0.1:{PORT_GOOD}
      api_key: sk-good-key-for-smoke-test
      model: 真实的好模型名
      auth_style: bearer

  # 专门用来验证卡流的虚拟模型：链首是那个吐两个字就挂住的上游，第二位是好上游喵
  auto-stall:
    - name: 卡流上游
      base_url: http://127.0.0.1:{PORT_STALL}
      api_key: sk-stall-key-for-smoke-test
      model: 卡流模型
      auth_style: bearer
    - name: 好上游
      base_url: http://127.0.0.1:{PORT_GOOD}
      api_key: sk-good-key-for-smoke-test
      model: 真实的好模型名
      auth_style: bearer

rules:
  - match:
      status: 429
      body_regex: 'refreshes?\\s+in\\s+(\\d+)\\s+minutes?'
    action: freeze
    freeze_from_group: 1
    freeze_unit: minutes
    freeze_seconds: 300
  - match:
      status: stalled_stream
    action: retry
    max_attempts: 2
    backoff_base: 1.0
  - match:
      status: bad_stream
    action: next
  - match:
      status: [401, 403, 404]
    action: next
  - match:
      status: 400
    action: passthrough
""".lstrip()
    # 写到临时目录里的 config.yaml 喵
    path = directory / "config.yaml"
    # 显式 utf-8 写入以正确保存中文喵
    path.write_text(text, encoding="utf-8")
    # 返回路径喵
    return path


async def serve(app: FastAPI, port: int) -> uvicorn.Server:
    """
    在后台把一个应用跑起来，返回服务器对象以便之后停掉喵~

    输入：应用对象和端口
    输出：uvicorn.Server 对象
    """
    # 构造 uvicorn 配置，关掉日志让冒烟测试的输出保持清爽喵
    config = uvicorn.Config(app=app, host="127.0.0.1", port=port, log_level="error", access_log=False)
    # 创建服务器对象喵
    server = uvicorn.Server(config)
    # 用后台任务跑起来，不阻塞当前协程喵
    asyncio.create_task(server.serve())
    # 等它真的开始监听，最多等 5 秒喵
    for _ in range(50):
        # started 标志为 True 说明已经在监听了喵
        if server.started:
            return server
        # 还没起来就再等一小会喵
        await asyncio.sleep(0.1)
    # 喵~防御：等太久还没起来就报错，避免后续测试对着一个没起来的端口打请求喵
    raise RuntimeError(f"端口 {port} 上的服务启动超时喵")


def check(name: str, ok: bool, detail: str = "") -> bool:
    """打印一条检查结果并返回是否通过喵~"""
    # 通过打对勾，失败打叉喵
    mark = "[通过]" if ok else "[失败]"
    # 打印结果行喵
    print(f"  {mark} {name}" + (f" —— {detail}" if detail else ""))
    # 返回结果供汇总喵
    return ok


async def run_smoke() -> bool:
    """
    跑完整的冒烟测试喵~

    输出：全部通过返回 True，有任何一项失败返回 False
    """
    # 用一个临时目录放配置文件，测完自动清理喵
    with tempfile.TemporaryDirectory() as tmp:
        # 写配置文件喵
        config_path = write_temp_config(Path(tmp))
        # 加载配置喵
        config = load_config(config_path)
        # 创建运行时状态喵
        state = RuntimeState(config)
        # 收集每项检查的结果喵
        results: list[bool] = []
        # 依次把三个服务跑起来喵
        print("\n启动三个服务喵~")
        # 坏上游喵
        bad_server = await serve(make_bad_upstream(), PORT_BAD)
        # 好上游喵
        good_server = await serve(make_good_upstream(), PORT_GOOD)
        # 卡流上游：吐两个字就挂住喵
        stall_server = await serve(make_stalling_upstream(), PORT_STALL)
        # 被测的代理本体喵
        proxy_server = await serve(create_app(state), PORT_PROXY)
        # 打印就绪信息喵
        print(f"  坏上游 http://127.0.0.1:{PORT_BAD}（一律返回 429 额度用尽）")
        print(f"  好上游 http://127.0.0.1:{PORT_GOOD}（正常工作）")
        print(f"  卡流上游 http://127.0.0.1:{PORT_STALL}（吐两个字然后永远挂着）")
        print(f"  autoapi 代理 http://127.0.0.1:{PORT_PROXY}\n")
        # 代理的基础地址喵
        base = f"http://127.0.0.1:{PORT_PROXY}"
        # 用真的 httpx 客户端打真的 socket 喵
        async with httpx.AsyncClient(timeout=30) as client:
            # ---- 检查 1：健康检查接口喵 ----
            print("检查 1：健康检查接口喵")
            # 打 /healthz 喵
            r = await client.get(f"{base}/healthz")
            # 状态码应该是 200 喵
            results.append(check("healthz 返回 200", r.status_code == 200, f"实际 {r.status_code}"))
            # 应该报告有 2 个虚拟模型（auto-smoke 和用于测卡流的 auto-stall）喵
            results.append(
                check(
                    "报告了 2 个虚拟模型",
                    r.json().get("virtual_models") == 2,
                    f"实际 {r.json().get('virtual_models')}",
                )
            )

            # ---- 检查 2：模型列表接口喵 ----
            print("\n检查 2：模型列表接口喵")
            # 打 /v1/models 喵
            r = await client.get(f"{base}/v1/models")
            # 取出返回的模型 id 列表喵
            ids = [m["id"] for m in r.json().get("data", [])]
            # 应该包含我们配的虚拟模型名喵
            results.append(check("列出了 auto-smoke 虚拟模型", "auto-smoke" in ids, f"实际 {ids}"))

            # ---- 检查 3：非流式故障转移喵 ----
            print("\n检查 3：非流式请求的故障转移喵（链首是必然失败的坏上游）")
            # 发一条非流式请求喵
            r = await client.post(
                f"{base}/v1/chat/completions",
                json={"model": "auto-smoke", "messages": [{"role": "user", "content": "你好"}]},
            )
            # 尽管链首必然失败，客户端拿到的应该是 200 喵
            results.append(check("客户端收到 200", r.status_code == 200, f"实际 {r.status_code} {r.text[:200]}"))
            # 解析响应体喵
            data = r.json() if r.status_code == 200 else {}
            # 内容应该来自好上游喵
            results.append(
                check(
                    "内容来自好上游",
                    "好上游" in json.dumps(data, ensure_ascii=False),
                    f"实际 {json.dumps(data, ensure_ascii=False)[:200]}",
                )
            )
            # 好上游回显的 model 应该是候选里配的真实模型名，证明 model 被正确替换了喵
            results.append(
                check(
                    "model 被替换成候选的真实模型名",
                    data.get("model") == "真实的好模型名",
                    f"实际 {data.get('model')!r}",
                )
            )

            # ---- 检查 4：动态统计窗口配置已生效喵 ----
            print("\n检查 4：动态 RPM/TPM 统计窗口配置喵")
            # 临时配置明确写的是 30 分钟，加载后必须是同一个值喵
            results.append(
                check(
                    "metrics_window_minutes 已加载为 30 分钟",
                    state.config.server.metrics_window_minutes == 30.0,
                    f"实际 {state.config.server.metrics_window_minutes}",
                )
            )

            # ---- 检查 5：坏上游被冻结了喵 ----
            print("\n检查 5：坏上游按上游说的 6 分钟被冻结喵")
            # 取出链首那个坏候选喵
            bad_candidate = state.config.virtual_models["auto-smoke"][0]
            # 查它的剩余冻结时间喵
            remaining = state.is_frozen(bad_candidate)
            # 应该在 6 分钟左右（360 秒 + 5 秒缓冲）喵
            results.append(
                check("冻结时长约为 6 分钟", 350 < remaining <= 365, f"实际剩余 {remaining:.0f} 秒")
            )

            # ---- 检查 5：流式故障转移，且内容完整喵 ----
            print("\n检查 5：流式请求的故障转移与内容完整性喵")
            # 收集收到的所有 SSE 行喵
            lines: list[str] = []
            # 用流式方式发请求喵
            async with client.stream(
                "POST",
                f"{base}/v1/chat/completions",
                json={"model": "auto-smoke", "messages": [{"role": "user", "content": "你好"}], "stream": True},
            ) as resp:
                # 状态码应该是 200 喵
                results.append(check("流式请求收到 200", resp.status_code == 200, f"实际 {resp.status_code}"))
                # content-type 应该是 SSE 喵
                results.append(
                    check(
                        "content-type 是 text/event-stream",
                        "text/event-stream" in resp.headers.get("content-type", ""),
                        f"实际 {resp.headers.get('content-type')}",
                    )
                )
                # 逐行读取流喵
                async for line in resp.aiter_lines():
                    # 只收集非空行喵
                    if line.strip():
                        lines.append(line)
            # 把所有行拼起来方便检查内容喵
            joined = "\n".join(lines)
            # 应该收到了好上游吐的全部四个词喵
            results.append(
                check(
                    "收到了流里全部四段内容",
                    all(w in joined for w in ["好上游", "正在", "流式", "回答"]),
                    f"共收到 {len(lines)} 行",
                )
            )
            # 应该收到结束标记喵
            results.append(check("收到了 [DONE] 结束标记", "[DONE]" in joined))
            # 关键：占位首包也该被完整转发，证明「先缓冲后replay」没丢字节喵
            results.append(
                check(
                    "探测阶段缓冲的首包也被完整转发",
                    '"role": "assistant"' in joined or '"role":"assistant"' in joined,
                    "首包丢失说明缓冲replay有问题",
                )
            )

            # ---- 检查 6：未配置的虚拟模型回 400 喵 ----
            print("\n检查 6：未配置的虚拟模型应该回 400 喵")
            # 请求一个没配过的模型喵
            r = await client.post(
                f"{base}/v1/chat/completions",
                json={"model": "根本没配过这个模型", "messages": []},
            )
            # 应该是 400 喵
            results.append(check("返回 400", r.status_code == 400, f"实际 {r.status_code}"))
            # 错误体里应该列出可用的虚拟模型，帮用户改配置喵
            results.append(
                check(
                    "错误里列出了可用的虚拟模型",
                    "auto-smoke" in json.dumps(r.json(), ensure_ascii=False),
                )
            )

            # ---- 检查 7：冻结生效后直接走好上游喵 ----
            print("\n检查 7：坏上游还在冻结中，后续请求应该直接跳过它喵")
            # 记下当前的请求计数喵
            before = state.total_requests
            # 再发一条请求喵
            r = await client.post(
                f"{base}/v1/chat/completions",
                json={"model": "auto-smoke", "messages": [{"role": "user", "content": "再来一次"}]},
            )
            # 应该照样成功喵
            results.append(check("依然返回 200", r.status_code == 200, f"实际 {r.status_code}"))
            # 请求计数应该加了 1 喵
            results.append(check("请求计数正确递增", state.total_requests == before + 1))

            # ---- 检查 8：卡流检测（走真 socket）喵 ----
            print("\n检查 8：卡流检测喵（链首吐两个字就永远挂着，应该重发一次后降级到好上游）")
            # 收集收到的所有 SSE 行喵
            stall_lines: list[str] = []
            # 记下开始时间，用来确认没有一直干等下去喵
            stall_start = asyncio.get_event_loop().time()
            # 用流式方式打那个链首是卡流上游的虚拟模型喵
            async with client.stream(
                "POST",
                f"{base}/v1/chat/completions",
                json={"model": "auto-stall", "messages": [{"role": "user", "content": "你好"}], "stream": True},
            ) as resp:
                # 尽管链首会卡住，客户端最终应该拿到 200 喵
                results.append(check("卡流后客户端仍收到 200", resp.status_code == 200, f"实际 {resp.status_code}"))
                # 逐行读取喵
                async for line in resp.aiter_lines():
                    # 只收集非空行喵
                    if line.strip():
                        stall_lines.append(line)
            # 算出总耗时喵
            stall_elapsed = asyncio.get_event_loop().time() - stall_start
            # 拼起来方便检查内容喵
            stall_joined = "\n".join(stall_lines)
            # 内容应该来自好上游，而不是卡流上游那两个「嗯嗯」喵
            results.append(
                check(
                    "内容来自好上游而非卡流上游",
                    all(w in stall_joined for w in ["好上游", "正在", "流式", "回答"]),
                    f"共收到 {len(stall_lines)} 行",
                )
            )
            # 卡流上游吐的那两个字绝不能漏给客户端 —— 它们在探测阶段就被丢掉了喵
            results.append(
                check(
                    "卡流上游吐的残缺内容没有漏给客户端",
                    "嗯嗯" not in stall_joined,
                    "漏出去说明放行门槛没起作用",
                )
            )
            # 耗时应该约等于「两次卡流各 3 秒 + 1 秒退避」，不该无限干等喵
            results.append(
                check(
                    "在合理时间内完成降级（没有无限干等）",
                    stall_elapsed < 20,
                    f"实际耗时 {stall_elapsed:.1f} 秒",
                )
            )
            # 打印耗时供参考喵
            print(f"       耗时 {stall_elapsed:.1f} 秒（两次卡流探测各 3 秒 + 1 秒退避 + 好上游生成时间）喵~")

            # ---- 检查 10：目标模式开关的冒烟检查喵 ----
            print("\n检查 10：目标模式开关与状态横幅喵")
            # 目标模式默认关闭喵
            results.append(check("目标模式默认关闭", state.target_mode_enabled is False))
            # 开启目标模式，验证它只改内存状态喵
            state.set_target_mode(True)
            results.append(check("目标模式可以开启", state.target_mode_enabled is True))
            # 关闭目标模式，避免影响冒烟进程后续行为喵
            state.set_target_mode(False)
            results.append(check("目标模式可以关闭", state.target_mode_enabled is False))
            # 临时配置文本中不应出现 target_mode 持久化字段喵
            config_text = config_path.read_text(encoding="utf-8")
            results.append(check("目标模式没有写入配置", "target_mode:" not in config_text))

            # ---- 检查 11：并发压测喵 ----
            print("\n检查 11：并发压测（60 条请求同时打进来，对应 60rpm 全挤在一秒）喵")
            # 记下开始时间喵
            start = asyncio.get_event_loop().time()
            # 同时发 60 条请求喵
            responses = await asyncio.gather(
                *(
                    client.post(
                        f"{base}/v1/chat/completions",
                        json={"model": "auto-smoke", "messages": [{"role": "user", "content": f"第{i}条"}]},
                    )
                    for i in range(60)
                ),
                # 收集异常而不是让第一个异常就中断全部喵
                return_exceptions=True,
            )
            # 算出总耗时喵
            elapsed = asyncio.get_event_loop().time() - start
            # 统计成功的条数喵
            ok_count = sum(1 for x in responses if isinstance(x, httpx.Response) and x.status_code == 200)
            # 60 条应该全部成功喵
            results.append(check("60 条并发请求全部成功", ok_count == 60, f"实际成功 {ok_count}/60"))
            # 打印吞吐量供参考喵
            print(f"       耗时 {elapsed:.2f} 秒，约合 {60 / max(elapsed, 0.001):.0f} 请求/秒，远超 60rpm 的要求喵~")

        # 测完把四个服务都停掉喵
        for server in (proxy_server, good_server, bad_server, stall_server):
            # 置位退出标志，uvicorn 会优雅停机喵
            server.should_exit = True
        # 给它们一点时间收尾喵
        await asyncio.sleep(0.5)
        # 汇总结果喵
        passed = sum(1 for x in results if x)
        # 打印总结喵
        print(f"\n{'=' * 60}")
        # 全通过和有失败分别打印不同的总结喵
        if passed == len(results):
            print(f"全部 {len(results)} 项检查都通过了喵~ 代理可以用啦~")
        else:
            print(f"{passed}/{len(results)} 项通过，有 {len(results) - passed} 项失败喵，需要看一下~")
        # 打印分隔线喵
        print(f"{'=' * 60}\n")
        # 返回是否全部通过喵
        return passed == len(results)


# 只有直接运行这个文件时才执行喵
if __name__ == "__main__":
    # 跑冒烟测试，全通过退出码 0，有失败退出码 1 喵
    sys.exit(0 if asyncio.run(run_smoke()) else 1)
