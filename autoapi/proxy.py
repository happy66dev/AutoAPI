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
# json 用来解析客户端请求体喵
import json
# logging 用来输出转发过程的日志喵
import logging
# dataclass 用来定义编排结果容器喵
from dataclasses import dataclass, field
# Any 用于标注解析出来的请求体喵
from typing import Any

# httpx 提供复用的异步客户端类型喵
import httpx

# 引入候选类型喵
from .config import Candidate
# 引入规则引擎喵
from .rules import decide
# 引入运行时状态喵
from .state import RuntimeState
# 引入上游调用相关的东西喵
from .upstream import AttemptResult, try_candidate

# 本模块的日志器，名字会显示在日志行里方便定位喵
logger = logging.getLogger("autoapi.proxy")


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
        )
        # 成功了，记一笔成功（顺带会自动解冻这个候选）然后返回喵
        if result.ok:
            # 更新统计并解冻喵
            state.record_success(candidate)
            # 记一条成功日志，第几次尝试也记上，方便观察上游稳定性喵
            logger.info("成功 %s（第 %d 次尝试）喵", candidate.label, attempt_no)
            # 返回成功结论喵
            return "ok", result
        # 失败了，先记一笔失败喵
        state.record_failure(candidate, result.error_text)
        # 问规则引擎该怎么办喵
        decision = decide(config.rules, result.status, result.error_text, result.retry_after)
        # 打一条失败日志，把状态码、决策和命中的规则都写清楚喵
        logger.warning(
            "失败 %s 状态=%s 决策=%s 依据=%s 原因=%s 喵",
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
            # 记一条冻结日志，写明冻多久喵
            logger.warning("冻结 %s 共 %.0f 秒喵", candidate.label, decision.freeze_seconds)
            # 冻结之后换下一个候选喵
            return "next", result
        # 规则要求原地重试，且重试次数还没用完喵
        if decision.action == "retry" and attempt_no < decision.max_attempts:
            # 计算这次要等多久：退避基数的 attempt_no 次方，等待时间随次数指数增长喵
            delay = decision.backoff_base ** attempt_no
            # 喵~防御：等待时间上限压到 30 秒，防止把 backoff_base 配大后一等好几分钟喵
            delay = min(delay, 30.0)
            # 记一条重试日志喵
            logger.info(
                "退避重试 %s，等待 %.1f 秒后进行第 %d/%d 次尝试喵",
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


async def handle_request(
    client: httpx.AsyncClient,
    state: RuntimeState,
    method: str,
    path: str,
    query: str,
    headers: dict[str, str],
    raw_body: bytes,
) -> ProxyOutcome:
    """
    处理一条客户端请求，走完整条候选链喵~ 这是本模块唯一的公开入口喵。

    输入：复用的 HTTP 客户端、运行时状态、以及客户端请求的全部原始要素
    输出：ProxyOutcome，由 server 模块转成真正的 HTTP 响应
    边界条件：请求体非法 → 400；虚拟模型不存在 → 400；整条链用尽 → 502。
            任何情况下都返回 ProxyOutcome，绝不把异常抛给 FastAPI 层喵。
    """
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
        logger.warning("客户端请求了未配置的虚拟模型 %r 喵", virtual_model)
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
    # 挑出当前没被冻结的候选，以及被跳过的说明喵
    usable, skipped = _pick_usable_candidates(state, chain)
    # 记一条开始处理的日志，写明虚拟模型、是否流式、可用候选数喵
    logger.info(
        "处理请求 虚拟模型=%s 流式=%s 可用候选=%d/%d 喵",
        virtual_model,
        is_stream,
        len(usable),
        len(chain),
    )
    # 收集每个候选的失败原因，整条链都失败时要回给客户端喵
    failures: list[str] = list(skipped)
    # 按严格优先级依次尝试每个可用候选喵
    for candidate in usable:
        # 在这个候选上尝试到底（含内部退避重试）喵
        verdict, result = await _run_one_candidate(
            # 复用的客户端喵
            client,
            # 运行时状态喵
            state,
            # 当前候选喵
            candidate,
            # 以下都是原样转发的请求要素喵
            method,
            path,
            query,
            headers,
            body_obj,
            is_stream,
        )
        # 成功了，直接返回，后面的候选不再尝试喵
        if verdict == "ok":
            return ProxyOutcome(success=True, attempt=result, is_stream=is_stream)
        # 规则要求原样回传上游响应（比如 400，客户端自己请求写错了）喵
        if verdict == "passthrough":
            # 用 success=True 走「直接回传」的路径，但注意此时 result 里装的是上游的错误响应喵
            return ProxyOutcome(
                success=True,
                attempt=result,
                # passthrough 的响应体已经完整读出来了，所以按非流式回传喵
                is_stream=False,
            )
        # 这个候选不行，记下原因然后试下一个喵
        failures.append(f"{candidate.label} → 状态 {result.status}：{result.error_text[:150]}")
    # 整条候选链都走完了还没成功喵
    state.total_exhausted += 1
    # 记一条错误日志，把所有失败原因都写进去喵
    logger.error("虚拟模型 %s 的所有候选都失败了喵：%s", virtual_model, " | ".join(failures))
    # 回 502 并附上每个候选各自的失败原因，方便一眼看出是全挂了还是全在冻结喵
    return ProxyOutcome(
        success=False,
        status=502,
        error_body={
            "error": {
                "message": f"虚拟模型 {virtual_model} 的所有候选都不可用喵",
                "type": "upstream_all_failed",
                "virtual_model": virtual_model,
                "attempts": failures,
            }
        },
    )
