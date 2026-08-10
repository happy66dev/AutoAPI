"""
上游单次调用模块喵~

职责边界（只做一件事）：
    「用指定的一个候选，打一次请求，并判断这次结果是否健康」。
    它不关心候选链、不关心重试、不关心冻结 —— 那些都是 proxy 模块的事喵。

两条路径：
    非流式：把整个响应读完，200 就算成功（顺手防一下「200 里面塞 error 字段」的假成功）
    流式：  边读边用 StreamProbe 探测，直到确认吐出第一个有效内容字符才算成功。
           成功时把「已经缓冲的前缀字节」和「还活着的响应对象」一起交回去，
           让 proxy 继续把后续字节吹给客户端；失败时就地关掉响应，客户端毫无感知喵。

边界条件：连接失败、握手超时、读超时、等首个内容超时、上游中途断连，
        全部转成带特殊状态码的失败结果，绝不让异常穿透到 FastAPI 层喵。
"""

# 引入注解特性喵
from __future__ import annotations

# asyncio 用来做超时控制喵
import asyncio
# json 用来序列化改过 model 的请求体喵
import json
# time 用来取单调时钟，给探测阶段的两个计时器算剩余预算喵
import time
# dataclass 用来定义结果容器喵
from dataclasses import dataclass, field
# AsyncIterator 用于标注流式生成器的返回类型喵
from typing import Any, AsyncIterator

# httpx 提供异步 HTTP 客户端喵
import httpx

# 引入候选类型和两个特殊状态码喵
from .config import (
    STATUS_BAD_STREAM,
    STATUS_NETWORK_ERROR,
    STATUS_STALLED_STREAM,
    STATUS_TIMEOUT,
    Candidate,
    EffectiveTimeouts,
    ServerConfig,
    resolve_timeouts,
)
# 引入流探测器和结论常量喵
from .sse import VERDICT_CONTENT, VERDICT_PENDING, StreamProbe

# 逐跳请求头：这些头只对「客户端到代理」这一段连接有意义，绝不能原样转给上游喵
HOP_BY_HOP_HEADERS = {
    # host 必须由 httpx 按上游地址重新生成，转发旧的会导致上游路由错误喵
    "host",
    # 请求体被我们改过（换了 model），长度变了，必须让 httpx 重新计算喵
    "content-length",
    # 连接管理头，属于单跳语义喵
    "connection",
    # 分块编码头，由 httpx 自己管喵
    "transfer-encoding",
    # 保持连接头，单跳语义喵
    "keep-alive",
    # 压缩协商交给 httpx，避免我们拿到压缩流后无法探测内容喵
    "accept-encoding",
    # 客户端的鉴权头要丢掉，换成候选自己的 key 喵
    "authorization",
    # Anthropic 风格的鉴权头，同样要换掉喵
    "x-api-key",
}

# 从上游响应往回传给客户端时要剔除的响应头，理由同上：这些由代理自己重新生成喵
DROP_RESPONSE_HEADERS = {
    # 长度由 FastAPI 重新算，转发旧值会导致客户端读取被截断喵
    "content-length",
    # 编码方式由代理这一段连接决定喵
    "content-encoding",
    # 分块编码头由 ASGI 服务器自己管喵
    "transfer-encoding",
    # 连接管理头，单跳语义喵
    "connection",
    # 保持连接头，单跳语义喵
    "keep-alive",
}


@dataclass
class AttemptResult:
    """
    一次上游调用的结果喵~

    三种形态：
        1. 非流式成功：ok=True，body 装着完整响应字节
        2. 流式成功：  ok=True，response 是还活着的响应对象，buffered 是已经读掉的前缀字节
        3. 失败：      ok=False，status 和 error_text 供规则引擎判断该怎么办
    """

    # 这次尝试是否健康喵
    ok: bool
    # 状态码，可能是真 HTTP 码，也可能是 -1(网络失败) 或 -2(假成功流) 喵
    status: int
    # 失败原因文本，同时也是规则引擎做正则匹配的输入喵
    error_text: str = ""
    # 上游的 Retry-After 响应头原文，用于计算冻结时长喵
    retry_after: str | None = None
    # 要回传给客户端的响应头喵
    headers: dict[str, str] = field(default_factory=dict)
    # 非流式成功时的完整响应体字节喵
    body: bytes | None = None
    # 流式成功时仍然活着的响应对象，用完要关掉它释放连接喵
    response: httpx.Response | None = None
    # 流式成功时，探测阶段已经从上游读掉、但还没给客户端的前缀字节喵
    buffered: bytes = b""
    # 探测阶段用的那个字节迭代器喵
    # 关键：必须把它带出来给后面接着用，绝不能重新调一次 response.aiter_bytes()。
    # httpx 的 aiter_raw 会在第一次迭代时把 is_stream_consumed 置位，再调第二次会直接抛
    # StreamConsumed。所以探测和转发必须共用同一个迭代器，从上次停下的地方接着读喵。
    iterator: AsyncIterator[bytes] | None = None

    @property
    def media_type(self) -> str:
        """从响应头里取 Content-Type，取不到就按 JSON 猜喵~"""
        # 大小写不敏感地找 content-type，找不到时用 application/json 兜底喵
        for key, value in self.headers.items():
            # 逐个比对小写后的头名喵
            if key.lower() == "content-type":
                return value
        # 没找到就返回默认值喵
        return "application/json"


def build_timeout(timeouts: EffectiveTimeouts, is_stream: bool) -> httpx.Timeout:
    """
    按这次请求实际生效的超时值现算一份 httpx 超时设置喵~

    为什么每条请求都现算而不是复用客户端上的默认值：
        httpx 客户端是在 lifespan 里建的、全程复用，它身上的 timeout 是启动那一刻定死的。
        如果只依赖那个值，主人在运行中把超时调大，客户端仍然按旧的小值掐断，
        改动看起来「没生效」，而且很难看出为什么。所以这里每条请求都现算一份传给 httpx，
        让超时配置（以及节点上的专属覆盖）都能立即生效喵。

    流式和非流式的 read 超时含义完全不同，所以要分开设：
        流式   read 超时 = 允许上游静默多久。SSE 是「一点点吐」，两个 chunk 之间的间隔
               才是有意义的度量；用总预算当 read 超时的话，卡死的流要等满整个预算才被发现喵。
        非流式 read 超时 = 总预算。非流式是「上游憋完整篇再一次性返回」，中间本来就没有
               任何字节可收，用静默时长去掐它会把慢模型全部冤枉掉喵。
    """
    # 流式看静默、非流式看总预算，选出这次该用的 read 超时喵
    read_timeout = timeouts.stall if is_stream else timeouts.nonstream
    # 分项设置各类超时喵
    return httpx.Timeout(
        # 兜底值给所有未显式指定的项（write、pool）用喵
        timeout=read_timeout,
        # 建立连接的超时单独设置，连不上要快速失败好换下一个候选喵
        connect=timeouts.connect,
        # 读超时按上面选好的值设置喵
        read=read_timeout,
    )


def build_upstream_headers(client_headers: dict[str, str], candidate: Candidate) -> dict[str, str]:
    """
    构造发给上游的请求头喵~

    做法：把客户端的头原样带过去（这样客户端的自定义头、beta 开关都能透传），
         但剔除所有逐跳头和客户端的鉴权头，然后按候选的风格补上我们自己的鉴权头喵。
    """
    # 先把客户端头里不该转发的过滤掉喵
    headers = {
        # 保留原始头名的大小写，HTTP 头名本身大小写不敏感喵
        key: value
        # 遍历客户端送来的所有头喵
        for key, value in client_headers.items()
        # 小写后不在逐跳黑名单里的才保留喵
        if key.lower() not in HOP_BY_HOP_HEADERS
    }
    # Anthropic 风格：用 x-api-key 头，并且必须带 anthropic-version 否则上游会 400 喵
    if candidate.auth_style == "x-api-key":
        # 写入 Anthropic 的鉴权头喵
        headers["x-api-key"] = candidate.api_key
        # 喵~防御：客户端没带 anthropic-version 时补一个稳定的默认版本，否则上游直接拒绝喵
        if not any(k.lower() == "anthropic-version" for k in headers):
            headers["anthropic-version"] = "2023-06-01"
    # 默认的 bearer 风格：OpenAI 及绝大多数中转站都用这个喵
    else:
        # 写入标准的 Bearer 鉴权头喵
        headers["Authorization"] = f"Bearer {candidate.api_key}"
    # 返回构造好的请求头喵
    return headers


def build_upstream_body(body_obj: dict[str, Any], candidate: Candidate) -> bytes:
    """
    构造发给上游的请求体喵~

    只改一个字段：把顶层的 model 换成候选的真实模型名。其余字段（messages、tools、
    temperature、上游自定义扩展字段）一律原样保留，做到最大程度透传喵。
    OpenAI 的 /v1/chat/completions 和 Anthropic 的 /v1/messages 都把 model 放在顶层，
    所以这一处替换对两种协议通用，不需要做协议探测喵。
    """
    # 浅拷贝一份，避免改动调用方持有的原始字典（原字典可能还要用于日志）喵
    new_body = dict(body_obj)
    # 把 model 换成这个候选的真实模型名喵
    new_body["model"] = candidate.model
    # 序列化成字节，ensure_ascii=False 保留中文原样以减小体积喵
    return json.dumps(new_body, ensure_ascii=False).encode("utf-8")


def _filter_response_headers(response: httpx.Response) -> dict[str, str]:
    """挑出可以安全回传给客户端的上游响应头喵~"""
    # 过滤掉由代理这一段连接自己决定的头喵
    return {
        # 保留头名和值喵
        key: value
        # 遍历上游响应的所有头喵
        for key, value in response.headers.items()
        # 小写后不在黑名单里的才保留喵
        if key.lower() not in DROP_RESPONSE_HEADERS
    }


def _summarize_error(status: int, text: str) -> str:
    """把上游的错误响应压成一行摘要，用于日志和规则匹配喵~"""
    # 去掉首尾空白并把换行压成空格，让日志保持单行喵
    flat = " ".join((text or "").split())
    # 截断到 300 字符，防止上游回一个巨大的 HTML 错误页把日志刷爆喵
    clipped = flat[:300]
    # 拼成「状态码 + 摘要」的形式喵
    return f"HTTP {status}: {clipped}" if clipped else f"HTTP {status}（响应体为空）"


# 探测阶段缓冲区的硬上限，单位：字节。超过就判定这条流不正常喵
# 主人注意：这个上限是为了防内存被打爆。正常的 SSE 流在吐出第一个字之前只有几百字节的
# 元信息（message_start、role 占位包之类），2MB 已经宽裕到不可能误伤；如果真有上游在
# 首个内容字符之前就塞了 2MB 元信息，那它本身就不正常，判失败换下一个是对的喵。
MAX_PROBE_BUFFER_BYTES = 2 * 1024 * 1024


# 探测阶段的额外结论：缓冲区被撑爆了喵
PROBE_BUFFER_OVERFLOW = "buffer_overflow"
# 探测阶段的额外结论：上游静默太久，连一个字节都不发了（连接像是挂死了）喵
PROBE_STALLED = "stalled"
# 探测阶段的额外结论：连接一直活着、上游也一直在发东西，但用光了总预算还没拿到足够内容喵
PROBE_TIMEOUT = "timeout"


async def _probe_until_content(
    iterator: AsyncIterator[bytes],
    probe: StreamProbe,
    buffered: bytearray,
    stall_timeout: float,
    stream_timeout: float,
) -> tuple[str, str]:
    """
    从上游流里持续读取，直到探测出明确结论、判定它静默太久、或者用光总预算喵~

    输入：
        iterator       上游的字节迭代器
        probe          探测器（内部维护字数门槛）
        buffered       用于暂存原始字节的可变缓冲区
        stall_timeout  允许上游连续静默多少秒（收到任何字节就重新计时）
        stream_timeout 整个探测阶段的总预算秒数
    输出：(结论, 说明文本)，结论是 VERDICT_* 或 PROBE_STALLED / PROBE_TIMEOUT /
         PROBE_BUFFER_OVERFLOW

    这里有两个语义完全不同的计时器，分清它们很重要喵：

        静默计时器（stall_timeout）—— 衡量「上游还活着吗」
            每收到任何一个字节就归零重新数。所以上游只要还在发东西（心跳注释行、
            message_start 元信息包、空的 role 占位包、思维链增量……），它就一直不会触发。
            触发了就说明连接真的挂死了，一个字节都不来了喵。

        总预算计时器（stream_timeout）—— 衡量「等太久了吗」
            从探测开始算起，一路数到底，中途绝不归零。上游可能一直在勤快地发心跳、
            连接非常健康，但正文就是迟迟不来 —— 这种情况静默计时器永远不会响，
            必须靠总预算兜住，否则会一直等下去喵。

    为什么这么改（这一版修掉的 bug）：
        上一版这里是「首字符计时器」：只要还没收到内容字符，就从探测开始计时，
        45 秒到了直接判失败。问题是它压根不看连接上有没有动静 —— 上游正在发心跳、
        正在吐思维链、正在正常地准备响应，全都不算，一律 45 秒斩。
        推理模型先想一两分钟再吐第一个字是很正常的事，于是就出现了主人看到的现象：
        「首字还没出就被我们自动重试了」。改成「静默才算卡」之后，正在干活的上游
        不会再被冤枉，而真正挂死的上游照旧能被快速发现喵。

    副作用：读到的所有原始字节都会追加进 buffered，一个字节都不丢，
           这样确认健康后才能把它们原样 replay 给客户端喵。

    注意：这里接收的是「迭代器」而不是「响应对象」，因为探测停下之后，转发阶段必须从
         同一个迭代器接着读。httpx 的流只能迭代一次，重新调 aiter_bytes() 会抛
         StreamConsumed，所以迭代器必须在两个阶段之间传递下去喵。
    """
    # 记下探测开始的时刻，总预算计时器以它为基准喵
    started_at = time.monotonic()
    # 最近一次收到字节的时刻，静默计时器以它为基准；一开始就是探测开始的时刻喵
    last_byte_at = started_at
    # 到目前为止一共收到了多少字节，用于写出说得清楚的失败说明喵
    total_bytes = 0
    # 一直读到得出结论为止喵
    while True:
        # 算出这次探测总共花了多少秒喵
        elapsed = time.monotonic() - started_at
        # 总预算的剩余量喵
        remaining_total = stream_timeout - elapsed
        # 总预算用完了，判定超时喵
        if remaining_total <= 0:
            # 说明里写清楚「连接一直有动静但正文没来」，这和挂死是两种完全不同的故障喵
            return PROBE_TIMEOUT, (
                f"流式请求超过总预算 {stream_timeout:.0f} 秒仍未拿到足够内容"
                f"（期间共收到 {total_bytes} 字节、{probe.content_chars} 个内容字符，"
                f"连接一直是活的，只是太慢了）"
            )
        # 算出距离上次收到字节过了多久喵
        silent_for = time.monotonic() - last_byte_at
        # 静默额度的剩余量喵
        remaining_stall = stall_timeout - silent_for
        # 静默太久了，判定卡流喵
        if remaining_stall <= 0:
            # 说明里区分「一个字节都没来过」和「来过字节后才挂死」，这俩排查方向不一样喵
            if total_bytes == 0:
                return PROBE_STALLED, (
                    f"上游返回了 200 但连续 {stall_timeout:.0f} 秒一个字节都没发过来"
                    f"（连响应头之后的第一个字节都没有，像是连接挂死了）"
                )
            # 之前有过字节，说明是发了一段之后才挂死的喵
            return PROBE_STALLED, (
                f"上游发过 {total_bytes} 字节（{probe.content_chars} 个内容字符）之后"
                f"连续 {stall_timeout:.0f} 秒再没有任何动静，判定为卡流"
            )
        # 本次读取允许等待的预算，取两个计时器里更紧的那个喵
        budget = min(remaining_total, remaining_stall)
        # 读下一个字节块，套上算好的预算喵
        try:
            chunk = await asyncio.wait_for(iterator.__anext__(), timeout=budget)
        # 迭代器正常结束，说明上游把流吐完了，交给 finish() 做最终判定喵
        except StopAsyncIteration:
            return probe.finish(), probe.detail
        # 喵~防御：本次读取超时。这里必须当场给出结论、绝不能 continue 回到循环顶部喵。
        #
        # 为什么：asyncio.wait_for 超时时会取消它包着的那个协程，也就是把
        # iterator.__anext__() 取消掉。异步生成器被取消之后就报废了 —— 再调一次
        # __anext__() 不会继续读，而是直接抛 StopAsyncIteration，看起来就像「上游把流
        # 正常吐完了」。于是上面那个 except StopAsyncIteration 分支会去调 probe.finish()，
        # 把一个明明是超时的情况判成「流结束了但没内容」的空流（bad_stream）。
        #
        # 这个坑只在「超时那一刻两个计时器都还差一点点没到」时才踩中，所以表现为偶发 ——
        # 同一个用例跑十次可能只错一次，特别难查。当场给结论就彻底避开了它喵。
        except asyncio.TimeoutError:
            # 哪个计时器先到就报哪一种故障。静默计时器更紧说明是上游挂死了喵
            if remaining_stall <= remaining_total:
                # 区分「一个字节都没来过」和「来过字节后才挂死」，排查方向不一样喵
                if total_bytes == 0:
                    return PROBE_STALLED, (
                        f"上游返回了 200 但连续 {stall_timeout:.0f} 秒一个字节都没发过来"
                        f"（连响应头之后的第一个字节都没有，像是连接挂死了）"
                    )
                # 之前有过字节，说明是发了一段之后才挂死的喵
                return PROBE_STALLED, (
                    f"上游发过 {total_bytes} 字节（{probe.content_chars} 个内容字符）之后"
                    f"连续 {stall_timeout:.0f} 秒再没有任何动静，判定为卡流"
                )
            # 总预算先到，说明连接一直活着但就是太慢喵
            return PROBE_TIMEOUT, (
                f"流式请求超过总预算 {stream_timeout:.0f} 秒仍未拿到足够内容"
                f"（期间共收到 {total_bytes} 字节、{probe.content_chars} 个内容字符，"
                f"连接一直是活的，只是太慢了）"
            )
        # 喵~防御：空字节块不算「有动静」，不重置静默计时器。
        # 否则一个疯狂吐空块的上游能让静默计时器永远不触发，白白耗掉整个总预算喵
        if chunk:
            # 收到了真字节，静默计时器归零重新数喵
            last_byte_at = time.monotonic()
            # 累加收到的字节数喵
            total_bytes += len(chunk)
        # 先把原始字节存好，replay 时要用喵
        buffered.extend(chunk)
        # 再喂给探测器做判断喵
        verdict = probe.feed(chunk)
        # 有明确结论就立刻返回；此时迭代器停在这一块之后，后续字节一个都没丢喵
        if verdict != VERDICT_PENDING:
            return verdict, probe.detail
        # 喵~防御：缓冲区超过硬上限说明上游一直在吐没用的东西，判定为不健康的流喵
        if len(buffered) > MAX_PROBE_BUFFER_BYTES:
            return PROBE_BUFFER_OVERFLOW, (
                f"上游在吐出足够内容前已发送超过 {MAX_PROBE_BUFFER_BYTES} 字节"
            )


async def _attempt_stream(
    client: httpx.AsyncClient,
    candidate: Candidate,
    method: str,
    url: str,
    headers: dict[str, str],
    body: bytes,
    timeouts: EffectiveTimeouts,
    min_content_chars: int,
) -> AttemptResult:
    """
    走流式路径打一次上游喵~

    成功的定义：HTTP 200，且在总预算 stream_timeout 之内探测到「这条流是健康的」，
    期间上游的静默时长也没超过 stall_timeout。
    失败时会就地关掉响应连接，客户端完全感知不到这次尝试发生过喵。

    注意总预算只管到「放行」为止。一旦确认健康、字节开始流向客户端，我们就不再计时了 ——
    模型愿意写多久就写多久，中途掐断一个正在正常输出的回答是最糟糕的行为喵。
    """
    # 记下发起请求的时刻。总预算是从这里开始算的，包含建连和等响应头的时间，
    # 否则「连了 4 分钟才拿到响应头」这种情况会白白绕过预算喵
    request_started_at = time.monotonic()
    # 构造请求对象，这里还没真正发出去喵
    # 超时按这次生效的值现算并显式传进去，这样改全局超时或节点专属超时都能当场生效喵
    request = client.build_request(
        method, url, headers=headers, content=body, timeout=build_timeout(timeouts, is_stream=True)
    )
    # 发出请求并要求流式接收（不自动读完 body）喵
    try:
        response = await client.send(request, stream=True)
    # 喵~防御：连接超时单独归为网络错误。连不上是「这个上游此刻不可达」，
    # 和「上游可达但很慢」是两种故障，规则上通常也想区别对待喵
    except httpx.ConnectTimeout as exc:
        return AttemptResult(
            ok=False,
            status=STATUS_NETWORK_ERROR,
            error_text=f"连接上游超时（{timeouts.connect:.0f} 秒内没握上手）：{exc}",
        )
    # 喵~防御：其余超时（读、写、连接池排队）说明上游可达但太慢，归为 timeout 状态喵
    except httpx.TimeoutException as exc:
        return AttemptResult(
            ok=False,
            status=STATUS_TIMEOUT,
            error_text=f"等上游响应头超时：{type(exc).__name__}: {exc}",
        )
    # 喵~防御：连接失败、DNS 失败等网络层问题统一转成 network 状态喵
    except (httpx.HTTPError, OSError) as exc:
        return AttemptResult(
            # 这次尝试失败喵
            ok=False,
            # 用网络失败的特殊状态码，好让规则里的 status: network 能命中喵
            status=STATUS_NETWORK_ERROR,
            # 带上异常类型和消息，方便排查喵
            error_text=f"网络错误：{type(exc).__name__}: {exc}",
        )
    # 上游直接返回了非 200，说明请求阶段就被拒了，读完错误体后交给规则引擎判断喵
    if response.status_code != 200:
        # 用 try/finally 保证无论读取是否成功都会关掉连接，避免连接泄漏喵
        try:
            # 把错误响应体完整读出来，规则引擎要拿它做正则匹配喵
            error_body = await response.aread()
        # 喵~防御：连错误体都读不出来时用空字节兜底，不让异常穿透喵
        except (httpx.HTTPError, OSError):
            error_body = b""
        # 无论如何都要关掉响应，释放连接池里的槽位喵
        finally:
            await response.aclose()
        # 把错误体按 utf-8 解码，遇到非法字节用替换符而不是抛异常喵
        text = error_body.decode("utf-8", errors="replace")
        # 返回失败结果，带上状态码、错误摘要和 Retry-After 头喵
        return AttemptResult(
            # 失败喵
            ok=False,
            # 上游的真实状态码喵
            status=response.status_code,
            # 原始错误文本，规则引擎要用它做正则匹配，所以不做压缩只做解码喵
            error_text=text,
            # 上游可能通过这个头告诉我们多久后可以再试喵
            retry_after=response.headers.get("retry-after"),
        )
    # 走到这里是 200，开始探测这条流到底是真健康、假成功、还是卡住了喵
    # 字数门槛从配置里取，这样主人改了 min_content_chars 能立即生效喵
    probe = StreamProbe(min_content_chars=min_content_chars)
    # 暂存探测阶段读到的所有原始字节，确认健康后要原样replay给客户端喵
    buffered = bytearray()
    # 拿到字节迭代器，整个请求全程只创建这一个，探测和转发共用它喵
    iterator = response.aiter_bytes()
    # 算出探测阶段还剩多少总预算：总预算减掉建连和等响应头已经花掉的时间喵
    probe_budget = timeouts.stream - (time.monotonic() - request_started_at)
    # 喵~防御：建连和等响应头就把预算耗光了（比如上游 4 分钟才给响应头，预算只有 5 分钟），
    # 此时不要用一个负数或 0 去调探测函数，直接判超时更清楚喵
    if probe_budget <= 0:
        # 关掉连接释放槽位喵
        await response.aclose()
        # 返回超时结果，说明里写清楚是「响应头就来得太晚」喵
        return AttemptResult(
            ok=False,
            status=STATUS_TIMEOUT,
            error_text=(
                f"等上游响应头就用光了整个 {timeouts.stream:.0f} 秒总预算，来不及读流内容"
            ),
        )
    # 开始探测。两个计时器都在函数内部处理，所以这里不再额外套 wait_for 喵
    try:
        verdict, detail = await _probe_until_content(
            # 字节迭代器喵
            iterator,
            # 探测器喵
            probe,
            # 原始字节缓冲区喵
            buffered,
            # 允许上游静默多少秒喵
            timeouts.stall,
            # 探测阶段还剩的总预算喵
            probe_budget,
        )
    # 喵~防御：读流时超时（httpx 自己的 read 超时先响了），归为卡流。
    # 因为 read 超时的语义正好就是「两次读取之间隔太久」，和我们的静默判定是一回事喵
    except httpx.TimeoutException as exc:
        # 关掉连接喵
        await response.aclose()
        # 返回卡流结果喵
        return AttemptResult(
            ok=False,
            status=STATUS_STALLED_STREAM,
            error_text=f"读流时上游静默超时：{type(exc).__name__}: {exc}",
        )
    # 喵~防御：探测过程中上游把连接掐断了，归为网络错误喵
    except (httpx.HTTPError, OSError) as exc:
        # 关掉连接喵
        await response.aclose()
        # 返回网络失败结果喵
        return AttemptResult(
            ok=False,
            status=STATUS_NETWORK_ERROR,
            error_text=f"读取流时网络错误：{type(exc).__name__}: {exc}",
        )
    # 探测到有效内容，这条流是健康的，可以放行给客户端了喵
    if verdict == VERDICT_CONTENT:
        return AttemptResult(
            # 成功喵
            ok=True,
            # 状态码就是 200 喵
            status=200,
            # 把还活着的响应对象交给 proxy，用完它负责关掉释放连接喵
            response=response,
            # 探测阶段已经读掉的前缀字节，proxy 要先把这些吹给客户端喵
            buffered=bytes(buffered),
            # 把探测用的迭代器一起带出去，转发阶段从它停下的地方接着读喵
            iterator=iterator,
            # 上游的响应头，过滤掉逐跳头后回传给客户端喵
            headers=_filter_response_headers(response),
        )
    # 走到这里说明这条流不能用了，先把连接关掉释放连接池槽位喵
    await response.aclose()
    # 卡流单独用一个状态码，因为它的处置方式和别的不一样喵：
    # 卡流是「等不到结论」，原地重发一次很可能就好了；而空流/error 是「已经确定坏了」，
    # 重发同一个上游大概率还是坏的，换候选更划算。所以两者要能被不同规则分别匹配喵。
    if verdict == PROBE_STALLED:
        # 返回卡流状态，让规则里的 status: stalled_stream 能命中喵
        return AttemptResult(
            ok=False,
            status=STATUS_STALLED_STREAM,
            error_text=detail or "上游的流卡住了",
        )
    # 用光总预算也单独用一个状态码。它和卡流的区别是「连接一直健康、只是太慢」，
    # 所以规则上可能想给它更少的重试次数（毕竟已经等了很久了）喵
    if verdict == PROBE_TIMEOUT:
        # 返回超时状态，让规则里的 status: timeout 能命中喵
        return AttemptResult(
            ok=False,
            status=STATUS_TIMEOUT,
            error_text=detail or "流式请求用光了总预算",
        )
    # 其余情况（空流、流内 error、缓冲区溢出）都归为「200 假成功」喵
    return AttemptResult(
        ok=False,
        status=STATUS_BAD_STREAM,
        error_text=detail or "上游返回 200 但流内容不正常",
    )


def _nonstream_fake_success(body: bytes) -> str:
    """
    检查非流式的 200 响应是不是「假成功」喵~

    有些中转站在额度不足或后端报错时，仍然回 200，把真正的错误塞进 body 的 error 字段。
    这里只做一个非常保守的检查：顶层有非空 error 字段就算假成功。
    不去检查 choices 是否为空之类的更激进的条件，因为那可能误伤合法的边缘响应喵。

    输入：响应体字节
    输出：假成功时返回错误说明文本，正常时返回空字符串
    """
    # 尝试解析 JSON，非 JSON 响应一律视为正常（可能是上游的特殊接口）喵
    try:
        obj = json.loads(body.decode("utf-8", errors="replace"))
    # 喵~防御：解析失败不代表出错，保守地判为正常，避免误伤非 JSON 接口喵
    except (json.JSONDecodeError, ValueError):
        return ""
    # 喵~防御：顶层不是字典时无法判断，保守判为正常喵
    if not isinstance(obj, dict):
        return ""
    # 取出顶层 error 字段喵
    error = obj.get("error")
    # 没有 error 字段说明是正常响应喵
    if not error:
        return ""
    # error 可能是字典也可能是字符串，分别提取可读消息喵
    message = error.get("message") if isinstance(error, dict) else str(error)
    # 返回错误说明，空消息时用兜底文案喵
    return str(message or "上游返回 200 但响应体里带有 error 字段")


async def _attempt_nonstream(
    client: httpx.AsyncClient,
    method: str,
    url: str,
    headers: dict[str, str],
    body: bytes,
    timeouts: EffectiveTimeouts,
) -> AttemptResult:
    """
    走非流式路径打一次上游喵~

    成功的定义：在 nonstream_timeout 总预算之内拿到 HTTP 200，且响应体里没有 error 字段。

    为什么非流式的预算天生要比流式大：
        流式是「一点点吐」，所以我们能在几秒内就判断出这条流健不健康，剩下的时间交给客户端。
        非流式是「上游把整篇憋完再一次性返回」，在它返回之前我们什么都看不到，
        必须一直等。一个长回答加上思考时间，十几分钟都可能，所以默认给 600 秒喵。
    """
    # 发请求并一次性读完整个响应喵
    try:
        response = await asyncio.wait_for(
            # 普通的非流式请求，超时同样按这次生效的值现算后显式传进去喵
            client.request(
                method,
                url,
                headers=headers,
                content=body,
                timeout=build_timeout(timeouts, is_stream=False),
            ),
            # 整个请求的总预算，作为 httpx 自身超时之外的第二道保险喵
            timeout=timeouts.nonstream,
        )
    # 喵~防御：总预算用完了，归为 timeout 状态，让规则可以给它安排重发喵
    except asyncio.TimeoutError:
        return AttemptResult(
            ok=False,
            status=STATUS_TIMEOUT,
            error_text=f"非流式请求超过总预算 {timeouts.nonstream:.0f} 秒仍未拿到完整响应",
        )
    # 喵~防御：连接超时归为网络错误，含义是「这个上游此刻不可达」喵
    except httpx.ConnectTimeout as exc:
        return AttemptResult(
            ok=False,
            status=STATUS_NETWORK_ERROR,
            error_text=f"连接上游超时（{timeouts.connect:.0f} 秒内没握上手）：{exc}",
        )
    # 喵~防御：其余超时（读、写、连接池排队）说明上游可达但太慢，归为 timeout 状态喵
    except httpx.TimeoutException as exc:
        return AttemptResult(
            ok=False,
            status=STATUS_TIMEOUT,
            error_text=f"非流式请求超时：{type(exc).__name__}: {exc}",
        )
    # 喵~防御：连接、DNS、读写等网络层异常统一转成 network 状态喵
    except (httpx.HTTPError, OSError) as exc:
        return AttemptResult(
            ok=False,
            status=STATUS_NETWORK_ERROR,
            error_text=f"网络错误：{type(exc).__name__}: {exc}",
        )
    # 拿到完整的响应体字节喵
    raw = response.content
    # 非 200 直接判失败，把原始错误体交给规则引擎做正则匹配喵
    if response.status_code != 200:
        return AttemptResult(
            ok=False,
            # 上游真实状态码喵
            status=response.status_code,
            # 原始错误文本喵
            error_text=raw.decode("utf-8", errors="replace"),
            # 上游给的重试建议喵
            retry_after=response.headers.get("retry-after"),
        )
    # 是 200，再查一下是不是「200 里塞 error」的假成功喵
    fake = _nonstream_fake_success(raw)
    # 检测到假成功，用 bad_stream 状态返回（这个状态泛指「表面 200 实则失败」）喵
    if fake:
        return AttemptResult(ok=False, status=STATUS_BAD_STREAM, error_text=fake)
    # 真正的成功，把完整响应体和响应头一起交回去喵
    return AttemptResult(
        # 成功喵
        ok=True,
        # 状态码 200 喵
        status=200,
        # 完整响应体字节，proxy 会原样回传给客户端喵
        body=raw,
        # 过滤后的响应头喵
        headers=_filter_response_headers(response),
    )


async def try_candidate(
    client: httpx.AsyncClient,
    candidate: Candidate,
    method: str,
    path: str,
    query: str,
    client_headers: dict[str, str],
    body_obj: dict[str, Any],
    is_stream: bool,
    server_cfg: ServerConfig,
) -> AttemptResult:
    """
    用一个候选打一次上游，这是本模块唯一的公开入口喵~

    输入：
        client        复用的异步 HTTP 客户端
        candidate     要使用的候选（决定 base_url、api_key、真实模型名）
        method        客户端原始请求方法，原样转发
        path          客户端原始请求路径，原样拼在候选的 base_url 后面
        query         客户端原始查询串，原样带上
        client_headers客户端原始请求头
        body_obj      客户端原始请求体（已解析成字典）
        is_stream     这是不是一个流式请求
        server_cfg    超时等服务器配置
    输出：AttemptResult
    """
    # 拼出完整的上游地址：候选的根地址 + 客户端原始路径喵
    url = f"{candidate.base_url}/{path.lstrip('/')}"
    # 有查询串就原样接上，保持完全透传喵
    if query:
        url = f"{url}?{query}"
    # 构造请求头：透传客户端头，但换掉鉴权头喵
    headers = build_upstream_headers(client_headers, candidate)
    # 构造请求体：只把顶层 model 换成候选的真实模型名喵
    body = build_upstream_body(body_obj, candidate)
    # 算出这次实际生效的超时值：节点上配了专属值就用它，没配就用 server 段的全局值喵
    timeouts = resolve_timeouts(server_cfg, candidate)
    # 按是否流式分派到两条不同的路径喵
    if is_stream:
        # 流式路径要先探测这条流健不健康才放行喵
        return await _attempt_stream(
            client,
            candidate,
            method,
            url,
            headers,
            body,
            timeouts,
            server_cfg.min_content_chars,
        )
    # 非流式路径一次读完喵
    return await _attempt_nonstream(client, method, url, headers, body, timeouts)


async def iter_upstream_bytes(result: AttemptResult) -> AsyncIterator[bytes]:
    """
    把一次流式成功的结果变成给客户端用的字节流喵~

    顺序很重要：先把探测阶段已经读掉的前缀字节吐出去，再接着读上游剩下的字节，
    这样客户端收到的字节序列和上游原始输出完全一致，一个字节都不差喵。

    关键点：这里复用的是探测阶段那个迭代器（result.iterator），而不是重新调一次
           response.aiter_bytes()。httpx 的流只能迭代一次，重新调会抛 StreamConsumed；
           而复用同一个迭代器就能从探测停下的位置无缝接着读，既不重复也不丢字节喵。

    边界条件：无论正常读完还是中途出异常，finally 里都会关掉上游响应，
            防止连接泄漏把连接池占满喵。
    """
    # 喵~防御：响应或迭代器缺失时直接结束而不是抛异常，理论上不会发生喵
    if result.response is None or result.iterator is None:
        return
    # 用 try/finally 保证连接一定会被关掉喵
    try:
        # 先吐探测阶段缓冲的前缀字节喵
        if result.buffered:
            yield result.buffered
        # 再从同一个迭代器接着读，把上游剩下的字节一块块转发出去喵
        async for chunk in result.iterator:
            yield chunk
    # 喵~防御：上游中途断连时不再抛给客户端（此时响应头已经发出去了，抛异常也没用），
    # 直接结束这条流，客户端的 SDK 会按「流意外结束」处理喵
    except (httpx.HTTPError, OSError):
        pass
    # 无论如何都要关掉上游响应喵
    finally:
        await result.response.aclose()
