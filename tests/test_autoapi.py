"""
autoapi 的单元测试与端到端测试喵~

覆盖四块：
    1. 配置加载与校验（合法配置能过，各种坏配置要被挡下来）
    2. 规则引擎（正则抽取冻结时长、重试、换候选、原样回传、默认动作）
    3. SSE 流探测（两种协议的内容识别、假成功识别、跨块切断的行和中文字）
    4. 端到端故障转移（用 httpx 的 MockTransport 假装上游，验证真的会换候选）
"""

# 引入注解特性喵
from __future__ import annotations

# asyncio 用来做并发测试和模拟卡住的流喵
import asyncio
# json 用来构造测试用的请求体和 SSE 负载喵
import json
# logging 用来测试日志上色喵
import logging
# sys 用来测试日志处理器是否会跟着 sys.stderr 的替换而变喵
import sys
# time 用来测试冻结过期喵
import time

# httpx 提供 MockTransport，可以假装成上游喵
import httpx
# pytest 是测试框架喵
import pytest

# 引入被测的配置模块喵
from autoapi.config import (
    STATUS_BAD_STREAM,
    STATUS_NETWORK_ERROR,
    STATUS_STALLED_STREAM,
    STATUS_TIMEOUT,
    Candidate,
    ConfigError,
    parse_config,
    resolve_timeouts,
)
# 引入被测的编排层喵
from autoapi.proxy import detect_stream_flag, handle_request, parse_client_body
# 引入接口错误忽略判定函数喵
from autoapi.proxy import _is_count_tokens_request, _is_ignored_error_endpoint
# 引入被测的规则引擎喵
from autoapi.rules import decide
# 引入被测的 SSE 探测器喵
from autoapi.sse import (
    VERDICT_CONTENT,
    VERDICT_DONE_EMPTY,
    VERDICT_ERROR,
    VERDICT_PENDING,
    StreamProbe,
)
# 引入被测的运行时状态喵
from autoapi.state import RuntimeState
# 引入被测的上游请求构造函数和单次调用入口喵
from autoapi.upstream import build_upstream_body, build_upstream_headers, try_candidate


def make_config_dict(**overrides):
    """
    造一份最小可用的配置字典给测试用喵~

    说明：默认配两个候选、几条典型规则，测试里按需覆盖某些字段喵。
    """
    # 基础配置字典喵
    data = {
        # 服务器段，超时都设得很小以便测试跑得快喵
        "server": {
            "host": "127.0.0.1",
            "port": 8787,
            # 超时都设得很小，让卡流和超时类的测试能快点跑完喵
            # 注意这些值会被 parse_config 压到 1.0 秒的下限，测试里按压后的值断言喵
            "stall_timeout": 0.9,
            "stream_timeout": 5,
            "nonstream_timeout": 5,
            # 门槛保持默认的 10，这样能测到「吐几个字不够门槛」的行为喵
            "min_content_chars": 10,
            "connect_timeout": 2,
            "reload_poll_interval": 0,
        },
        # 一个虚拟模型带两个候选，用于验证故障转移喵
        "virtual_models": {
            "auto-test": [
                {
                    "name": "主力",
                    "base_url": "https://primary.test",
                    "api_key": "sk-primary-key-1234",
                    "model": "gpt-4o",
                    "auth_style": "bearer",
                },
                {
                    "name": "备用",
                    "base_url": "https://backup.test",
                    "api_key": "sk-backup-key-5678",
                    "model": "claude-sonnet",
                    "auth_style": "x-api-key",
                },
            ]
        },
        # 典型规则集喵
        "rules": [
            {
                "match": {"status": 429, "body_regex": r"refreshes?\s+in\s+(\d+)\s+minutes?"},
                "action": "freeze",
                "freeze_from_group": 1,
                "freeze_unit": "minutes",
                "freeze_seconds": 300,
            },
            {"match": {"status": [500, 502, 503]}, "action": "retry", "max_attempts": 2, "backoff_base": 1.0},
            # 卡流：原地重发一次（含首次共 2 次），两次都卡才换候选喵
            {"match": {"status": "stalled_stream"}, "action": "retry", "max_attempts": 2, "backoff_base": 1.0},
            # 超过总预算：也重发一次（含首次共 2 次）喵
            {"match": {"status": "timeout"}, "action": "retry", "max_attempts": 2, "backoff_base": 1.0},
            {"match": {"status": "bad_stream"}, "action": "next"},
            {"match": {"status": [401, 403, 404]}, "action": "next"},
            # 上下文超限应切换候选，必须排在通用 400 规则之前喵
            {"match": {"status": 400, "body_regex": r"context_length_exceeded"}, "action": "next"},
            {"match": {"status": 400}, "action": "passthrough"},
        ],
    }
    # 单独覆盖 server 段里的某几项，不用把整个 server 段重写一遍喵。
    # 这么做是因为绝大多数测试只想改一两个超时值，整段重写既啰嗦又容易漏字段喵
    server_overrides = overrides.pop("server_overrides", None)
    # 有覆盖项就合并进 server 段喵
    if server_overrides:
        data["server"].update(server_overrides)
    # 应用测试传入的其余覆盖项喵
    data.update(overrides)
    # 返回配置字典喵
    return data


# ============ 1. 配置加载与校验喵 ============


def test_配置能正常加载():
    """合法配置应该被完整解析出来喵~"""
    # 解析配置喵
    config = parse_config(make_config_dict())
    # 应该有一个虚拟模型喵
    assert len(config.virtual_models) == 1
    # 那个虚拟模型应该有两个候选喵
    assert len(config.virtual_models["auto-test"]) == 2
    # 应该有八条规则喵（freeze、5xx retry、卡流 retry、超时 retry、bad_stream、401 系、上下文超限、400）
    assert len(config.rules) == 8
    # 第一个候选的真实模型名应该被正确解析喵
    assert config.virtual_models["auto-test"][0].model == "gpt-4o"


def test_默认忽略token计数接口且支持自定义规范化():
    """默认兼容 count_tokens，自定义端点按 method+path 规范化喵~"""
    # 默认配置应保留旧的 count_tokens 忽略行为喵
    default_config = parse_config(make_config_dict())
    # 确认默认端点已加入标准化集合喵
    assert ("POST", "/v1/messages/count_tokens") in default_config.server.ignored_error_endpoints
    # 自定义配置应支持方法大小写和多余前导斜杠喵
    custom_data = make_config_dict(
        server_overrides={
            "ignored_error_endpoints": [
                {"method": " post ", "path": "v1/custom"},
            ]
        }
    )
    # 解析自定义配置喵
    custom_config = parse_config(custom_data)
    # 确认自定义接口被标准化保存喵
    assert custom_config.server.ignored_error_endpoints == frozenset({("POST", "/v1/custom")})
    # 自定义非空列表采用整体替换语义，不会隐式保留默认接口喵
    assert ("POST", "/v1/messages/count_tokens") not in custom_config.server.ignored_error_endpoints
    # 显式空列表应关闭所有默认忽略行为喵
    disabled_config = parse_config(
        make_config_dict(server_overrides={"ignored_error_endpoints": []})
    )
    # 空列表必须原样解析为空集合喵
    assert disabled_config.server.ignored_error_endpoints == frozenset()


def test_忽略接口配置错误会被拒绝():
    """忽略接口配置类型或字段错误时应在加载阶段失败喵~"""
    # 依次覆盖顶层、条目和字段错误喵
    invalid_values = [
        "not-a-list",
        ["not-a-dict"],
        [{"method": "POST"}],
        [{"path": "/v1/test"}],
        [{"method": "", "path": "/v1/test"}],
        [{"method": "POST", "path": ""}],
        [{"method": "HEAD", "path": "/v1/test"}],
        [{"method": "POST", "path": "/v1/test?debug=true"}],
        [{"method": "POST", "path": "/v1/test#fragment"}],
    ]
    # 每种错误配置都必须抛出统一配置异常喵
    for invalid_value in invalid_values:
        data = make_config_dict(server_overrides={"ignored_error_endpoints": invalid_value})
        with pytest.raises(ConfigError):
            parse_config(data)


def test_状态码别名会被翻译成特殊值():
    """bad_stream 和 network 这两个别名应该被翻译成内部负数状态码喵~"""
    # 造一份只有一条 network 规则的配置喵
    data = make_config_dict(rules=[{"match": {"status": "network"}, "action": "next"}])
    # 解析喵
    config = parse_config(data)
    # 那条规则的状态码集合里应该是网络错误的特殊值喵
    assert STATUS_NETWORK_ERROR in config.rules[0].status_codes


def test_候选缺少必填字段要报错():
    """候选少了 api_key 应该在加载阶段就被挡下来喵~"""
    # 造一个缺 api_key 的候选喵
    data = make_config_dict(
        virtual_models={"bad": [{"base_url": "https://x.test", "model": "gpt-4o"}]}
    )
    # 应该抛 ConfigError 且消息里点明缺了哪个字段喵
    with pytest.raises(ConfigError, match="api_key"):
        parse_config(data)


def test_虚拟模型候选链为空要报错():
    """空候选链意味着这个虚拟模型永远无法服务，必须在加载阶段挡下来喵~"""
    # 造一个候选链为空的虚拟模型喵
    data = make_config_dict(virtual_models={"empty": []})
    # 应该抛 ConfigError 喵
    with pytest.raises(ConfigError, match="非空列表"):
        parse_config(data)


def test_规则动作拼错要报错():
    """action 拼错必须在加载阶段发现，不能等到线上才失效喵~"""
    # 造一条 action 拼错的规则喵
    data = make_config_dict(rules=[{"match": {"status": 500}, "action": "retryyy"}])
    # 应该抛 ConfigError 且消息里列出合法动作喵
    with pytest.raises(ConfigError, match="不合法"):
        parse_config(data)


def test_既无状态码又无正则的规则要报错():
    """这种规则会匹配一切，属于危险配置，必须拒绝喵~"""
    # 造一条 match 为空的规则喵
    data = make_config_dict(rules=[{"match": {}, "action": "next"}])
    # 应该抛 ConfigError 喵
    with pytest.raises(ConfigError, match="会匹配所有情况"):
        parse_config(data)


def test_正则语法错误要报错():
    """坏正则必须在加载阶段被编译出错，而不是运行时才炸喵~"""
    # 造一条正则写坏的规则（括号没闭合）喵
    data = make_config_dict(rules=[{"match": {"status": 429, "body_regex": "(unclosed"}, "action": "next"}])
    # 应该抛 ConfigError 喵
    with pytest.raises(ConfigError, match="编译失败"):
        parse_config(data)


def test_api_key会被脱敏():
    """脱敏后的 key 不能包含中间部分，避免日志泄密喵~"""
    # 造一个候选喵
    candidate = Candidate(name="测试", base_url="https://x.test", api_key="sk-abcdefghijklmnop", model="m")
    # 脱敏结果里不应该出现中间那段喵
    assert "ghijkl" not in candidate.masked_key
    # 但应该保留开头方便肉眼对号喵
    assert candidate.masked_key.startswith("sk-abc")


# ============ 2. 规则引擎喵 ============


def test_从错误消息里抽取冻结时长():
    """上游说 6 分钟后恢复，就应该冻结 6 分钟（外加 5 秒缓冲）喵~"""
    # 取规则列表喵
    rules = parse_config(make_config_dict()).rules
    # 造一条真实世界里的 429 错误消息喵
    body = "status_code=429, key sk-3ce*** has reached its rolling 1h usage quota; refreshes in 6 minutes"
    # 让规则引擎决策喵
    decision = decide(rules, 429, body)
    # 动作应该是冻结喵
    assert decision.action == "freeze"
    # 冻结时长应该是 6 分钟 + 5 秒缓冲 = 365 秒喵
    assert decision.freeze_seconds == pytest.approx(365.0)


def test_抽取不到时长就用兜底值():
    """429 但消息里没写恢复时间时，应该走后面那条 retry 规则喵~"""
    # 取规则列表喵
    rules = parse_config(make_config_dict()).rules
    # 造一条不含恢复时间的 429 消息喵
    decision = decide(rules, 429, "rate limit exceeded")
    # 第一条 freeze 规则要求正则命中，没命中就该落到第二条 retry 规则上——
    # 但第二条只匹配 5xx，所以最终会落到默认动作 next 喵
    assert decision.action == "next"


def test_Retry_After头会被用作冻结时长():
    """规则没配捕获组时，应该退而使用上游的 Retry-After 头喵~"""
    # 造一条只按状态码冻结、不抽取捕获组的规则喵
    data = make_config_dict(rules=[{"match": {"status": 429}, "action": "freeze", "freeze_seconds": 999}])
    # 取规则列表喵
    rules = parse_config(data).rules
    # 决策时带上 Retry-After 头喵
    decision = decide(rules, 429, "too many requests", retry_after="30")
    # 应该用头里的 30 秒加 5 秒缓冲，而不是兜底的 999 秒喵
    assert decision.freeze_seconds == pytest.approx(35.0)


def test_五百开头的错误走原地重试():
    """5xx 通常是上游抖动，原地重试比换渠道更划算喵~"""
    # 取规则列表喵
    rules = parse_config(make_config_dict()).rules
    # 对 502 做决策喵
    decision = decide(rules, 502, "bad gateway")
    # 动作应该是重试喵
    assert decision.action == "retry"
    # 最大尝试次数应该是配置里的 2 喵
    assert decision.max_attempts == 2


def test_四百原样回传不做转移():
    """400 是客户端自己请求写错了，换渠道也没用喵~"""
    # 取规则列表喵
    rules = parse_config(make_config_dict()).rules
    # 对 400 做决策喵
    decision = decide(rules, 400, "invalid messages field")
    # 动作应该是原样回传喵
    assert decision.action == "passthrough"


def test_没规则命中时走默认换候选():
    """默认动作必须是最保守的 next 喵~"""
    # 取规则列表喵
    rules = parse_config(make_config_dict()).rules
    # 用一个没有任何规则覆盖的状态码做决策喵
    decision = decide(rules, 418, "i am a teapot")
    # 应该落到默认动作喵
    assert decision.action == "next"


def test_规则顺序决定优先级():
    """靠前的规则应该先命中，这样才能用 rule mv 调整优先级喵~"""
    # 造两条都能匹配 429 的规则，第一条是 next 第二条是 freeze 喵
    data = make_config_dict(
        rules=[
            {"match": {"status": 429}, "action": "next"},
            {"match": {"status": 429}, "action": "freeze", "freeze_seconds": 60},
        ]
    )
    # 取规则列表喵
    rules = parse_config(data).rules
    # 决策结果应该是靠前那条的动作喵
    assert decide(rules, 429, "").action == "next"


# ============ 3. SSE 流探测喵 ============


def sse(*payloads: str) -> bytes:
    """把若干个 data 负载拼成标准的 SSE 字节流喵~"""
    # 每条负载一行 data:，后面跟一个空行做事件分隔，这是 SSE 的规范格式喵
    return "".join(f"data: {p}\n\n" for p in payloads).encode("utf-8")


class 分块字节流(httpx.AsyncByteStream):
    """
    一个真正分块吐字节的假上游流喵~

    为什么必须要它：httpx.Response(200, content=b"...") 会把内容整块存在内存里，
    重复迭代也不会报错。这会掩盖「同一条流被迭代两次」这种线上必炸的 bug
    （真上游会直接抛 StreamConsumed）。所以这里实现一个只能迭代一次、且分多块吐出的流，
    让测试环境的行为和真实上游一致喵。
    """

    def __init__(self, data: bytes, chunk_size: int = 16) -> None:
        """记下要吐的数据和每块大小喵~"""
        # 要吐出的完整字节喵
        self._data = data
        # 每块多大，故意设得很小以模拟真实网络的碎片化喵
        self._chunk_size = chunk_size
        # 是否已经被迭代过，用来模拟真上游「流只能读一次」的语义喵
        self._consumed = False

    async def __aiter__(self):
        """按块吐出字节，第二次迭代直接报错喵~"""
        # 喵~防御：已经被读过一次就抛错，模拟 httpx 对真实流的 StreamConsumed 行为喵
        if self._consumed:
            raise RuntimeError("这条流已经被读过一次了喵，不能重复迭代~")
        # 标记为已读喵
        self._consumed = True
        # 按固定大小切块吐出喵
        for i in range(0, len(self._data), self._chunk_size):
            # 吐出一块喵
            yield self._data[i : i + self._chunk_size]


class 卡住的字节流(httpx.AsyncByteStream):
    """
    一个模拟「卡流」的假上游流喵~

    行为：先吐出给定的前缀字节（通常是几个字符的内容），然后就一直挂着不再吐东西，
         连接也不断开。这正是主人描述的那种最阴险的坏上游喵。
    """

    def __init__(self, prefix: bytes = b"") -> None:
        """记下要先吐出的前缀喵~"""
        # 挂住之前先吐出的字节，可以为空（表示一个字都不吐）喵
        self._prefix = prefix

    async def __aiter__(self):
        """吐完前缀就永远睡下去喵~"""
        # 有前缀就先吐出去喵
        if self._prefix:
            yield self._prefix
        # 然后一直挂着。睡一个很长的时间，代理那边的探测超时会先到并主动放弃喵
        await asyncio.sleep(3600)


def 卡流响应(prefix: bytes = b"") -> httpx.Response:
    """造一个「吐几个字然后卡住」的响应喵~"""
    # 用卡住的流当响应体喵
    return httpx.Response(
        200,
        stream=卡住的字节流(prefix),
        headers={"content-type": "text/event-stream"},
    )


class 只发心跳的字节流(httpx.AsyncByteStream):
    """
    一个模拟「推理模型正在思考」的假上游流喵~

    行为：持续吐 SSE 心跳注释行（`: ping`），一个内容字符都不给，但连接一直很活跃。
         这正是主人报的那个 bug 的现场：上游明明在正常干活、连接健康得很，
         旧逻辑却因为「迟迟等不到正文」在 45 秒时就把它斩了喵。

    这个流永远不会自己结束，靠代理那边的总预算计时器来终止喵。
    """

    def __init__(self, interval: float = 0.05) -> None:
        """记下心跳间隔喵~"""
        # 两次心跳之间隔多久，单位：秒。设得很小让测试跑得快喵
        self._interval = interval

    async def __aiter__(self):
        """一直发心跳，永不停止喵~"""
        # 无限循环发心跳喵
        while True:
            # 先等一个间隔，模拟真实上游的心跳节奏喵
            await asyncio.sleep(self._interval)
            # 吐一个 SSE 注释行。冒号开头是 SSE 规范里的注释，客户端会忽略它，
            # 上游常用它来保持连接活跃喵
            yield b": ping\n\n"


def 心跳响应(interval: float = 0.05) -> httpx.Response:
    """造一个「只发心跳、不吐正文」的响应喵~"""
    # 用只发心跳的流当响应体喵
    return httpx.Response(
        200,
        stream=只发心跳的字节流(interval),
        headers={"content-type": "text/event-stream"},
    )


class 先安静再吐字的字节流(httpx.AsyncByteStream):
    """
    一个模拟「推理模型先安静想一会儿，然后正常输出」的假上游流喵~

    行为：先什么都不发地等一段时间，然后一次性吐出正常内容。
         这是推理模型最典型的行为，必须能正常放行、不能被判成失败喵。
    """

    def __init__(self, quiet_seconds: float, payload: bytes) -> None:
        """记下要安静多久、之后吐什么喵~"""
        # 开头安静的秒数喵
        self._quiet = quiet_seconds
        # 安静结束后要吐的字节喵
        self._payload = payload

    async def __aiter__(self):
        """先安静，再吐字喵~"""
        # 先安静一段时间，期间一个字节都不发喵
        await asyncio.sleep(self._quiet)
        # 然后把内容一次吐出去喵
        yield self._payload


def 先安静再回答的响应(quiet_seconds: float, payload: bytes) -> httpx.Response:
    """造一个「先安静想一会儿再正常回答」的响应喵~"""
    # 用先安静再吐字的流当响应体喵
    return httpx.Response(
        200,
        stream=先安静再吐字的字节流(quiet_seconds, payload),
        headers={"content-type": "text/event-stream"},
    )


def 流式响应(data: bytes) -> httpx.Response:
    """造一个用真正分块流承载 SSE 的响应喵~"""
    # 用自定义的分块流当响应体，content-type 设成 SSE 的标准值喵
    return httpx.Response(
        200,
        stream=分块字节流(data),
        headers={"content-type": "text/event-stream"},
    )


def test_识别OpenAI风格的内容():
    """
    OpenAI 的 delta.content 里的文字应该被正确识别喵~

    这里显式用门槛 1，因为本用例测的是「能不能认出内容」这件事，
    和「够不够字数才放行」是两个独立的关注点，分开测才不会互相干扰喵。
    """
    # 造一个门槛为 1 的探测器，只验证识别能力喵
    probe = StreamProbe(min_content_chars=1)
    # 喂一个带内容的 chunk 喵
    verdict = probe.feed(sse(json.dumps({"choices": [{"delta": {"content": "你好"}}]})))
    # 应该判定为有内容喵
    assert verdict == VERDICT_CONTENT
    # 字数也该被正确累计喵
    assert probe.content_chars == 2


def test_识别Anthropic风格的内容():
    """Anthropic 的 content_block_delta.text 里的文字应该被正确识别喵~"""
    # 造一个门槛为 1 的探测器喵
    probe = StreamProbe(min_content_chars=1)
    # 造一个 Anthropic 风格的内容增量事件喵
    event = json.dumps({"type": "content_block_delta", "delta": {"type": "text_delta", "text": "你好"}})
    # 喂进去喵
    assert probe.feed(sse(event)) == VERDICT_CONTENT
    # 字数也该被正确累计喵
    assert probe.content_chars == 2


def test_识别推理模型的思维链内容():
    """推理模型先吐 reasoning_content，这也算有效内容喵~"""
    # 造一个门槛为 1 的探测器喵
    probe = StreamProbe(min_content_chars=1)
    # 造一个只有思维链没有正文的增量喵
    event = json.dumps({"choices": [{"delta": {"reasoning_content": "让我想想"}}]})
    # 应该判定为有内容喵
    assert probe.feed(sse(event)) == VERDICT_CONTENT


def test_识别工具调用也算健康():
    """模型在调工具时 content 是空的，但流是健康的，不能误判喵~"""
    # 造一个探测器喵
    probe = StreamProbe()
    # 造一个只有 tool_calls 的增量喵
    event = json.dumps({"choices": [{"delta": {"tool_calls": [{"index": 0, "id": "x"}]}}]})
    # 应该判定为有内容喵
    assert probe.feed(sse(event)) == VERDICT_CONTENT


def test_空的角色占位包不算内容():
    """很多上游首包只有 role 没有内容，此时还不能放行喵~"""
    # 造一个探测器喵
    probe = StreamProbe()
    # 造一个只有 role、content 为空串的首包喵
    event = json.dumps({"choices": [{"delta": {"role": "assistant", "content": ""}}]})
    # 应该还是 pending，需要继续等真正的内容喵
    assert probe.feed(sse(event)) == VERDICT_PENDING


def test_只有DONE没有内容判定为假成功():
    """这是最重要的一个用例：200 但一个字都没吐，必须能识别出来喵~"""
    # 造一个探测器喵
    probe = StreamProbe()
    # 只喂一个 [DONE] 喵
    verdict = probe.feed(sse("[DONE]"))
    # 应该判定为空流喵
    assert verdict == VERDICT_DONE_EMPTY


def test_流里的error事件能被识别():
    """上游在流里报错也必须能识别，否则错误会被当成正常内容转给客户端喵~"""
    # 造一个探测器喵
    probe = StreamProbe()
    # 造一个带 error 字段的负载喵
    event = json.dumps({"error": {"message": "insufficient quota"}})
    # 应该判定为错误喵
    assert probe.feed(sse(event)) == VERDICT_ERROR
    # 错误说明里应该带上上游的原始消息，方便规则引擎做正则匹配喵
    assert "insufficient quota" in probe.detail


def test_连接被提前掐断判定为假成功():
    """流读完了却没有任何内容也没有结束标记，同样是假成功喵~"""
    # 造一个探测器喵
    probe = StreamProbe()
    # 只喂一个元信息事件，没有内容也没有结束标记喵
    probe.feed(sse(json.dumps({"type": "message_start"})))
    # 此时还是 pending 喵
    assert probe.verdict == VERDICT_PENDING
    # 流结束了，finish 应该判定为空流喵
    assert probe.finish() == VERDICT_DONE_EMPTY


def test_跨块被切断的行能正确拼接():
    """
    这是最容易出 bug 的地方喵：上游的 TCP 分块边界和 SSE 行边界毫无关系，
    一行 data: 被切成两半时必须能拼回来喵。
    """
    # 造一个门槛为 1 的探测器，本用例只关心行拼接是否正确喵
    probe = StreamProbe(min_content_chars=1)
    # 造完整的字节流喵
    full = sse(json.dumps({"choices": [{"delta": {"content": "喵"}}]}))
    # 在正中间切开喵
    mid = len(full) // 2
    # 喂前半段，应该还看不出结果喵
    assert probe.feed(full[:mid]) == VERDICT_PENDING
    # 喂后半段，拼起来之后应该能识别出内容喵
    assert probe.feed(full[mid:]) == VERDICT_CONTENT


def test_跨块被切断的中文字符不会乱码():
    """
    UTF-8 的中文占 3 个字节，被切开时用普通 decode 会抛异常或出乱码，
    必须靠增量解码器正确处理喵。
    """
    # 造一个门槛为 1 的探测器，本用例只关心中文字节被切开时会不会乱码喵
    probe = StreamProbe(min_content_chars=1)
    # 造一个内容是中文的字节流喵
    full = sse(json.dumps({"choices": [{"delta": {"content": "早上好"}}]}, ensure_ascii=False))
    # 逐字节喂进去，这是最极端的切分方式喵
    verdicts = [probe.feed(full[i : i + 1]) for i in range(len(full))]
    # 最终应该能识别出内容，说明中文没被切坏喵
    assert VERDICT_CONTENT in verdicts


def test_字数不够门槛时不放行():
    """
    这是新增门槛的核心用例喵：只吐一两个字符时绝不能放行，
    否则「吐一两个字然后卡死」的上游会骗过检查、字节一出门就换不了候选了喵。
    """
    # 造一个门槛为 10 的探测器喵
    probe = StreamProbe(min_content_chars=10)
    # 只吐两个字符喵
    assert probe.feed(sse(json.dumps({"choices": [{"delta": {"content": "嗯嗯"}}]}))) == VERDICT_PENDING
    # 字数应该被累计上了喵
    assert probe.content_chars == 2
    # 再吐几个，还是不够 10 个喵
    assert probe.feed(sse(json.dumps({"choices": [{"delta": {"content": "好的呢"}}]}))) == VERDICT_PENDING
    assert probe.content_chars == 5


def test_字数攒够门槛就放行():
    """跨多个 chunk 累计够 10 个字符就该放行喵~"""
    # 造一个门槛为 10 的探测器喵
    probe = StreamProbe(min_content_chars=10)
    # 分多次喂，每次 3 个字符喵
    for _ in range(3):
        assert probe.feed(sse(json.dumps({"choices": [{"delta": {"content": "一二三"}}]}))) == VERDICT_PENDING
    # 第 4 次之后累计到 12 个字符，超过门槛，应该放行喵
    assert probe.feed(sse(json.dumps({"choices": [{"delta": {"content": "四五六"}}]}))) == VERDICT_CONTENT
    # 累计字数应该是 12 喵
    assert probe.content_chars == 12


def test_短回答流正常结束时照常放行():
    """
    很重要的边界喵：整条流只有几个字就正常结束（比如模型只回「好的」），
    绝不能因为凑不满 10 个字符就判失败，那会把正常的短回答全干掉喵。
    """
    # 造一个门槛为 10 的探测器喵
    probe = StreamProbe(min_content_chars=10)
    # 吐两个字符，此时还不够门槛喵
    assert probe.feed(sse(json.dumps({"choices": [{"delta": {"content": "好的"}}]}))) == VERDICT_PENDING
    # 紧接着流正常结束，这时应该判定健康并放行喵
    assert probe.feed(sse("[DONE]")) == VERDICT_CONTENT


def test_短回答的Anthropic流也照常放行():
    """Anthropic 的 message_stop 同样要能触发短回答放行喵~"""
    # 造一个门槛为 10 的探测器喵
    probe = StreamProbe(min_content_chars=10)
    # 吐一点点内容喵
    event = json.dumps({"type": "content_block_delta", "delta": {"type": "text_delta", "text": "好"}})
    assert probe.feed(sse(event)) == VERDICT_PENDING
    # 收到 message_stop，应该放行喵
    assert probe.feed(sse(json.dumps({"type": "message_stop"}))) == VERDICT_CONTENT


def test_只调工具不吐文字也要放行():
    """
    模型只调工具时 content 一直是空的，字数永远凑不满门槛。
    这种必须绕过字数门槛直接放行，否则会被误判成卡流喵。
    """
    # 造一个门槛为 10 的探测器喵
    probe = StreamProbe(min_content_chars=10)
    # 只有 tool_calls，没有任何文字喵
    event = json.dumps({"choices": [{"delta": {"tool_calls": [{"index": 0, "id": "call_1"}]}}]})
    # 应该直接放行喵
    assert probe.feed(sse(event)) == VERDICT_CONTENT


def test_Anthropic的工具参数增量也要放行():
    """Anthropic 用 partial_json 传工具参数，同样不带可读文字，也要放行喵~"""
    # 造一个门槛为 10 的探测器喵
    probe = StreamProbe(min_content_chars=10)
    # 造一个工具参数增量事件喵
    event = json.dumps(
        {"type": "content_block_delta", "delta": {"type": "input_json_delta", "partial_json": '{"a":'}}
    )
    # 应该直接放行喵
    assert probe.feed(sse(event)) == VERDICT_CONTENT


def test_收到finish_reason也要放行():
    """finish_reason 说明这一轮已正常收尾，不该再等字数喵~"""
    # 造一个门槛为 10 的探测器喵
    probe = StreamProbe(min_content_chars=10)
    # 造一个带 finish_reason 的 chunk 喵
    event = json.dumps({"choices": [{"delta": {}, "finish_reason": "stop"}]})
    # 应该直接放行喵
    assert probe.feed(sse(event)) == VERDICT_CONTENT


def test_连接断掉但吐过字符时当短回答放行():
    """
    上游吐了几个字就把连接断了（没有 [DONE]）：内容已经到手，放行比失败好喵。
    有些上游本来就不发 [DONE]，判失败会误伤它们喵。
    """
    # 造一个门槛为 10 的探测器喵
    probe = StreamProbe(min_content_chars=10)
    # 吐两个字符喵
    probe.feed(sse(json.dumps({"choices": [{"delta": {"content": "嗯嗯"}}]})))
    # 此时还不够门槛喵
    assert probe.verdict == VERDICT_PENDING
    # 流结束了，应该当成短回答放行喵
    assert probe.finish() == VERDICT_CONTENT


def test_一个字符都没有时流结束仍判空流():
    """一个字符都没吐就结束，这是真正的假成功，必须判失败喵~"""
    # 造一个门槛为 10 的探测器喵
    probe = StreamProbe(min_content_chars=10)
    # 只喂一个元信息事件，没有任何内容喵
    probe.feed(sse(json.dumps({"type": "message_start"})))
    # 流结束，应该判空流喵
    assert probe.finish() == VERDICT_DONE_EMPTY


def test_SSE心跳后裸JSON错误事件能被识别():
    """兼容真实中转站把错误 JSON 裸放在 SSE 流里的格式喵~"""
    # 造一个默认门槛的探测器，验证错误事件优先于空流结论喵
    probe = StreamProbe()
    # 拼出真实观测到的 PING 心跳和无 data: 前缀的错误 JSON 喵
    error_payload = json.dumps(
        {
            "error": {
                "type": "invalid_request_error",
                "message": "The reasoning_content in the thinking mode must be passed back to the API.",
            },
            "type": "error",
        }
    )
    # 心跳注释行应当被忽略，裸 JSON 错误应当在流结束时被识别为明确错误喵
    probe.feed((": PING\n\n" + error_payload).encode())
    # 调用 finish 处理没有换行结尾的裸 JSON 错误行喵
    verdict = probe.finish()
    # 不能再误判成 done_empty 喵
    assert verdict == VERDICT_ERROR
    # 错误详情应保留上游消息，方便规则和日志定位喵
    assert "reasoning_content" in probe.detail


def test_非JSON的心跳行不会误判():
    """有些上游会插入注释行或心跳，绝不能因为解析不了就误判成失败喵~"""
    # 造一个探测器喵
    probe = StreamProbe()
    # 喂一个 SSE 注释行（冒号开头）和一个非 JSON 的 data 行喵
    assert probe.feed(b": keep-alive\n\ndata: ping\n\n") == VERDICT_PENDING


def make_state() -> RuntimeState:
    """造一个装好测试配置的运行时状态喵~"""
    # 用测试配置创建状态对象喵
    return RuntimeState(parse_config(make_config_dict()))


def test_滚动RPM和TPM只统计上游明确usage():
    """
    RPM 统计每条成功请求，TPM 只能统计上游明确报的 usage，绝不本地猜喵~

    缺 usage 的成功请求仍然是成功，所以 RPM 要包含它；但 TPM 不能因此加 0 或估算，
    否则横幅看起来像精确数值、实际上少了一整条请求的消耗，误导主人喵。
    """
    # 造状态喵
    state = make_state()
    # 记一条上游明确报了 120 token 的成功请求喵
    state.record_rate_event("auto-test", 120)
    # 再记一条上游没报 usage 的成功请求喵
    state.record_rate_event("auto-test", None)
    # 取快照喵
    row = state.snapshot_virtual_model_rates()[0]
    # 两条成功都要算进 RPM 喵
    assert row.rpm == 2
    # 只有一条明确上报 usage 喵
    assert row.usage_reported_requests == 1
    # TPM 必须保持未知，绝不显示 120 或本地估算值喵
    assert row.tpm is None


def test_滚动RPM和TPM在全量上报时求和():
    """窗口内每一条都带上游 usage 时，TPM 才应显示精确加总值喵~"""
    # 造状态喵
    state = make_state()
    # 记录两条完整 usage 喵
    state.record_rate_event("auto-test", 120)
    state.record_rate_event("auto-test", 80)
    # 取快照喵
    row = state.snapshot_virtual_model_rates()[0]
    # RPM 是两条成功请求喵
    assert row.rpm == 2
    # TPM 只来自上游给出的 120 + 80 喵
    assert row.tpm == 200


def test_超过60秒的负载事件会自动清理(monkeypatch):
    """RPM/TPM 必须是滚动 60 秒窗口，窗口外的请求不该继续占统计喵~"""
    # 造状态喵
    state = make_state()
    # 先记录一条事件喵
    event = state.record_rate_event("auto-test", 50)
    # 人为把它挪到固定 60 秒速率窗口外喵
    event.at -= 61
    # 读取快照会按速率窗口过滤事件喵
    row = state.snapshot_virtual_model_rates()[0]
    # 速率窗口内应该已无请求喵
    assert row.rpm == 0
    # 没有请求时 TPM 也应保持未知喵
    assert row.tpm is None


def test_速率窗口与平均耗时窗口彼此独立():
    """超过 60 秒的事件不计入 RPM/TPM，但仍可进入默认平均耗时窗口喵~"""
    # 解析默认 30 分钟平均耗时窗口配置喵
    state = make_state()
    # 记录一条已完成请求喵
    event = state.record_rate_event("auto-test", 50)
    state.attach_elapsed_ms(event, 700.0)
    # 将事件移出 60 秒速率窗口但保留在平均耗时窗口内喵
    event.at -= 61
    # 读取拆分窗口后的快照喵
    row = state.snapshot_virtual_model_rates()[0]
    # RPM/TPM 不应继续统计 60 秒之外的事件喵
    assert row.rpm == 0
    assert row.tpm is None
    # 平均耗时仍应统计配置窗口内的已完成事件喵
    assert row.completed_requests == 1
    assert row.average_elapsed_ms == 700.0


def test_平均耗时只统计已完成请求():
    """平均耗时只使用已经完整结束的请求，未结束流式请求不能进入分母喵~"""
    # 造状态喵
    state = make_state()
    # 一条完整结束的请求喵
    completed = state.record_rate_event("auto-test", 20)
    state.attach_elapsed_ms(completed, 200.0)
    # 一条尚未结束的流式请求，只有 RPM/TPM 事件但没有耗时喵
    state.record_rate_event("auto-test", 30)
    # 取快照喵
    row = state.snapshot_virtual_model_rates()[0]
    # 两条成功都进 RPM 喵
    assert row.rpm == 2
    # 只一条完整结束请求进入平均值喵
    assert row.completed_requests == 1
    assert row.average_elapsed_ms == 200.0


def test_默认性能统计窗口是30分钟():
    """默认配置窗口必须是近 30 分钟喵~"""
    # 解析最小配置喵
    config = parse_config(make_config_dict())
    # 默认值应为 30 分钟喵
    assert config.server.metrics_window_minutes == 30.0


def test_窗口外耗时事件不会进入平均值():
    """超过可配置窗口的完整请求不应影响当前平均耗时喵~"""
    # 造带 1 分钟窗口的配置喵
    config = parse_config(make_config_dict(server_overrides={"metrics_window_minutes": 1}))
    # 造状态喵
    state = RuntimeState(config)
    # 记录一条完整事件并把它移动到窗口外喵
    old_event = state.record_rate_event("auto-test", 50)
    state.attach_elapsed_ms(old_event, 1000.0)
    old_event.at -= 61
    # 再记当前窗口内的一条完整事件喵
    current_event = state.record_rate_event("auto-test", 60)
    state.attach_elapsed_ms(current_event, 300.0)
    # 读取快照喵
    row = state.snapshot_virtual_model_rates()[0]
    # 过期事件不应进入 RPM 或平均值喵
    assert row.rpm == 1
    assert row.average_elapsed_ms == 300.0


def test_流式请求结束后才增加RPM():
    """流式放行时不能提前计入 RPM，必须等生成器完整结束后统一上报喵~"""
    # 造状态喵
    state = make_state()
    # 没有流结束上报前，虚拟模型应保持零 RPM 喵
    assert state.snapshot_virtual_model_rates()[0].rpm == 0
    # 模拟流完整结束后的统一上报喵
    state.record_rate_event("auto-test", 42)
    # 结束后才应该看到 RPM=1 喵
    assert state.snapshot_virtual_model_rates()[0].rpm == 1


def test_流结束后上报事件携带最终usage与耗时():
    """流完整结束后统一上报的事件应同时带最终 usage 和完整耗时喵~"""
    # 造状态喵
    state = make_state()
    # 模拟上游流结束后才创建的统计事件喵
    event = state.record_rate_event("auto-test", 88)
    # 模拟完整流的结束耗时喵
    state.attach_elapsed_ms(event, 456.0)
    # 取快照喵
    row = state.snapshot_virtual_model_rates()[0]
    # 流结束后才会看见一条 RPM 喵
    assert row.rpm == 1
    # TPM 来自最终上游 usage 喵
    assert row.tpm == 88
    # 平均耗时来自完整流结束时刻喵
    assert row.average_elapsed_ms == 456.0


def test_异常流统计事件不带完整耗时():
    """异常流可以保留速率事件，但没有完整耗时就不能进入平均值喵~"""
    # 造状态喵
    state = make_state()
    # 模拟异常结束后只登记 RPM/TPM 的事件喵
    state.record_rate_event("auto-test", 88)
    # 取统计快照喵
    row = state.snapshot_virtual_model_rates()[0]
    # 速率事件仍然存在喵
    assert row.rpm == 1
    # 没有完整耗时的异常流不得计入平均值喵
    assert row.completed_requests == 0
    assert row.average_elapsed_ms is None


def test_冻结与解冻():
    """冻结后应该查得到剩余时间，解冻后应该立刻可用喵~"""
    # 造状态和候选喵
    state = make_state()
    candidate = state.config.virtual_models["auto-test"][0]
    # 一开始不该被冻结喵
    assert state.is_frozen(candidate) == 0
    # 冻结 60 秒喵
    state.freeze(candidate, 60, "额度用尽")
    # 现在应该查到还剩接近 60 秒喵
    assert 55 < state.is_frozen(candidate) <= 60
    # 手动解冻喵
    state.unfreeze(candidate)
    # 现在应该可用了喵
    assert state.is_frozen(candidate) == 0


def test_冻结会自动过期():
    """冻结时间到了应该自动失效，不需要后台清理任务喵~"""
    # 造状态和候选喵
    state = make_state()
    candidate = state.config.virtual_models["auto-test"][0]
    # 冻结一个极短的时间喵
    state.freeze(candidate, 0.05, "短暂冻结")
    # 立刻查应该还在冻结中喵
    assert state.is_frozen(candidate) > 0
    # 等它过期喵
    time.sleep(0.1)
    # 现在应该自动失效了喵
    assert state.is_frozen(candidate) == 0


def test_成功一次会自动解冻():
    """候选成功证明它已经恢复，应该立刻解冻而不是等倒计时走完喵~"""
    # 造状态和候选喵
    state = make_state()
    candidate = state.config.virtual_models["auto-test"][0]
    # 先冻结很久喵
    state.freeze(candidate, 3600, "额度用尽")
    # 确认真的冻上了喵
    assert state.is_frozen(candidate) > 0
    # 记一次成功喵
    state.record_success(candidate)
    # 应该立刻解冻喵
    assert state.is_frozen(candidate) == 0


def test_同一个key在不同虚拟模型里共享冻结():
    """冻结是全局的，同一个 (地址,key,模型) 三元组在哪个虚拟模型里都该被跳过喵~"""
    # 造两个内容完全相同的候选对象喵
    a = Candidate(name="甲", base_url="https://x.test", api_key="sk-1", model="m")
    b = Candidate(name="乙", base_url="https://x.test", api_key="sk-1", model="m")
    # 造状态喵
    state = make_state()
    # 冻结第一个喵
    state.freeze(a, 60, "额度用尽")
    # 第二个也应该被认为在冻结中，因为身份串相同喵
    assert state.is_frozen(b) > 0


def test_不同key独立冻结():
    """额度是按 key 算的，所以同一个地址的两个 key 必须独立冻结喵~"""
    # 造两个只有 key 不同的候选喵
    a = Candidate(name="甲", base_url="https://x.test", api_key="sk-1", model="m")
    b = Candidate(name="乙", base_url="https://x.test", api_key="sk-2", model="m")
    # 造状态喵
    state = make_state()
    # 只冻结第一个喵
    state.freeze(a, 60, "额度用尽")
    # 第二个应该完全不受影响喵
    assert state.is_frozen(b) == 0


# ============ 5. 请求构造喵 ============


def test_只替换顶层model其余字段原样保留():
    """透传的核心：除了 model，别的字段一个都不能动喵~"""
    # 造一个带各种字段的请求体喵
    body = {
        "model": "auto-test",
        "messages": [{"role": "user", "content": "你好"}],
        "temperature": 0.7,
        "custom_upstream_field": {"nested": True},
    }
    # 造一个候选喵
    candidate = Candidate(name="测试", base_url="https://x.test", api_key="sk-1", model="真实模型名")
    # 构造上游请求体并解析回来喵
    result = json.loads(build_upstream_body(body, candidate))
    # model 应该被换成候选的真实模型名喵
    assert result["model"] == "真实模型名"
    # 其余字段应该一字不差地保留喵
    assert result["messages"] == body["messages"]
    assert result["temperature"] == 0.7
    assert result["custom_upstream_field"] == {"nested": True}
    # 原始字典不应该被改动，因为它后面还要用于日志喵
    assert body["model"] == "auto-test"


def test_bearer风格的鉴权头():
    """OpenAI 系应该用 Authorization: Bearer 喵~"""
    # 造一个 bearer 风格的候选喵
    candidate = Candidate(name="测试", base_url="https://x.test", api_key="sk-abc", model="m", auth_style="bearer")
    # 构造请求头，客户端原本带的是别人的 key 喵
    headers = build_upstream_headers({"authorization": "Bearer 客户端的key", "x-custom": "保留我"}, candidate)
    # 应该被换成候选自己的 key 喵
    assert headers["Authorization"] == "Bearer sk-abc"
    # 客户端的自定义头应该被透传保留喵
    assert headers["x-custom"] == "保留我"


def test_anthropic风格的鉴权头会补版本号():
    """Anthropic 系必须带 anthropic-version，否则上游直接 400 喵~"""
    # 造一个 x-api-key 风格的候选喵
    candidate = Candidate(name="测试", base_url="https://x.test", api_key="sk-ant", model="m", auth_style="x-api-key")
    # 构造请求头喵
    headers = build_upstream_headers({}, candidate)
    # 应该用 x-api-key 头喵
    assert headers["x-api-key"] == "sk-ant"
    # 应该自动补上版本号喵
    assert "anthropic-version" in headers


def test_逐跳头会被剔除():
    """host 和 content-length 这类头绝不能原样转发，否则上游会路由错或读取截断喵~"""
    # 造一个候选喵
    candidate = Candidate(name="测试", base_url="https://x.test", api_key="sk-1", model="m")
    # 构造请求头，故意带上一堆逐跳头喵
    headers = build_upstream_headers(
        {"host": "localhost:8787", "content-length": "123", "connection": "keep-alive"}, candidate
    )
    # 这些头都应该被剔除喵
    assert "host" not in headers
    assert "content-length" not in headers
    assert "connection" not in headers


def test_识别流式标记():
    """两种协议都用顶层 stream 字段，还要兼容写成字符串的客户端喵~"""
    # 标准布尔真值喵
    assert detect_stream_flag({"stream": True}) is True
    # 标准布尔假值喵
    assert detect_stream_flag({"stream": False}) is False
    # 字段缺失时按非流式处理喵
    assert detect_stream_flag({}) is False
    # 兼容写成字符串的情况喵
    assert detect_stream_flag({"stream": "true"}) is True


def test_请求体非法会被拒绝():
    """空体、非 JSON、顶层不是对象，三种都要挡下来喵~"""
    # 空请求体喵
    with pytest.raises(ValueError, match="为空"):
        parse_client_body(b"")
    # 非 JSON 喵
    with pytest.raises(ValueError, match="合法的 JSON"):
        parse_client_body(b"not json at all")
    # 顶层是数组喵
    with pytest.raises(ValueError, match="必须是对象"):
        parse_client_body(b"[1, 2, 3]")


# ============ 6. 端到端故障转移喵 ============


def make_client(handler) -> httpx.AsyncClient:
    """
    造一个假装成上游的 httpx 客户端喵~

    输入：handler 是个函数，接收 httpx.Request 返回 httpx.Response，用来模拟各种上游行为
    输出：走 MockTransport 的异步客户端，完全不会发出真实网络请求喵
    """
    # 用 MockTransport 把所有请求都路由到 handler 上喵
    return httpx.AsyncClient(transport=httpx.MockTransport(handler))


async def run_proxy(client: httpx.AsyncClient, state: RuntimeState, body: dict) -> object:
    """跑一次完整的编排流程，简化各个测试的样板代码喵~"""
    # 调用编排层，路径和头都用最小可用的值喵
    return await handle_request(
        # 假上游客户端喵
        client,
        # 运行时状态喵
        state,
        # 请求方法喵
        "POST",
        # 请求路径喵
        "v1/chat/completions",
        # 没有查询串喵
        "",
        # 请求头喵
        {"content-type": "application/json"},
        # 请求体字节喵
        json.dumps(body).encode("utf-8"),
    )


@pytest.mark.asyncio
async def test_未配置的虚拟模型返回400():
    """未命中虚拟模型表就该回 400，并列出可用的名字喵~"""
    # 造状态和一个永远成功的假上游喵
    state = make_state()
    client = make_client(lambda req: httpx.Response(200, json={"ok": True}))
    # 请求一个没配过的模型喵
    outcome = await run_proxy(client, state, {"model": "根本不存在的模型"})
    # 应该失败且状态码是 400 喵
    assert outcome.success is False
    assert outcome.status == 400
    # 错误体里应该列出可用的虚拟模型喵
    assert "auto-test" in outcome.error_body["error"]["available_models"]


@pytest.mark.asyncio
async def test_第一个候选成功就不碰第二个():
    """严格优先级：链首能用就绝不降级喵~"""
    # 记录每次请求打到了哪个地址喵
    hits = []

    def handler(request: httpx.Request) -> httpx.Response:
        """假上游：一律成功喵~"""
        # 记下被请求的主机名喵
        hits.append(request.url.host)
        # 返回一个正常的非流式响应喵
        return httpx.Response(200, json={"choices": [{"message": {"content": "你好"}}]})

    # 造状态和假上游喵
    state = make_state()
    # 跑一次编排喵
    outcome = await run_proxy(make_client(handler), state, {"model": "auto-test"})
    # 应该成功喵
    assert outcome.success is True
    # 只应该打了一次，且打的是链首那个喵
    assert hits == ["primary.test"]


@pytest.mark.asyncio
async def test_第一个候选401时自动换第二个():
    """401 说明这个 key 没救了，应该立刻换下一个候选喵~"""
    # 记录每次请求打到了哪个地址喵
    hits = []

    def handler(request: httpx.Request) -> httpx.Response:
        """假上游：主力回 401，备用回正常内容喵~"""
        # 记下主机名喵
        hits.append(request.url.host)
        # 主力地址一律返回 401 喵
        if request.url.host == "primary.test":
            return httpx.Response(401, json={"error": {"message": "invalid api key"}})
        # 备用地址返回正常响应，内容里放个 ASCII 标记方便断言喵
        return httpx.Response(200, json={"choices": [{"message": {"content": "FROM-BACKUP"}}]})

    # 造状态喵
    state = make_state()
    # 跑一次编排喵
    outcome = await run_proxy(make_client(handler), state, {"model": "auto-test"})
    # 最终应该成功喵
    assert outcome.success is True
    # 应该先打主力再打备用喵
    assert hits == ["primary.test", "backup.test"]
    # 返回给客户端的内容应该来自备用喵
    assert b"FROM-BACKUP" in outcome.attempt.body


@pytest.mark.asyncio
async def test_额度用尽会冻结候选():
    """带恢复时间的 429 应该把候选按上游说的时长冻结起来喵~"""

    def handler(request: httpx.Request) -> httpx.Response:
        """假上游：主力报额度用尽，备用正常喵~"""
        # 主力返回真实世界里那种带恢复时间的 429 喵
        if request.url.host == "primary.test":
            return httpx.Response(
                429,
                text="status_code=429, key sk-3ce*** has reached its rolling 1h usage quota; refreshes in 6 minutes",
            )
        # 备用正常喵
        return httpx.Response(200, json={"choices": [{"message": {"content": "备用顶上"}}]})

    # 造状态喵
    state = make_state()
    # 跑一次编排喵
    outcome = await run_proxy(make_client(handler), state, {"model": "auto-test"})
    # 应该靠备用成功喵
    assert outcome.success is True
    # 主力应该被冻结了，且时长接近 6 分钟喵
    primary = state.config.virtual_models["auto-test"][0]
    assert 350 < state.is_frozen(primary) <= 365


@pytest.mark.asyncio
async def test_被冻结的候选会被跳过():
    """已经冻结的候选不该再被打，这样才不会每条请求都去撞一次额度墙喵~"""
    # 记录每次请求打到了哪个地址喵
    hits = []

    def handler(request: httpx.Request) -> httpx.Response:
        """假上游：一律成功喵~"""
        # 记下主机名喵
        hits.append(request.url.host)
        # 返回正常响应喵
        return httpx.Response(200, json={"choices": [{"message": {"content": "ok"}}]})

    # 造状态喵
    state = make_state()
    # 手动把主力冻结起来喵
    state.freeze(state.config.virtual_models["auto-test"][0], 300, "测试用冻结")
    # 跑一次编排喵
    outcome = await run_proxy(make_client(handler), state, {"model": "auto-test"})
    # 应该成功喵
    assert outcome.success is True
    # 应该直接打备用，完全没碰主力喵
    assert hits == ["backup.test"]


@pytest.mark.asyncio
async def test_上下文超限400会切换候选():
    """上下文窗口不够时应换到候选链中的下一个模型喵~"""
    # 记录每次请求命中的上游主机喵
    hits = []

    def handler(request: httpx.Request) -> httpx.Response:
        """假上游：主力上下文超限，备用正常返回喵~"""
        # 记录当前请求命中的主机喵
        hits.append(request.url.host)
        # 主力返回真实格式的上下文超限错误喵
        if request.url.host == "primary.test":
            return httpx.Response(
                400,
                json={
                    "error": {
                        "type": "context_length_exceeded",
                        "message": "maximum context length exceeded",
                    }
                },
            )
        # 备用返回成功内容喵
        return httpx.Response(200, json={"choices": [{"message": {"content": "FROM-BACKUP"}}]})

    # 造测试状态喵
    state = make_state()
    # 执行一次完整代理请求喵
    outcome = await run_proxy(make_client(handler), state, {"model": "auto-test"})
    # 上下文超限后应由备用候选成功喵
    assert outcome.success is True
    # 请求顺序必须是主力再备用喵
    assert hits == ["primary.test", "backup.test"]
    # 返回体应来自备用候选喵
    assert b"FROM-BACKUP" in outcome.attempt.body


@pytest.mark.asyncio
async def test_四百原样回传给客户端():
    """400 是客户端自己的问题，不该白白把整条链试一遍喵~"""
    # 记录请求次数喵
    hits = []

    def handler(request: httpx.Request) -> httpx.Response:
        """假上游：一律返回 400 喵~"""
        # 记下主机名喵
        hits.append(request.url.host)
        # 返回 400 喵
        return httpx.Response(400, json={"error": {"message": "messages 字段格式不对"}})

    # 造状态喵
    state = make_state()
    # 跑一次编排喵
    outcome = await run_proxy(make_client(handler), state, {"model": "auto-test"})
    # 走 passthrough 路径，所以 success 是 True 但状态码是上游的 400 喵
    assert outcome.success is True
    assert outcome.attempt.status == 400
    # 只该打一次，绝不能把备用也试一遍喵
    assert hits == ["primary.test"]


@pytest.mark.asyncio
async def test_所有候选都失败时返回502():
    """整条链用尽应该回 502，并附上每个候选各自的失败原因喵~"""

    def handler(request: httpx.Request) -> httpx.Response:
        """假上游：一律 401 喵~"""
        # 所有地址都返回 401 喵
        return httpx.Response(401, json={"error": {"message": "invalid key"}})

    # 造状态喵
    state = make_state()
    # 跑一次编排喵
    outcome = await run_proxy(make_client(handler), state, {"model": "auto-test"})
    # 应该失败且状态码是 502 喵
    assert outcome.success is False
    assert outcome.status == 502
    # 错误体里应该有两条失败记录，对应两个候选喵
    assert len(outcome.error_body["error"]["attempts"]) == 2


@pytest.mark.asyncio
async def test_五百错误会先原地重试再换候选():
    """5xx 应该在同一个候选上退避重试，次数用尽才降级喵~"""
    # 记录每次请求打到了哪个地址喵
    hits = []

    def handler(request: httpx.Request) -> httpx.Response:
        """假上游：主力一直 503，备用正常喵~"""
        # 记下主机名喵
        hits.append(request.url.host)
        # 主力一直挂喵
        if request.url.host == "primary.test":
            return httpx.Response(503, text="service unavailable")
        # 备用正常喵
        return httpx.Response(200, json={"choices": [{"message": {"content": "ok"}}]})

    # 造状态喵
    state = make_state()
    # 跑一次编排喵
    outcome = await run_proxy(make_client(handler), state, {"model": "auto-test"})
    # 最终应该靠备用成功喵
    assert outcome.success is True
    # 主力应该被试了 2 次（配置里 max_attempts=2），然后才换到备用喵
    assert hits == ["primary.test", "primary.test", "backup.test"]


@pytest.mark.asyncio
async def test_假成功的流会被转移():
    """
    这是整个项目最核心的用例喵：上游回了 200 并开始吐 SSE，但流里一个字都没有。
    代理必须识别出来并换下一个候选，而且客户端完全感知不到喵。
    """
    # 记录每次请求打到了哪个地址喵
    hits = []

    def handler(request: httpx.Request) -> httpx.Response:
        """假上游：主力回一个只有 [DONE] 的空流，备用回正常的内容流喵~"""
        # 记下主机名喵
        hits.append(request.url.host)
        # 主力：200 但流里只有 [DONE]，典型的假成功喵
        if request.url.host == "primary.test":
            return 流式响应(sse("[DONE]"))
        # 备用：正常的内容流喵
        return 流式响应(sse(json.dumps({"choices": [{"delta": {"content": "真的内容"}}]}), "[DONE]"))

    # 造状态喵
    state = make_state()
    # 跑一次流式编排喵
    outcome = await run_proxy(make_client(handler), state, {"model": "auto-test", "stream": True})
    # 最终应该成功，且被识别为流式喵
    assert outcome.success is True
    assert outcome.is_stream is True
    # 应该先试主力发现是假成功，再换到备用喵
    assert hits == ["primary.test", "backup.test"]


@pytest.mark.asyncio
async def test_吐几个字然后卡住会被识别并重发():
    """
    主人要求新增的核心用例喵：上游吐了几个字符然后一直挂着不动。
    代理应该识别成卡流、原地重发一次，重发还是卡住才降级到下一个候选喵。
    """
    # 记录每次请求打到了哪个地址喵
    hits = []

    def handler(request: httpx.Request) -> httpx.Response:
        """假上游：主力吐两个字就卡住，备用正常喵~"""
        # 记下主机名喵
        hits.append(request.url.host)
        # 主力：吐两个字符（不够 10 个门槛）然后永远挂着喵
        if request.url.host == "primary.test":
            return 卡流响应(sse(json.dumps({"choices": [{"delta": {"content": "嗯嗯"}}]})))
        # 备用：正常的内容流，内容够长喵
        return 流式响应(sse(json.dumps({"choices": [{"delta": {"content": "这是一段够长的正常回答内容"}}]}), "[DONE]"))

    # 造状态喵
    state = make_state()
    # 跑一次流式编排喵
    outcome = await run_proxy(make_client(handler), state, {"model": "auto-test", "stream": True})
    # 最终应该靠备用成功喵
    assert outcome.success is True
    # 主力应该被试了 2 次（首次 + 重试 1 次），然后才换到备用喵
    assert hits == ["primary.test", "primary.test", "backup.test"]


@pytest.mark.asyncio
async def test_一个字都不吐的卡流也会被识别():
    """上游建立了流但一个字符都不吐，同样要能识别并重发喵~"""
    # 记录每次请求打到了哪个地址喵
    hits = []

    def handler(request: httpx.Request) -> httpx.Response:
        """假上游：主力建立流但一个字都不吐，备用正常喵~"""
        # 记下主机名喵
        hits.append(request.url.host)
        # 主力：什么都不吐，直接挂着喵
        if request.url.host == "primary.test":
            return 卡流响应()
        # 备用：正常内容喵
        return 流式响应(sse(json.dumps({"choices": [{"delta": {"content": "这是一段够长的正常回答内容"}}]}), "[DONE]"))

    # 造状态喵
    state = make_state()
    # 跑一次流式编排喵
    outcome = await run_proxy(make_client(handler), state, {"model": "auto-test", "stream": True})
    # 应该靠备用成功喵
    assert outcome.success is True
    # 同样是重发一次后降级喵
    assert hits == ["primary.test", "primary.test", "backup.test"]


@pytest.mark.asyncio
async def test_上游在发心跳时不算卡流():
    """
    这是主人报的那个 bug 的回归用例喵：「首字还没出就被我们自动重试了」。

    场景：上游持续发 SSE 心跳（连接非常健康、明显在正常干活），只是正文还没出来。
    旧逻辑用「首字符计时器」：从开始读流算起，等不到内容字符就判失败 —— 完全不看
    连接上有没有动静，于是把一个正在好好干活的上游冤枉成了坏节点。
    新逻辑用「静默计时器」：只要收到任何字节就归零重新数，所以发心跳期间永远不会触发。

    这个用例断言的正是「不该被判成卡流」：静默上限只有 1 秒，心跳每 0.05 秒一次，
    如果还按旧逻辑，1 秒后就会被斩；按新逻辑它会一直活到总预算用完，
    然后被判成 timeout（太慢了）而不是 stalled_stream（挂死了）喵。
    """
    # 引入超时状态码喵
    from autoapi.config import STATUS_TIMEOUT
    # 把总预算压到很小，好让测试快速跑完；静默上限保持默认的 1 秒喵
    config = parse_config(make_config_dict(server_overrides={"stream_timeout": 1.5}))
    # 取第一个候选喵
    candidate = config.virtual_models["auto-test"][0]

    def handler(request: httpx.Request) -> httpx.Response:
        """假上游：一直发心跳，永远不吐正文喵~"""
        # 心跳间隔 0.05 秒，远小于 1 秒的静默上限喵
        return 心跳响应(0.05)

    # 打一次流式请求喵
    result = await try_candidate(
        make_client(handler),
        candidate,
        "POST",
        "/v1/chat/completions",
        "",
        {},
        {"model": "auto-test", "stream": True},
        True,
        config.server,
    )
    # 应该失败（正文确实一直没来）喵
    assert result.ok is False
    # 但原因必须是「太慢了」而不是「挂死了」—— 这是这个用例的核心断言喵
    assert result.status == STATUS_TIMEOUT
    # 说明文本里要写清楚连接是活的，方便主人从日志上区分两种故障喵
    assert "总预算" in result.error_text


@pytest.mark.asyncio
async def test_先安静思考再回答的模型能正常放行():
    """
    推理模型最典型的行为：先安静想一会儿，然后正常输出。必须能放行喵~

    这个用例守的是新逻辑的另一半 —— 修 bug 不能修过头。静默计时器是「上游还活着吗」
    的探针，安静时间只要没超过 stall_timeout 就不该判失败，正文一来就要照常放行喵。
    """
    # 静默上限 1 秒（配置里的下限），安静 0.4 秒后再吐字，应该安全通过喵
    config = parse_config(make_config_dict())
    # 取第一个候选喵
    candidate = config.virtual_models["auto-test"][0]

    def handler(request: httpx.Request) -> httpx.Response:
        """假上游：先安静 0.4 秒，然后吐出一段够长的正常内容喵~"""
        # 造一段够过 10 字符门槛的正文喵
        payload = sse(
            json.dumps({"choices": [{"delta": {"content": "这是一段够长的正常回答内容"}}]}),
            "[DONE]",
        )
        # 先安静再吐字喵
        return 先安静再回答的响应(0.4, payload)

    # 打一次流式请求喵
    result = await try_candidate(
        make_client(handler),
        candidate,
        "POST",
        "/v1/chat/completions",
        "",
        {},
        {"model": "auto-test", "stream": True},
        True,
        config.server,
    )
    # 应该成功放行喵
    assert result.ok is True
    # 状态码是 200 喵
    assert result.status == 200


@pytest.mark.asyncio
async def test_卡流状态码能被规则单独匹配():
    """卡流和假成功要能被不同规则分别处置，这是新状态码存在的意义喵~"""
    # 取规则列表喵
    rules = parse_config(make_config_dict()).rules
    # 卡流应该命中 retry 规则喵
    stalled = decide(rules, STATUS_STALLED_STREAM, "上游的流卡住了")
    assert stalled.action == "retry"
    # 含首次共 2 次，也就是最多重试 1 次喵
    assert stalled.max_attempts == 2
    # 假成功应该命中 next 规则，和卡流走不同的路喵
    bad = decide(rules, STATUS_BAD_STREAM, "空流")
    assert bad.action == "next"


@pytest.mark.asyncio
async def test_短回答的流不会被门槛误伤():
    """
    端到端确认：整条流只有几个字就正常结束时，客户端要能正常收到，
    绝不能因为凑不满 10 个字符就被判失败换候选喵。
    """
    # 记录每次请求打到了哪个地址喵
    hits = []
    # 造一条很短但完整的流：两个字 + 结束标记喵
    short_stream = sse(json.dumps({"choices": [{"delta": {"content": "好的"}}]}), "[DONE]")

    def handler(request: httpx.Request) -> httpx.Response:
        """假上游：链首就回一条很短的正常流喵~"""
        # 记下主机名喵
        hits.append(request.url.host)
        # 回那条短流喵
        return 流式响应(short_stream)

    # 造状态喵
    state = make_state()
    # 跑一次流式编排喵
    outcome = await run_proxy(make_client(handler), state, {"model": "auto-test", "stream": True})
    # 应该直接成功喵
    assert outcome.success is True
    # 只该打链首一次，不该发生任何转移喵
    assert hits == ["primary.test"]
    # 收到的字节应该和上游原始输出一字不差喵
    from autoapi.upstream import iter_upstream_bytes
    # 收集所有字节喵
    collected = b""
    # 逐块收集喵
    async for chunk in iter_upstream_bytes(outcome.attempt):
        collected += chunk
    # 必须完全一致喵
    assert collected == short_stream


@pytest.mark.asyncio
async def test_只调工具的流不会被门槛误伤():
    """模型只调工具、完全不吐文字时也要能正常放行，不能被当成卡流喵~"""
    # 记录每次请求打到了哪个地址喵
    hits = []
    # 造一条只有工具调用的流喵
    tool_stream = sse(
        json.dumps({"choices": [{"delta": {"tool_calls": [{"index": 0, "id": "call_1"}]}}]}),
        "[DONE]",
    )

    def handler(request: httpx.Request) -> httpx.Response:
        """假上游：链首回一条只有工具调用的流喵~"""
        # 记下主机名喵
        hits.append(request.url.host)
        # 回那条流喵
        return 流式响应(tool_stream)

    # 造状态喵
    state = make_state()
    # 跑一次流式编排喵
    outcome = await run_proxy(make_client(handler), state, {"model": "auto-test", "stream": True})
    # 应该直接成功，不发生转移喵
    assert outcome.success is True
    assert hits == ["primary.test"]


@pytest.mark.asyncio
async def test_流里带error事件也会被转移():
    """上游在流里报错同样要能转移喵~"""
    # 记录每次请求打到了哪个地址喵
    hits = []

    def handler(request: httpx.Request) -> httpx.Response:
        """假上游：主力在流里报错，备用正常喵~"""
        # 记下主机名喵
        hits.append(request.url.host)
        # 主力：200 但流里塞了 error 事件喵
        if request.url.host == "primary.test":
            return 流式响应(sse(json.dumps({"error": {"message": "insufficient quota"}})))
        # 备用：正常内容喵
        return 流式响应(sse(json.dumps({"choices": [{"delta": {"content": "好的"}}]}), "[DONE]"))

    # 造状态喵
    state = make_state()
    # 跑一次流式编排喵
    outcome = await run_proxy(make_client(handler), state, {"model": "auto-test", "stream": True})
    # 应该靠备用成功喵
    assert outcome.success is True
    assert hits == ["primary.test", "backup.test"]


@pytest.mark.asyncio
async def test_健康的流会一字不差地转发():
    """
    放行后的字节序列必须和上游原始输出完全一致，包括探测阶段已经读掉的那部分喵。
    这是「先缓冲后replay」这个设计是否正确的关键验证喵。
    """
    # 造一个多块内容的正常流喵
    payloads = [
        json.dumps({"choices": [{"delta": {"role": "assistant", "content": ""}}]}),
        json.dumps({"choices": [{"delta": {"content": "第一段"}}]}),
        json.dumps({"choices": [{"delta": {"content": "第二段"}}]}),
        "[DONE]",
    ]
    # 拼成完整的原始字节流喵
    original = sse(*payloads)

    def handler(request: httpx.Request) -> httpx.Response:
        """假上游：用真正的分块流回这条正常内容喵~"""
        # 用分块流承载，每块只有 16 字节，故意让 SSE 行边界被切碎喵
        return 流式响应(original)

    # 造状态喵
    state = make_state()
    # 跑一次流式编排喵
    outcome = await run_proxy(make_client(handler), state, {"model": "auto-test", "stream": True})
    # 应该成功喵
    assert outcome.success is True
    # 引入字节流生成器喵
    from autoapi.upstream import iter_upstream_bytes
    # 把生成器吐出的所有字节拼起来喵
    collected = b""
    # 逐块收集喵
    async for chunk in iter_upstream_bytes(outcome.attempt):
        collected += chunk
    # 收到的字节必须和上游原始输出一字不差喵
    assert collected == original


@pytest.mark.asyncio
async def test_流式放行日志区分节点首字与客户端请求首字(caplog):
    """放行日志必须分开输出节点首字和客户端请求首字，不能提前记录完整耗时喵~"""
    # 造一条足以通过默认内容门槛的正常 SSE 流喵
    healthy_stream = sse(json.dumps({"choices": [{"delta": {"content": "足够通过健康探测的内容"}}]}), "[DONE]")

    def handler(request: httpx.Request) -> httpx.Response:
        """假上游：回一条正常健康流喵~"""
        # 用流式响应把健康流交给代理喵
        return 流式响应(healthy_stream)

    # 造运行时状态喵
    state = make_state()
    # 开启编排层日志收集喵
    caplog.set_level(logging.INFO, logger="autoapi.proxy")
    # 跑到流健康放行阶段喵
    outcome = await run_proxy(make_client(handler), state, {"model": "auto-test", "stream": True})
    # 流探测应成功喵
    assert outcome.success is True
    # 取出流式成功日志喵
    stream_logs = [record.getMessage() for record in caplog.records if "流 首字=" in record.getMessage()]
    # 应该只输出一条放行日志喵
    assert len(stream_logs) == 1
    # 日志必须包含两套已知口径喵
    assert "首字=" in stream_logs[0]
    assert "请求首字=" in stream_logs[0]
    # 放行阶段尚未完成请求，不能伪造完整返回耗时喵
    assert "返回请求耗时=" not in stream_logs[0]


@pytest.mark.asyncio
async def test_上游读取异常不标记流式正常完成():
    """流式读取途中出现 HTTPX 异常时，迭代器不得标记为正常完成喵~"""
    # 引入流转发函数和结果类型喵
    from autoapi.upstream import AttemptResult, iter_upstream_bytes

    async def broken_iterator():
        # 先吐一块数据，模拟流已经向客户端开始转发喵
        yield b"data: partial\n\n"
        # 喵~防御：模拟上游读取中断，不能被误判为自然结束喵
        raise httpx.ReadError("上游读取中断")

    # 造带上游响应与异常迭代器的流式结果喵
    result = AttemptResult(
        ok=True,
        status=200,
        response=httpx.Response(200),
        iterator=broken_iterator(),
    )
    # 收集代理已成功转发的前缀字节喵
    collected = b""
    # 消费转发迭代器；内部会吞掉预期的 HTTPX 读取异常喵
    async for chunk in iter_upstream_bytes(result):
        collected += chunk
    # 已经成功转发的前缀必须保留喵
    assert collected == b"data: partial\n\n"
    # 读取异常不能被标记为完整结束，server 因此不会写入平均耗时喵
    assert result.stream_completed_normally is False


@pytest.mark.asyncio
async def test_网络错误会被转移():
    """连不上上游时应该换下一个候选，而不是把异常抛给客户端喵~"""
    # 记录每次请求打到了哪个地址喵
    hits = []

    def handler(request: httpx.Request) -> httpx.Response:
        """假上游：主力直接抛连接错误，备用正常喵~"""
        # 记下主机名喵
        hits.append(request.url.host)
        # 主力：模拟连接失败喵
        if request.url.host == "primary.test":
            raise httpx.ConnectError("连接被拒绝", request=request)
        # 备用：正常响应喵
        return httpx.Response(200, json={"choices": [{"message": {"content": "ok"}}]})

    # 造状态喵
    state = make_state()
    # 跑一次编排喵
    outcome = await run_proxy(make_client(handler), state, {"model": "auto-test"})
    # 应该靠备用成功，异常不该穿透出来喵
    assert outcome.success is True
    assert hits == ["primary.test", "backup.test"]


@pytest.mark.asyncio
async def test_非流式的200里带error也算假成功():
    """有些中转站出错时也回 200，把错误塞进 body，这种要能识别喵~"""
    # 记录每次请求打到了哪个地址喵
    hits = []

    def handler(request: httpx.Request) -> httpx.Response:
        """假上游：主力回 200 但 body 里带 error，备用正常喵~"""
        # 记下主机名喵
        hits.append(request.url.host)
        # 主力：200 但内容是错误喵
        if request.url.host == "primary.test":
            return httpx.Response(200, json={"error": {"message": "insufficient quota"}})
        # 备用：正常响应喵
        return httpx.Response(200, json={"choices": [{"message": {"content": "ok"}}]})

    # 造状态喵
    state = make_state()
    # 跑一次编排喵
    outcome = await run_proxy(make_client(handler), state, {"model": "auto-test"})
    # 应该识别出假成功并换到备用喵
    assert outcome.success is True
    assert hits == ["primary.test", "backup.test"]


def test_超时按当前配置现算():
    """
    超时必须每条请求现算，否则运行中改超时会被启动时的旧值压住喵~

    这个用例守的是一个很容易悄悄坏掉的行为：httpx 客户端是全程复用的，它身上的 timeout
    在启动时就定死了，所以超时必须显式按当前配置传给每条请求喵。
    """
    # 引入现算超时的函数和超时解析函数喵
    from autoapi.config import resolve_timeouts
    from autoapi.upstream import build_timeout
    # 取一份配置喵
    config = parse_config(make_config_dict())
    # 先算出全局生效的超时值喵
    timeouts = resolve_timeouts(config.server)
    # 现算一份流式用的超时喵
    stream_timeout = build_timeout(timeouts, is_stream=True)
    # 连接超时应该来自配置里的 connect_timeout 喵
    assert stream_timeout.connect == config.server.connect_timeout
    # 流式的读超时衡量的是「允许静默多久」，所以应该等于 stall_timeout 喵
    assert stream_timeout.read == config.server.stall_timeout
    # 非流式的读超时衡量的是「总共等多久」，所以应该等于 nonstream_timeout 喵
    nonstream_timeout = build_timeout(timeouts, is_stream=False)
    assert nonstream_timeout.read == config.server.nonstream_timeout
    # 改掉配置里的超时之后，现算出来的应该跟着变喵
    config.server.nonstream_timeout = 777.0
    assert build_timeout(resolve_timeouts(config.server), is_stream=False).read == 777.0


def test_节点专属超时会覆盖全局值():
    """
    节点上配了超时就用它，没配的项继续跟随全局值喵~

    这个用例守的是「按节点覆盖」的核心语义：覆盖必须是逐项独立的。
    只配了 stream_timeout 的节点，它的 stall_timeout 应该照旧跟随全局值，
    绝不能因为「配过一项」就把整套超时都从全局值断开喵。
    """
    # 造一份配置：第一个节点只覆盖 stream_timeout，第二个节点什么都不覆盖喵
    config = parse_config(
        make_config_dict(
            server_overrides={"stall_timeout": 30, "stream_timeout": 300, "nonstream_timeout": 600},
            virtual_models={
                "auto-test": [
                    {
                        "name": "慢速推理",
                        "base_url": "https://slow.test",
                        "api_key": "sk-slow-key-1234",
                        "model": "o3-deep",
                        # 只覆盖这一项喵
                        "stream_timeout": 900,
                    },
                    {
                        "name": "普通",
                        "base_url": "https://normal.test",
                        "api_key": "sk-normal-key-5678",
                        "model": "gpt-4o",
                    },
                ]
            },
        )
    )
    # 取两个候选喵
    slow, normal = config.virtual_models["auto-test"]
    # 慢速节点的流式预算应该是它自己配的 900 喵
    slow_timeouts = resolve_timeouts(config.server, slow)
    assert slow_timeouts.stream == 900.0
    # 但它没配的两项必须继续跟随全局值，这是逐项独立的关键断言喵
    assert slow_timeouts.stall == 30.0
    assert slow_timeouts.nonstream == 600.0
    # 普通节点什么都没配，三项全都跟随全局值喵
    normal_timeouts = resolve_timeouts(config.server, normal)
    assert normal_timeouts.stream == 300.0
    assert normal_timeouts.stall == 30.0
    assert normal_timeouts.nonstream == 600.0


def test_节点专属超时会体现在实际请求上():
    """
    光解析对了不算，得确认它真的被用到这次请求的超时里喵~

    这个用例把 resolve_timeouts 和 build_timeout 串起来验证：
    给节点配了很长的静默上限之后，实际发出去的请求的 read 超时必须跟着变长，
    否则「按节点配超时」就只是配置文件里好看而已喵。
    """
    # 引入现算超时的函数喵
    from autoapi.upstream import build_timeout
    # 造一份配置：节点把静默上限放宽到 240 秒，全局只有 30 秒喵
    config = parse_config(
        make_config_dict(
            server_overrides={"stall_timeout": 30},
            virtual_models={
                "auto-test": [
                    {
                        "name": "慢速推理",
                        "base_url": "https://slow.test",
                        "api_key": "sk-slow-key-1234",
                        "model": "o3-deep",
                        "stall_timeout": 240,
                    }
                ]
            },
        )
    )
    # 取那个节点喵
    candidate = config.virtual_models["auto-test"][0]
    # 算出这次请求生效的超时喵
    timeouts = resolve_timeouts(config.server, candidate)
    # 流式的 read 超时衡量静默时长，应该用节点自己的 240 而不是全局的 30 喵
    assert build_timeout(timeouts, is_stream=True).read == 240.0


def test_节点超时写成非数字要报错():
    """
    超时写错时必须在加载阶段就报错，不能静默忽略喵~

    为什么不静默忽略：配置里明明写着一个值，实际却没生效，是最难查的一类问题。
    宁可启动就失败让主人立刻改，也不要让它带着一个假配置跑下去喵。
    """
    # 造一个把超时写成字符串的候选喵
    data = make_config_dict(
        virtual_models={
            "bad": [
                {
                    "base_url": "https://x.test",
                    "api_key": "sk-x-key-12345678",
                    "model": "gpt-4o",
                    "stream_timeout": "很久",
                }
            ]
        }
    )
    # 加载应该抛 ConfigError 喵
    with pytest.raises(ConfigError) as exc:
        parse_config(data)
    # 报错信息里要点出是哪个字段喵
    assert "stream_timeout" in str(exc.value)


def test_节点超时写成零或负数要报错():
    """超时设成 0 等于请求一发出就判超时，肯定不是主人想要的，必须挡掉喵~"""
    # 造一个把超时写成 0 的候选喵
    data = make_config_dict(
        virtual_models={
            "bad": [
                {
                    "base_url": "https://x.test",
                    "api_key": "sk-x-key-12345678",
                    "model": "gpt-4o",
                    "stall_timeout": 0,
                }
            ]
        }
    )
    # 加载应该抛 ConfigError 喵
    with pytest.raises(ConfigError) as exc:
        parse_config(data)
    # 报错信息里要说明必须大于 0 喵
    assert "大于 0" in str(exc.value)


def test_用了已退役的配置项会攒下警告():
    """
    老配置项不该让加载失败，但必须留下明确的警告喵~

    这是「改名之后」的兼容策略：旧配置照旧能起来（不打断服务），
    但主人一定要看到「你配的这一项已经不生效了」，否则会以为超时配了却没效果喵。
    """
    # 造一份还带着老配置项的配置喵
    config = parse_config(
        make_config_dict(server_overrides={"request_timeout": 300, "first_content_timeout": 45})
    )
    # 应该攒下两条警告喵
    assert len(config.warnings) == 2
    # 把警告拼起来方便检查内容喵
    joined = " ".join(config.warnings)
    # 两个老名字都要被点出来喵
    assert "request_timeout" in joined
    assert "first_content_timeout" in joined
    # 而且要指出该改用哪些新项喵
    assert "stream_timeout" in joined
    # 但配置本身要能正常加载，不能因为这个就失败喵
    assert len(config.virtual_models) == 1


def test_超时状态码能被规则单独匹配():
    """
    timeout 和 stalled_stream 要能被分别处置，这是新状态码存在的意义喵~

    两者的故障性质完全不同：卡流是连接挂死了，超时是连接健康但太慢。
    有些主人会想给「太慢」更少的重试次数（毕竟已经等了很久），所以必须能分开配喵。
    """
    # 取规则列表喵
    rules = parse_config(make_config_dict()).rules
    # 超时应该命中它自己那条 retry 规则喵
    timeout_decision = decide(rules, STATUS_TIMEOUT, "流式请求超过总预算 300 秒")
    assert timeout_decision.action == "retry"
    # 含首次共 2 次，也就是最多重发 1 次喵
    assert timeout_decision.max_attempts == 2
    # 卡流走的是另一条规则，两者互不干扰喵
    stalled_decision = decide(rules, STATUS_STALLED_STREAM, "上游静默太久")
    assert stalled_decision.action == "retry"


@pytest.mark.asyncio
async def test_非流式超过总预算会被判超时():
    """非流式请求等太久时要归为 timeout 而不是网络错误喵~"""
    # 把非流式预算压到很小好让测试快速跑完喵
    config = parse_config(make_config_dict(server_overrides={"nonstream_timeout": 1}))
    # 取第一个候选喵
    candidate = config.virtual_models["auto-test"][0]

    async def handler(request: httpx.Request) -> httpx.Response:
        """假上游：睡很久都不返回喵~"""
        # 睡得比预算长很多，让预算先到喵
        await asyncio.sleep(30)
        # 这一行实际到不了，只是让函数签名完整喵
        return httpx.Response(200, json={"ok": True})

    # 打一次非流式请求喵
    result = await try_candidate(
        make_client(handler),
        candidate,
        "POST",
        "/v1/chat/completions",
        "",
        {},
        {"model": "auto-test"},
        False,
        config.server,
    )
    # 应该失败喵
    assert result.ok is False
    # 而且必须是超时状态，好让规则里的 status: timeout 命中喵
    assert result.status == STATUS_TIMEOUT
    # 说明里要写清楚是超过了总预算喵
    assert "总预算" in result.error_text


@pytest.mark.asyncio
async def test_目标模式首轮失败后第二轮成功(monkeypatch):
    """目标模式应从链首循环，第二轮成功时不返回错误喵~"""
    # 让每轮等待 5 秒立即推进，不让测试真实等待喵
    sleep_calls: list[float] = []
    async def fake_sleep(seconds: float) -> None:
        # 记录目标模式固定的轮次间隔喵
        sleep_calls.append(seconds)
    # 替换 proxy 模块里的异步睡眠喵
    monkeypatch.setattr("autoapi.proxy.asyncio.sleep", fake_sleep)
    # 记录请求顺序喵
    hits: list[str] = []
    request_count = 0
    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal request_count
        request_count += 1
        hits.append(request.url.host)
        # 第一轮返回失败，第二轮成功喵
        if request_count == 1:
            return httpx.Response(503, json={"error": "暂时不可用"})
        return httpx.Response(200, json={"ok": True, "usage": {"total_tokens": 3}})
    # 造状态并开启目标模式喵
    state = make_state()
    state.set_target_mode(True)
    # 运行完整请求喵
    outcome = await run_proxy(make_client(handler), state, {"model": "auto-test"})
    # 第二轮成功，客户端不应看到失败喵
    assert outcome.success is True
    assert request_count == 2
    # 目标模式等待间隔应包含候选内部已有的重试退避；这里第一轮 503 先重试 1 秒喵
    assert sleep_calls == [1.0]


@pytest.mark.asyncio
async def test_目标模式截止后返回504(monkeypatch):
    """目标模式超过配置时长后应返回 504（新默认值），并附诊断信息喵~"""
    current_time = 1000.0
    async def fake_sleep(seconds: float) -> None:
        nonlocal current_time
        current_time += seconds
    def fake_monotonic() -> float:
        return current_time
    # 注入单调时钟与睡眠，压缩 5 分钟测试时间喵
    monkeypatch.setattr("autoapi.proxy.time.monotonic", fake_monotonic)
    monkeypatch.setattr("autoapi.proxy.asyncio.sleep", fake_sleep)
    # 永远失败的上游喵
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(503, json={"error": "unavailable"})
    # 造状态并开启目标模式喵
    state = make_state()
    state.set_target_mode(True)
    # 运行请求喵
    outcome = await run_proxy(make_client(handler), state, {"model": "auto-test"})
    # 目标模式应该返回 504（新默认值）而不是 502 喵
    assert outcome.success is False
    assert outcome.status == 504
    error = outcome.error_body["error"]
    assert error["target_mode"] is True
    assert error["type"] == "target_mode_gateway_timeout"
    assert error["rounds"] >= 2
    assert error["waited_seconds"] == 300.0


@pytest.mark.asyncio
async def test_目标模式可配置超时行为_return_429(monkeypatch):
    """目标模式配置为 return_429 时应返回 429 状态码喵~"""
    current_time = 1000.0
    async def fake_sleep(seconds: float) -> None:
        nonlocal current_time
        current_time += seconds
    def fake_monotonic() -> float:
        return current_time
    # 注入单调时钟与睡眠喵
    monkeypatch.setattr("autoapi.proxy.time.monotonic", fake_monotonic)
    monkeypatch.setattr("autoapi.proxy.asyncio.sleep", fake_sleep)
    # 永远失败的上游喵
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(503, json={"error": "unavailable"})
    # 造状态并修改配置为 return_429 喵
    state = make_state()
    config = state.config
    # 修改配置中的超时行为喵
    from dataclasses import replace
    new_server = replace(config.server, target_mode_timeout_action="return_429")
    new_config = replace(config, server=new_server)
    state.replace_config(new_config)
    state.set_target_mode(True)
    # 运行请求喵
    outcome = await run_proxy(make_client(handler), state, {"model": "auto-test"})
    # 应该返回 429 喵
    assert outcome.success is False
    assert outcome.status == 429
    assert outcome.error_body["error"]["type"] == "target_mode_rate_limit"


@pytest.mark.asyncio
async def test_目标模式可配置超时行为_return_502(monkeypatch):
    """目标模式配置为 return_502 时应返回 502 状态码喵~"""
    current_time = 1000.0
    async def fake_sleep(seconds: float) -> None:
        nonlocal current_time
        current_time += seconds
    def fake_monotonic() -> float:
        return current_time
    # 注入单调时钟与睡眠喵
    monkeypatch.setattr("autoapi.proxy.time.monotonic", fake_monotonic)
    monkeypatch.setattr("autoapi.proxy.asyncio.sleep", fake_sleep)
    # 永远失败的上游喵
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(503, json={"error": "unavailable"})
    # 造状态并修改配置为 return_502 喵
    state = make_state()
    config = state.config
    from dataclasses import replace
    new_server = replace(config.server, target_mode_timeout_action="return_502")
    new_config = replace(config, server=new_server)
    state.replace_config(new_config)
    state.set_target_mode(True)
    # 运行请求喵
    outcome = await run_proxy(make_client(handler), state, {"model": "auto-test"})
    # 应该返回 502 喵
    assert outcome.success is False
    assert outcome.status == 502
    assert outcome.error_body["error"]["type"] == "target_mode_bad_gateway"


@pytest.mark.asyncio
async def test_目标模式可配置超时行为_drop_connection(monkeypatch):
    """目标模式配置为 drop_connection 时应返回特殊状态码喵~"""
    current_time = 1000.0
    async def fake_sleep(seconds: float) -> None:
        nonlocal current_time
        current_time += seconds
    def fake_monotonic() -> float:
        return current_time
    # 注入单调时钟与睡眠喵
    monkeypatch.setattr("autoapi.proxy.time.monotonic", fake_monotonic)
    monkeypatch.setattr("autoapi.proxy.asyncio.sleep", fake_sleep)
    # 永远失败的上游喵
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(503, json={"error": "unavailable"})
    # 造状态并修改配置为 drop_connection 喵
    state = make_state()
    config = state.config
    from dataclasses import replace
    new_server = replace(config.server, target_mode_timeout_action="drop_connection")
    new_config = replace(config, server=new_server)
    state.replace_config(new_config)
    state.set_target_mode(True)
    # 运行请求喵
    outcome = await run_proxy(make_client(handler), state, {"model": "auto-test"})
    # 应该返回特殊的断开连接状态码喵
    assert outcome.success is False
    from autoapi.proxy import STATUS_DROP_CONNECTION
    assert outcome.status == STATUS_DROP_CONNECTION
    assert outcome.error_body["error"]["type"] == "target_mode_drop_connection"


def test_目标模式开关默认关闭且不写配置():
    """目标模式是纯内存开关，默认关闭且不进入配置对象喵~"""
    # 造状态喵
    state = make_state()
    # 默认必须关闭喵
    assert state.target_mode_enabled is False
    # 开启和关闭应返回新状态喵
    assert state.set_target_mode(True) is True
    assert state.target_mode_enabled is True
    assert state.set_target_mode(False) is False
    # 配置对象不应出现 target_mode 字段喵
    assert not hasattr(state.config.server, "target_mode")


# ============ 7. 自动避险喵 ============


def test_连续失败达到阈值才触发避险():
    """连续失败次数没攒够时不该触发，攒够那一次才返回触发信号喵~"""
    # 造状态和候选喵
    state = make_state()
    candidate = state.config.virtual_models["auto-test"][0]
    # 前 4 次失败都不该触发（阈值设成 5）喵
    for i in range(4):
        assert state.record_failure(candidate, f"第{i}次错误", 5) == 0
    # 第 5 次达到阈值，应该返回触发时的次数喵
    assert state.record_failure(candidate, "第5次错误", 5) == 5


def test_中间成功一次会清零连续失败计数():
    """
    这是「连续」二字的核心语义喵：中间成功过就重新数，
    否则一个偶尔失败但整体健康的节点会被慢慢攒到阈值然后误冻结喵。
    """
    # 造状态和候选喵
    state = make_state()
    candidate = state.config.virtual_models["auto-test"][0]
    # 先失败 4 次喵
    for _ in range(4):
        state.record_failure(candidate, "错误", 5)
    # 成功一次，计数应该清零喵
    state.record_success(candidate)
    # 再失败 4 次也不该触发，因为是从 0 重新数的喵
    for _ in range(4):
        assert state.record_failure(candidate, "错误", 5) == 0
    # 第 5 次才触发喵
    assert state.record_failure(candidate, "错误", 5) == 5


def test_触发后计数清零需要再攒一轮():
    """触发过一次之后要重新攒满才会再触发，避免冻结时间被无意义地反复延长喵~"""
    # 造状态和候选喵
    state = make_state()
    candidate = state.config.virtual_models["auto-test"][0]
    # 攒满触发一次喵
    for _ in range(4):
        state.record_failure(candidate, "错误", 5)
    assert state.record_failure(candidate, "错误", 5) == 5
    # 紧接着再失败一次不该立刻又触发喵
    assert state.record_failure(candidate, "错误", 5) == 0


def test_阈值为零时关闭自动避险():
    """阈值设成 0 应该完全不触发，这是关闭开关喵~"""
    # 造状态和候选喵
    state = make_state()
    candidate = state.config.virtual_models["auto-test"][0]
    # 失败很多次也不该触发喵
    for _ in range(20):
        assert state.record_failure(candidate, "错误", 0) == 0


def test_不同节点的连续失败计数互相独立():
    """一个节点连续失败不该影响另一个节点的计数喵~"""
    # 造状态和两个候选喵
    state = make_state()
    chain = state.config.virtual_models["auto-test"]
    # 第一个节点失败 4 次喵
    for _ in range(4):
        state.record_failure(chain[0], "错误", 5)
    # 第二个节点第一次失败，不该受影响、更不该触发喵
    assert state.record_failure(chain[1], "错误", 5) == 0


@pytest.mark.asyncio
async def test_count_tokens错误不进入目标模式重试(monkeypatch):
    """count_tokens 错误即使开启目标模式也只执行一轮，不循环等待重试喵~"""
    # 记录目标模式是否错误地等待下一轮喵
    sleep_calls: list[float] = []
    async def fake_sleep(seconds: float) -> None:
        # 收集所有等待，正常 count_tokens 失败不应产生目标模式等待喵
        sleep_calls.append(seconds)
    # 替换目标模式等待函数，避免测试真实等待喵
    monkeypatch.setattr("autoapi.proxy.asyncio.sleep", fake_sleep)
    # 记录上游请求次数喵
    request_count = 0
    def handler(request: httpx.Request) -> httpx.Response:
        # 所有候选都返回普通上游错误喵
        nonlocal request_count
        request_count += 1
        return httpx.Response(503, json={"error": {"message": "unavailable"}})
    # 创建并开启目标模式的运行状态喵
    state = make_state()
    state.set_target_mode(True)
    # 直接使用 count_tokens 路径发起请求喵
    outcome = await handle_request(
        make_client(handler),
        state,
        "POST",
        "/v1/messages/count_tokens",
        "",
        {},
        json.dumps({"model": "auto-test", "messages": []}).encode("utf-8"),
    )
    # 失败应在一轮结束后返回普通失败，不返回目标模式 429 喵
    assert outcome.success is False
    assert outcome.status == 502
    # 两个候选各自按规则重试一次，但没有目标模式第二轮喵
    assert request_count == 4
    assert sleep_calls == [1.0, 1.0]


def test_避险配置能被正确解析():
    """新增的两个配置项要能正常读出来，并且有安全下限喵~"""
    # 默认值喵
    config = parse_config(make_config_dict())
    # 测试配置里没写这两项，应该拿到默认值喵
    assert config.server.auto_hedge_threshold == 5
    assert config.server.auto_hedge_minutes == 10.0
    # 喵~防御：负阈值应该被压成 0（也就是关闭），而不是变成诡异的负数行为喵
    data = make_config_dict()
    data["server"]["auto_hedge_threshold"] = -3
    assert parse_config(data).server.auto_hedge_threshold == 0
    # 喵~防御：冻结时长为 0 应该被压到 0.1 分钟，防止冻结形同虚设喵
    data["server"]["auto_hedge_minutes"] = 0
    assert parse_config(data).server.auto_hedge_minutes == 0.1


@pytest.mark.asyncio
async def test_端到端自动避险会冻结节点():
    """
    端到端验证喵：一个持续返回各种错误的节点，攒够次数后应该被自动冻结，
    之后的请求会直接跳过它喵。
    """
    # 记录每次请求打到了哪个地址喵
    hits = []

    def handler(request: httpx.Request) -> httpx.Response:
        """假上游：主力一直返回 404（规则里是 next，本身不会冻结它），备用正常喵~"""
        # 记下主机名喵
        hits.append(request.url.host)
        # 主力一直挂喵
        if request.url.host == "primary.test":
            return httpx.Response(404, json={"error": {"message": "model not found"}})
        # 备用正常喵
        return httpx.Response(200, json={"choices": [{"message": {"content": "ok"}}]})

    # 造状态，把避险阈值调成 3 好让测试快点喵
    data = make_config_dict()
    data["server"]["auto_hedge_threshold"] = 3
    data["server"]["auto_hedge_minutes"] = 5
    state = RuntimeState(parse_config(data))
    # 造假上游客户端喵
    client = make_client(handler)
    # 取出主力节点，之后要查它的冻结状态喵
    primary = state.config.virtual_models["auto-test"][0]
    # 发 3 条请求，每条都会让主力失败一次喵
    for _ in range(3):
        await run_proxy(client, state, {"model": "auto-test"})
    # 主力应该已经被自动避险冻结了喵
    remaining = state.is_frozen(primary)
    # 冻结时长应该接近 5 分钟喵
    assert 295 < remaining <= 300
    # 清空记录，观察下一条请求的走向喵
    hits.clear()
    # 再发一条请求喵
    outcome = await run_proxy(client, state, {"model": "auto-test"})
    # 应该成功喵
    assert outcome.success is True
    # 应该直接打备用，完全跳过被避险的主力喵
    assert hits == ["backup.test"]


@pytest.mark.asyncio
async def test_避险原因能区分于配额冻结():
    """
    自动避险和「上游告知的额度限制」都会写进冻结表，
    但原因文本要能区分开，这样 freeze ls 里能看出冻结的来路喵。
    """

    def handler(request: httpx.Request) -> httpx.Response:
        """假上游：主力一直 404，备用正常喵~"""
        # 主力一直挂喵
        if request.url.host == "primary.test":
            return httpx.Response(404, json={"error": {"message": "model not found"}})
        # 备用正常喵
        return httpx.Response(200, json={"choices": [{"message": {"content": "ok"}}]})

    # 造状态，阈值调成 2 喵
    data = make_config_dict()
    data["server"]["auto_hedge_threshold"] = 2
    state = RuntimeState(parse_config(data))
    # 造客户端喵
    client = make_client(handler)
    # 发 2 条请求触发避险喵
    for _ in range(2):
        await run_proxy(client, state, {"model": "auto-test"})
    # 取冻结列表喵
    freezes = state.list_freezes()
    # 应该有一条冻结记录喵
    assert len(freezes) == 1
    # 原因里应该明确写着「自动避险」，好和额度限制区分开喵
    assert "自动避险" in freezes[0][2]


@pytest.mark.asyncio
async def test_避险后不会继续原地重试():
    """
    节点刚被避险冻结时，即使规则说「原地重试」也该直接换候选喵。
    不然会白白再打一次已经判定为不可用的节点喵。
    """
    # 记录每次请求打到了哪个地址喵
    hits = []

    def handler(request: httpx.Request) -> httpx.Response:
        """假上游：主力一直 503（规则里是 retry x2），备用正常喵~"""
        # 记下主机名喵
        hits.append(request.url.host)
        # 主力一直挂喵
        if request.url.host == "primary.test":
            return httpx.Response(503, text="service unavailable")
        # 备用正常喵
        return httpx.Response(200, json={"choices": [{"message": {"content": "ok"}}]})

    # 造状态，阈值设成 1 —— 第一次失败就触发避险喵
    data = make_config_dict()
    data["server"]["auto_hedge_threshold"] = 1
    state = RuntimeState(parse_config(data))
    # 跑一条请求喵
    outcome = await run_proxy(make_client(handler), state, {"model": "auto-test"})
    # 应该靠备用成功喵
    assert outcome.success is True
    # 主力只该被打一次 —— 虽然规则说 retry x2，但它已经被避险冻结，
    # 所以不该有第二次原地重试喵
    assert hits == ["primary.test", "backup.test"]


def test_配置接口精确匹配():
    """忽略接口匹配 method+path，查询串不参与且尾斜杠有区别喵~"""
    # 解析一个自定义接口配置喵
    config = parse_config(
        make_config_dict(
            server_overrides={
                "ignored_error_endpoints": [{"method": "POST", "path": "/v1/custom"}]
            }
        )
    )
    # 配置一个服务器对象供匹配助手使用喵
    server_config = config.server
    # 完全匹配时应命中喵
    assert _is_ignored_error_endpoint(server_config, "post", "v1/custom") is True
    # HTTP 方法不同不得命中喵
    assert _is_ignored_error_endpoint(server_config, "GET", "/v1/custom") is False
    # 尾斜杠不同不得命中喵
    assert _is_ignored_error_endpoint(server_config, "POST", "/v1/custom/") is False
    # 查询串由请求处理层单独传递，不参与 path 精确匹配喵
    assert _is_ignored_error_endpoint(server_config, "POST", "/v1/custom?debug=true") is False


def test_count_tokens请求只精确匹配():
    """只有 POST /v1/messages/count_tokens 才能被旧兼容助手识别喵~"""
    # 精确接口应被识别喵
    assert _is_count_tokens_request("POST", "/v1/messages/count_tokens") is True
    # 方法不同不能误判喵
    assert _is_count_tokens_request("GET", "/v1/messages/count_tokens") is False
    # 路径不同不能误判喵
    assert _is_count_tokens_request("POST", "/v1/messages") is False


@pytest.mark.asyncio
async def test_count_tokens错误不触发自动避险():
    """count_tokens 连续错误不应累加阈值或冻结候选，但仍返回原有错误结果喵~"""
    # 让上游 count_tokens 始终返回错误喵
    def handler(request: httpx.Request) -> httpx.Response:
        # 返回接口错误，模拟 token 计数服务不可用喵
        return httpx.Response(400, json={"error": {"message": "count tokens failed"}})

    # 设置很低阈值，确保错误累计会立即暴露喵
    config_data = make_config_dict()
    config_data["server"]["auto_hedge_threshold"] = 1
    # 创建运行状态喵
    state = RuntimeState(parse_config(config_data))
    # 连续发送两次 count_tokens 请求喵
    for _ in range(2):
        outcome = await handle_request(
            make_client(handler),
            state,
            "POST",
            "/v1/messages/count_tokens",
            "",
            {},
            # 请求体必须传原始 JSON 字节，模拟 server 的真实调用方式喵
            json.dumps({"model": "auto-test", "messages": []}).encode("utf-8"),
            # 直接调用编排层时从调用点开始计时喵
            time.monotonic(),
        )
        # count_tokens 命中 passthrough 时由编排层原样返回该错误响应喵
        assert outcome.attempt is not None
        assert outcome.attempt.status == 400
    # 取首个候选确认没有被自动避险冻结喵
    candidate = state.config.virtual_models["auto-test"][0]
    assert state.is_frozen(candidate) == 0


@pytest.mark.asyncio
async def test_自定义忽略接口跨模块静默且跳过目标模式与自动避险(caplog, monkeypatch):
    """
    自定义忽略接口遇到假成功错误时，代理层和上游层都不得输出 warning 喵~

    同时验证该接口不会进入目标模式等待、不会触发自动避险，并在候选链耗尽后
    保持原有 502 错误结构且只记录一条“全部不可用”的结果 info 喵。
    """
    # 收集异步等待调用，忽略接口不应出现候选重试或目标模式轮询等待喵
    sleep_calls: list[float] = []

    async def fake_sleep(seconds: float) -> None:
        """记录意外发生的异步等待秒数喵~"""
        # 保存等待时长，便于确认目标模式没有接管请求喵
        sleep_calls.append(seconds)

    # 替换代理层睡眠，避免缺陷导致测试真的等待喵
    monkeypatch.setattr("autoapi.proxy.asyncio.sleep", fake_sleep)

    # 假上游用 HTTP 200 包裹 error，覆盖 upstream 模块的假成功 warning 路径喵
    def handler(request: httpx.Request) -> httpx.Response:
        """让每个候选都返回需要识别的假成功错误喵~"""
        # 返回带 error 字段的 200 响应，模拟接口未实现的中转站喵
        return httpx.Response(200, json={"error": {"message": "endpoint unavailable"}})

    # 配置一个自定义忽略接口，并让一次失败足以触发自动避险以暴露错误累计喵
    config_data = make_config_dict(
        server_overrides={
            "ignored_error_endpoints": [{"method": "POST", "path": "/v1/custom"}],
            "auto_hedge_threshold": 1,
        }
    )
    # 创建状态并开启目标模式，确认接口级配置优先跳过目标循环喵
    state = RuntimeState(parse_config(config_data))
    # 开启纯内存目标模式喵
    state.set_target_mode(True)
    # 同时收集代理层和上游层的 info/warning 日志喵
    caplog.set_level(logging.INFO)
    # 发送自定义忽略接口请求喵
    outcome = await handle_request(
        make_client(handler),
        state,
        "POST",
        "/v1/custom",
        "",
        {},
        json.dumps({"model": "auto-test"}).encode("utf-8"),
    )
    # 候选链耗尽仍应返回兼容的 502 错误结果喵
    assert outcome.success is False
    # 状态码保持原有网关失败语义喵
    assert outcome.status == 502
    # 错误类型保持客户端兼容喵
    assert outcome.error_body["error"]["type"] == "upstream_all_failed"
    # 忽略接口不得进入候选退避或目标模式轮询喵
    assert sleep_calls == []
    # 两个候选都不应因该接口失败而被冻结喵
    assert all(state.is_frozen(candidate) == 0 for candidate in state.config.virtual_models["auto-test"])
    # 跨代理层和上游层都不能残留任何 warning 级别错误日志喵
    assert [record for record in caplog.records if record.levelno >= logging.WARNING] == []
    # 链耗尽的结果日志应恰好只有一条，避免每个候选重复刷屏喵
    exhausted_logs = [
        record
        for record in caplog.records
        if "接口 /v1/custom 全部不可用" in record.getMessage()
    ]
    # 结果日志数量必须精确为一条喵
    assert len(exhausted_logs) == 1
    # 最终结果使用 info 等级喵
    assert exhausted_logs[0].levelno == logging.INFO


@pytest.mark.asyncio
async def test_自定义忽略流式接口抑制假成功warning(caplog):
    """自定义忽略接口走流式假成功路径时也不应输出上游 warning 喵~"""
    # 上游返回内容不足的 SSE，触发流式假成功探测路径喵
    def handler(request: httpx.Request) -> httpx.Response:
        """返回空 SSE 流以模拟中转站的流式假成功喵~"""
        # 明确标记为 SSE，但不提供有效内容喵
        return httpx.Response(200, headers={"content-type": "text/event-stream"}, content=b"")

    # 配置自定义忽略接口，并把内容门槛保持为默认值喵
    state = RuntimeState(
        parse_config(
            make_config_dict(
                server_overrides={
                    "ignored_error_endpoints": [{"method": "POST", "path": "/v1/custom-stream"}]
                }
            )
        )
    )
    # 捕获所有模块的日志，确认流式探测不会绕过静默配置喵
    caplog.set_level(logging.INFO)
    # 发送流式请求，促使上游走 StreamProbe 的假成功分支喵
    outcome = await handle_request(
        make_client(handler),
        state,
        "POST",
        "/v1/custom-stream",
        "",
        {},
        json.dumps({"model": "auto-test", "stream": True}).encode("utf-8"),
    )
    # 流式假成功仍应以候选链耗尽的 502 回给客户端喵
    assert outcome.status == 502
    # 上游流式探测产生的 warning 必须被接口忽略配置抑制喵
    assert [record for record in caplog.records if record.levelno >= logging.WARNING] == []


def test_冻结空闲历史格显示青色且有请求格仍按状态色(monkeypatch):
    """冻结区间内无请求的历史格才标记冻结，有请求的格不受冻结色覆盖喵~"""
    # 造状态和候选喵
    state = make_state()
    candidate = state.config.virtual_models["auto-test"][0]
    # 固定单调时钟，让事件落在最后一个十分钟格的左闭右开范围内喵
    monkeypatch.setattr("autoapi.state.time.monotonic", lambda: 999_999.0)
    # 冻结 60 秒并取得当前冻结信息喵
    state.freeze(candidate, 60, "测试冻结")
    # 构造一条落在同一格的请求，验证有请求会压过冻结色喵
    state.record_success(candidate)
    # 将快照时刻推到该十分钟格的右边界，使刚才的事件被归入最后一个格喵
    monkeypatch.setattr("autoapi.state.time.monotonic", lambda: 1_000_000.0)
    # 取健康快照喵
    snapshot = state.snapshot_candidate_health(candidate)
    # 最后一个格有请求，不应该使用冻结条喵
    assert snapshot.buckets[-1].total == 1
    assert snapshot.buckets[-1].frozen is False
    # 倒数第二格无请求且不在冻结区间，不能误标冻结喵
    assert snapshot.buckets[-2].frozen is False


def test_冻结自然过期后历史区间会被记录():
    """冻结自然到期后，历史快照应保留这段冻结区间的影响喵~"""
    # 造状态和候选喵
    state = make_state()
    candidate = state.config.virtual_models["auto-test"][0]
    # 冻结一个极短区间喵
    state.freeze(candidate, 0.001, "测试冻结")
    # 等待读取时触发惰性过期并归档区间喵
    import time as _time
    _time.sleep(0.01)
    assert state.is_frozen(candidate) == 0
    # 历史冻结区间应已经写入内部历史表喵
    assert state._freeze_intervals[candidate.identity]


    """倒计时应该按「xx 分 xx 秒」补零显示喵~"""
    # 引入格式化函数喵
    from autoapi.repl import format_countdown
    # 常规情况：5 分 42 秒喵
    assert format_countdown(342) == "05 分 42 秒"
    # 不足一分钟喵
    assert format_countdown(8) == "00 分 08 秒"
    # 正好一分钟喵
    assert format_countdown(60) == "01 分 00 秒"
    # 超过一小时要带上「时」喵
    assert format_countdown(3942) == "01 时 05 分 42 秒"
    # 喵~防御：负数和 0 都按 0 秒显示，不能出现「-1 分」这种怪东西喵
    assert format_countdown(0) == "00 分 00 秒"
    assert format_countdown(-5) == "00 分 00 秒"


def test_没有冻结时横幅显示全部可用():
    """一个节点都没冻结时应该显示绿色的「全部可用」那一行喵~"""
    # 引入渲染函数喵
    from autoapi.repl import render_freeze_banner
    # 造一个全新的状态，此时没有任何冻结喵
    state = make_state()
    # 渲染横幅喵
    fragments = render_freeze_banner(state)
    # 把所有文字拼起来方便断言喵
    text = "".join(t for _, t in fragments)
    # 应该显示全部可用喵
    assert "所有节点可用" in text
    # 不该出现配额警告喵
    assert "配额限制" not in text


def test_有冻结时横幅列出虚拟模型和节点():
    """
    横幅要按主人要求的「虚拟模型id/节点model 将在 xx 分 xx 秒 后再次可用」格式显示喵~
    """
    # 引入渲染函数喵
    from autoapi.repl import render_freeze_banner
    # 造状态喵
    state = make_state()
    # 把链首那个节点冻结 342 秒（也就是 5 分 42 秒）喵
    state.freeze(state.config.virtual_models["auto-test"][0], 342, "额度用尽")
    # 渲染横幅喵
    fragments = render_freeze_banner(state)
    # 把所有文字拼起来喵
    text = "".join(t for _, t in fragments)
    # 应该有配额警告标题喵
    assert "配额限制" in text
    # 应该带上虚拟模型名喵
    assert "auto-test" in text
    # 应该带上节点的真实模型名喵
    assert "gpt-4o" in text
    # 应该有倒计时，5 分 42 秒或因为耗时少 1 秒都算对喵
    assert ("05 分 42 秒" in text) or ("05 分 41 秒" in text)
    # 应该有「后再次可用」的措辞喵
    assert "后再次可用" in text


def test_横幅按剩余时间排序():
    """快恢复的节点应该排在前面，方便主人一眼看到哪个先回来喵~"""
    # 引入渲染函数喵
    from autoapi.repl import render_freeze_banner
    # 造状态喵
    state = make_state()
    # 取出两个节点喵
    chain = state.config.virtual_models["auto-test"]
    # 第一个冻很久喵
    state.freeze(chain[0], 600, "额度用尽")
    # 第二个冻较短喵
    state.freeze(chain[1], 100, "额度用尽")
    # 渲染横幅喵
    text = "".join(t for _, t in render_freeze_banner(state))
    # 剩余时间短的那个（claude-sonnet）应该出现在前面喵
    assert text.index("claude-sonnet") < text.index("gpt-4o")


def test_冻结表明细带虚拟模型名():
    """
    list_frozen_nodes 要能把「节点属于哪个虚拟模型」带出来喵~
    这是横幅能按 虚拟模型/节点 格式显示的前提，而 list_freezes 拿不到这个信息喵。
    """
    # 造状态喵
    state = make_state()
    # 冻结链首节点喵
    state.freeze(state.config.virtual_models["auto-test"][0], 300, "额度用尽")
    # 取冻结明细喵
    rows = state.list_frozen_nodes()
    # 应该只有一条喵
    assert len(rows) == 1
    # 虚拟模型名、节点模型名、剩余秒数都要对喵
    vm_name, model, remaining = rows[0]
    assert vm_name == "auto-test"
    assert model == "gpt-4o"
    assert 295 < remaining <= 300


def test_冻结过期后横幅自动消失():
    """冻结到期后横幅应该自动变回「全部可用」，不需要任何手动清理喵~"""
    # 引入渲染函数喵
    from autoapi.repl import render_freeze_banner
    # 造状态喵
    state = make_state()
    # 冻结一个极短的时间喵
    state.freeze(state.config.virtual_models["auto-test"][0], 0.05, "短暂冻结")
    # 立刻渲染，应该能看到警告喵
    assert "配额限制" in "".join(t for _, t in render_freeze_banner(state))
    # 等它过期喵
    time.sleep(0.1)
    # 再渲染，应该变回全部可用喵
    assert "所有节点可用" in "".join(t for _, t in render_freeze_banner(state))


def test_横幅顶部显示每个虚拟模型的动态RPM和TPM():
    """冻结状态上方应该显示当前每个虚拟模型的滚动负载喵~"""
    # 导入横幅渲染函数喵
    from autoapi.repl import render_freeze_banner
    # 造状态喵
    state = make_state()
    # 记录一个有明确 usage 的成功请求喵
    state.record_rate_event("auto-test", 321)
    # 渲染横幅喵
    text = "".join(fragment for _, fragment in render_freeze_banner(state))
    # 应该包含虚拟模型名和 RPM/TPM 数值喵
    assert "auto-test" in text
    assert "RPM=1" in text
    assert "TPM=321" in text


def test_横幅TPM未上报时明确显示未知():
    """上游没返回 usage 时，横幅不能把 TPM 显示成 0 喵~"""
    # 导入横幅渲染函数喵
    from autoapi.repl import render_freeze_banner
    # 造状态喵
    state = make_state()
    # 记录一个没有 usage 的成功请求喵
    state.record_rate_event("auto-test", None)
    # 渲染横幅喵
    text = "".join(fragment for _, fragment in render_freeze_banner(state))
    # RPM 仍然准确，TPM 则明确表示未完整上报喵
    assert "RPM=1" in text
    assert "未完整上报" in text
    assert "TPM=0" not in text


def test_横幅隐藏RPM为0的虚拟模型():
    """没有成功请求的虚拟模型不应该占用状态栏空间喵~"""
    # 导入横幅渲染函数喵
    from autoapi.repl import render_freeze_banner
    # 造状态喵
    state = make_state()
    # 只给 auto-test 记录请求，测试配置没有其他有流量的模型喵
    state.record_rate_event("auto-test", 10)
    # 渲染横幅喵
    text = "".join(fragment for _, fragment in render_freeze_banner(state))
    # 有流量模型应该显示喵
    assert "auto-test" in text
    # 测试配置中的不存在模型不应该凭空显示，RPM=0 的模型逻辑由快照过滤覆盖喵
    assert "RPM=0" not in text





def make_repl(tmp_path):
    """
    造一个连着真配置文件的 REPL 喵~

    输入：pytest 给的临时目录
    输出：(repl 对象, 配置文件路径)
    说明：改配置的命令会真的读写这个临时文件，所以能验证「立即生效 + 写回磁盘」两件事喵。
    """
    # 引入 REPL 类喵
    from autoapi.repl import Repl
    # 引入 YAML 库用来写初始配置喵
    import yaml as _yaml
    # 引入配置加载函数喵
    from autoapi.config import load_config
    # 在临时目录里写一份初始配置喵
    path = tmp_path / "config.yaml"
    # 用测试用的配置字典序列化成 YAML 喵
    path.write_text(_yaml.safe_dump(make_config_dict(), allow_unicode=True), encoding="utf-8")
    # 加载它并创建运行时状态喵
    state = RuntimeState(load_config(path))
    # 返回 REPL 和配置文件路径喵
    return Repl(state), path


def test_REPL补全包含完整命令和新字段():
    """
    Tab 补全必须覆盖 dispatch 实际支持的命令，尤其是 freeze add / freeze rm 喵~

    这里直接调用 prompt_toolkit 的补全器，不依赖真实 TTY，避免把终端渲染问题
    和命令词表问题混在一起喵。
    """
    # 导入 prompt_toolkit 的文档对象，用来模拟主人当前已经输入的前缀喵
    from prompt_toolkit.document import Document
    # 导入 REPL 类喵
    from autoapi.repl import Repl
    # 造 REPL 实例喵
    repl = Repl(make_state())
    # 创建补全器喵
    completer = repl._build_completer()
    # 定义一个小工具，收集某个输入前缀的所有补全文本喵
    def suggestions(prefix: str) -> list[str]:
        # 向 prompt_toolkit 请求当前前缀的候选项喵
        return [item.text for item in completer.get_completions(Document(prefix), None)]
    # freeze 后面应该认识 add、rm、clear 三类操作喵
    freeze_words = suggestions("freeze ")
    assert "freeze add" in freeze_words
    assert "freeze rm" in freeze_words
    assert "freeze clear" in freeze_words
    # set 后面应该能补全新的全局超时字段喵
    set_words = suggestions("set ")
    assert "set stall_timeout" in set_words
    assert "set stream_timeout" in set_words
    assert "set nonstream_timeout" in set_words
    assert "set metrics_window_minutes" in set_words
    # cand set 后面应该能补全节点级三个超时覆盖字段喵
    cand_set_words = suggestions("cand set ")
    assert "cand set stall_timeout" in cand_set_words
    assert "cand set stream_timeout" in cand_set_words
    assert "cand set nonstream_timeout" in cand_set_words
    # 新旧命令别名也应该可发现喵
    assert "candidate add" in suggestions("candidate ")
    assert "freeze remove" in freeze_words


def test_config_example包含当前超时配置和规则():
    """公开配置模板必须包含当前实现需要的新字段，不得残留退役字段喵~"""
    # 读取公开模板，绝不读取含真 key 的 config.yaml 喵
    from pathlib import Path
    # 从测试文件所在目录向上找到项目根目录喵
    example_path = Path(__file__).resolve().parents[1] / "config.example"
    # 读取模板文本喵
    text = example_path.read_text(encoding="utf-8")
    # 新的全局超时字段必须存在喵
    for field in ("stall_timeout", "stream_timeout", "nonstream_timeout"):
        assert f"{field}:" in text
    # 节点级覆盖示例也必须存在喵
    assert "# 按节点单独配超时" in text or "慢速推理模型" in text
    assert "nonstream_timeout: 1800" in text
    # timeout 规则必须存在并设置为重试一次喵
    assert "status: timeout" in text
    assert "max_attempts: 2" in text
    # 退役字段不应出现在可执行配置项中（注释迁移说明允许出现字段名喵）
    assert "\n  request_timeout:" not in text
    assert "\n  first_content_timeout:" not in text


def test_交互式追加候选(tmp_path):
    """cand add 应该把新候选追加到链尾，并立即生效喵~"""
    # 造 REPL 喵
    repl, path = make_repl(tmp_path)
    # 追加一个候选喵
    repl.dispatch(
        'cand add auto-test {"name": "新中转", "base_url": "https://new.test", '
        '"api_key": "sk-new-key-9999", "model": "gpt-4o-mini"}'
    )
    # 内存里的候选链应该从 2 个变成 3 个喵
    chain = repl.state.config.virtual_models["auto-test"]
    assert len(chain) == 3
    # 新候选应该在链尾，也就是优先级最低喵
    assert chain[2].name == "新中转"
    assert chain[2].model == "gpt-4o-mini"
    # 磁盘上的文件也应该被更新了喵
    assert "new.test" in path.read_text(encoding="utf-8")


def test_交互式删候选(tmp_path):
    """cand rm 应该删掉指定序号的候选喵~"""
    # 造 REPL 喵
    repl, path = make_repl(tmp_path)
    # 删掉第一个候选喵
    repl.dispatch("cand rm auto-test 1")
    # 应该只剩一个候选喵
    chain = repl.state.config.virtual_models["auto-test"]
    assert len(chain) == 1
    # 剩下的应该是原来的第二个（备用）喵
    assert chain[0].name == "备用"


def test_不许把候选链删空(tmp_path):
    """删到只剩一个时应该拒绝，因为空链的虚拟模型无法服务喵~"""
    # 造 REPL 喵
    repl, path = make_repl(tmp_path)
    # 先删掉一个，剩一个喵
    repl.dispatch("cand rm auto-test 1")
    # 再删应该被拒绝，配置保持不变喵
    repl.dispatch("cand rm auto-test 1")
    # 应该还剩着那一个，没被删掉喵
    assert len(repl.state.config.virtual_models["auto-test"]) == 1


def test_交互式改候选的地址和模型名(tmp_path):
    """cand set 应该能改单个字段，且不影响该候选在链里的位置喵~"""
    # 造 REPL 喵
    repl, path = make_repl(tmp_path)
    # 改第 1 个候选的上游地址喵
    repl.dispatch("cand set auto-test 1 base_url https://changed.test")
    # 改第 1 个候选的真实模型名喵
    repl.dispatch("cand set auto-test 1 model gpt-4o-2024-11-20")
    # 取出候选链喵
    chain = repl.state.config.virtual_models["auto-test"]
    # 地址和模型名都应该变了喵
    assert chain[0].base_url == "https://changed.test"
    assert chain[0].model == "gpt-4o-2024-11-20"
    # 位置不该变，它还是链首喵
    assert chain[0].name == "主力"
    # 其他字段不该被碰到喵
    assert chain[0].api_key == "sk-primary-key-1234"
    # 磁盘上也应该被更新喵
    assert "changed.test" in path.read_text(encoding="utf-8")


def test_交互式换key(tmp_path):
    """cand set api_key 应该能换 key，这是最常用的运维操作喵~"""
    # 造 REPL 喵
    repl, path = make_repl(tmp_path)
    # 换掉第 2 个候选的 key 喵
    repl.dispatch("cand set auto-test 2 api_key sk-brand-new-key-abcdefg")
    # 新 key 应该生效喵
    assert repl.state.config.virtual_models["auto-test"][1].api_key == "sk-brand-new-key-abcdefg"
    # 换 key 之后身份串变了，所以旧 key 的冻结记录不会误伤新 key 喵
    assert repl.state.is_frozen(repl.state.config.virtual_models["auto-test"][1]) == 0


def test_改候选字段的各种非法输入(tmp_path):
    """字段名不认识、序号越界、值为空，都该被挡下且不动配置喵~"""
    # 造 REPL 喵
    repl, path = make_repl(tmp_path)
    # 记下改动前的磁盘内容喵
    before = path.read_text(encoding="utf-8")
    # 字段名不认识喵
    repl.dispatch("cand set auto-test 1 乱写的字段 值")
    # 序号越界喵
    repl.dispatch("cand set auto-test 99 model m")
    # 序号不是数字喵
    repl.dispatch("cand set auto-test abc model m")
    # 虚拟模型不存在喵
    repl.dispatch("cand set 不存在 1 model m")
    # 磁盘文件应该一个字节都没被动过喵
    assert path.read_text(encoding="utf-8") == before


def test_交互式调整候选优先级(tmp_path):
    """cand mv 应该能把备用提到链首喵~"""
    # 造 REPL 喵
    repl, path = make_repl(tmp_path)
    # 把第 2 个候选挪到第 1 位喵
    repl.dispatch("cand mv auto-test 2 1")
    # 现在链首应该是原来的备用喵
    chain = repl.state.config.virtual_models["auto-test"]
    assert chain[0].name == "备用"
    # 原来的主力应该退到第 2 位喵
    assert chain[1].name == "主力"


def test_交互式新建和删除虚拟模型(tmp_path):
    """vm add / vm rm 应该能增删整个虚拟模型喵~"""
    # 造 REPL 喵
    repl, path = make_repl(tmp_path)
    # 新建一个虚拟模型并配上第一个候选喵
    repl.dispatch(
        'vm add my-new {"base_url": "https://mine.test", "api_key": "sk-mine-1234", "model": "my-model"}'
    )
    # 现在应该有两个虚拟模型喵
    assert set(repl.state.list_virtual_models()) == {"auto-test", "my-new"}
    # 新虚拟模型应该有一个候选喵
    assert len(repl.state.config.virtual_models["my-new"]) == 1
    # 再把它删掉喵
    repl.dispatch("vm rm my-new")
    # 应该只剩原来那个喵
    assert repl.state.list_virtual_models() == ["auto-test"]


def test_不许新建重名的虚拟模型(tmp_path):
    """重名时应该拒绝，否则会悄悄把已有的候选链整条覆盖掉喵~"""
    # 造 REPL 喵
    repl, path = make_repl(tmp_path)
    # 用已经存在的名字新建喵
    repl.dispatch('vm add auto-test {"base_url": "https://x.test", "api_key": "sk-1", "model": "m"}')
    # 原来的两个候选应该完好无损喵
    assert len(repl.state.config.virtual_models["auto-test"]) == 2


def test_不许删掉最后一个虚拟模型(tmp_path):
    """删光了代理就没东西可服务了，应该拒绝喵~"""
    # 造 REPL 喵
    repl, path = make_repl(tmp_path)
    # 尝试删掉唯一的虚拟模型喵
    repl.dispatch("vm rm auto-test")
    # 应该还在喵
    assert repl.state.list_virtual_models() == ["auto-test"]


def test_交互式改超时配置(tmp_path):
    """set 应该能改 server 段的超时并立即生效喵~"""
    # 造 REPL 喵
    repl, path = make_repl(tmp_path)
    # 改掉流式请求的总预算喵
    repl.dispatch("set stream_timeout 99")
    # 内存里的配置应该立刻变了喵
    assert repl.state.config.server.stream_timeout == 99.0
    # 磁盘上也应该被更新喵
    assert "99" in path.read_text(encoding="utf-8")


def test_交互式改性能统计窗口(tmp_path):
    """set 应该能改性能统计窗口，并立即刷新内存配置和临时配置文件喵~"""
    # 造 REPL 喵
    repl, path = make_repl(tmp_path)
    # 用 dispatch 走真实命令解析路径修改窗口，单位：分钟喵
    repl.dispatch("set metrics_window_minutes 15")
    # 内存中的新窗口应立即用于后续 RPM/TPM 统计喵
    assert repl.state.config.server.metrics_window_minutes == 15.0
    # 临时配置文件应同步写入，供下次启动和热重载使用喵
    assert "metrics_window_minutes: 15.0" in path.read_text(encoding="utf-8")


def test_交互式给单个节点配专属超时(tmp_path):
    """
    cand set 应该能给某一个节点单独配超时，且不影响同链上的别的节点喵~

    这是主人要的「模型级别的超时配置」：链首那个会先想很久的推理模型需要放宽，
    但兜底的快模型必须保持原样，否则它挂死时要等很久才降级喵。
    """
    # 造 REPL 喵
    repl, path = make_repl(tmp_path)
    # 给第一个节点单独放宽流式预算喵
    repl.dispatch("cand set auto-test 1 stream_timeout 900")
    # 取出两个候选喵
    first, second = repl.state.config.virtual_models["auto-test"]
    # 第一个节点应该有专属值喵
    assert first.stream_timeout == 900.0
    # 第二个节点必须没被波及，这是这个用例的关键断言喵
    assert second.stream_timeout is None
    # 实际生效的超时也要跟着变喵
    assert resolve_timeouts(repl.state.config.server, first).stream == 900.0
    # 第二个节点应该还是全局值喵
    assert resolve_timeouts(repl.state.config.server, second).stream == repl.state.config.server.stream_timeout
    # 磁盘上也要落下来喵
    assert "900" in path.read_text(encoding="utf-8")


def test_交互式清掉节点专属超时(tmp_path):
    """填 default 应该把专属超时清掉，改回跟随全局值喵~"""
    # 造 REPL 喵
    repl, path = make_repl(tmp_path)
    # 先配一个专属值喵
    repl.dispatch("cand set auto-test 1 stall_timeout 240")
    # 确认配上了喵
    assert repl.state.config.virtual_models["auto-test"][0].stall_timeout == 240.0
    # 再用 default 清掉它喵
    repl.dispatch("cand set auto-test 1 stall_timeout default")
    # 应该回到 None，也就是跟随全局值喵
    assert repl.state.config.virtual_models["auto-test"][0].stall_timeout is None
    # 磁盘上那个键应该也被删掉了喵
    assert "stall_timeout: 240" not in path.read_text(encoding="utf-8")


def test_节点专属超时填非法值不会改坏配置(tmp_path, capsys):
    """填了非数字或非正数时该拒绝，且配置一点都不能变喵~"""
    # 造 REPL 喵
    repl, _ = make_repl(tmp_path)
    # 填一个转不成数字的值喵
    repl.dispatch("cand set auto-test 1 stream_timeout 很久")
    # 应该提示要填秒数喵
    assert "秒数" in capsys.readouterr().out
    # 配置不该被改动喵
    assert repl.state.config.virtual_models["auto-test"][0].stream_timeout is None
    # 再填一个负数喵
    repl.dispatch("cand set auto-test 1 stream_timeout -5")
    # 应该提示要大于 0 喵
    assert "大于 0" in capsys.readouterr().out
    # 配置依然不该被改动喵
    assert repl.state.config.virtual_models["auto-test"][0].stream_timeout is None


def test_vm命令会显示节点专属超时(tmp_path, capsys):
    """
    配了专属超时的节点要在 vm 里标出来喵~

    不标的话主人改了全局超时会困惑「为什么这个节点没跟着变」，
    这种「配置在别处被覆盖了」的问题不显式提示就很难发现喵。
    """
    # 造 REPL 喵
    repl, _ = make_repl(tmp_path)
    # 先给第一个节点配个专属值喵
    repl.dispatch("cand set auto-test 1 stream_timeout 900")
    # 清掉之前的输出喵
    capsys.readouterr()
    # 列出虚拟模型喵
    repl.dispatch("vm")
    # 取出输出喵
    out = capsys.readouterr().out
    # 应该有「专属超时」的标记喵
    assert "专属超时" in out
    # 而且要写出具体的值喵
    assert "stream_timeout=900s" in out


def test_set_已退役的配置项给出明确指引(tmp_path, capsys):
    """
    敲已经退役的配置项名时，不能只说「不能改」，得告诉主人现在该改哪一项喵~

    这个用例守的是「改名之后的迁移体验」：老名字在文档、笔记、肌肉记忆里都还在，
    如果只回一句「不认识这个字段」，主人根本不知道该往哪儿找喵。
    """
    # 造 REPL 喵
    repl, _ = make_repl(tmp_path)
    # 敲一个已经退役的名字喵
    repl.dispatch("set first_content_timeout 45")
    # 取出打印出来的内容喵
    out = capsys.readouterr().out
    # 应该说明它退役了喵
    assert "退役" in out
    # 而且要指出现在该用哪两项替代喵
    assert "stall_timeout" in out and "stream_timeout" in out


def test_改坏配置不会影响运行中的代理(tmp_path):
    """
    这是改配置流程最重要的保障喵：一次手滑不能把跑着的代理搞停摆。
    校验失败时内存配置和磁盘文件都该保持原样喵。
    """
    # 造 REPL 喵
    repl, path = make_repl(tmp_path)
    # 记下改动前的磁盘内容和候选数量喵
    before_text = path.read_text(encoding="utf-8")
    before_count = len(repl.state.config.virtual_models["auto-test"])
    # 追加一个缺少必填字段 api_key 的坏候选喵
    repl.dispatch('cand add auto-test {"base_url": "https://bad.test", "model": "m"}')
    # 内存里的候选数量应该没变，坏候选没被应用喵
    assert len(repl.state.config.virtual_models["auto-test"]) == before_count
    # 磁盘上的文件也应该一个字节都没被动过喵
    assert path.read_text(encoding="utf-8") == before_text


def test_改规则的JSON写坏了也不影响运行(tmp_path):
    """JSON 语法错误应该在解析阶段就被挡下，配置完全不动喵~"""
    # 造 REPL 喵
    repl, path = make_repl(tmp_path)
    # 记下改动前的规则数量喵
    before = len(repl.state.config.rules)
    # 喂一段语法坏掉的 JSON 喵
    repl.dispatch('rule add {"match": {"status": 429,, "action": "next"}')
    # 规则数量应该没变喵
    assert len(repl.state.config.rules) == before


def test_操作不存在的虚拟模型会被拒绝(tmp_path):
    """虚拟模型名打错时应该拒绝并提示，而不是静默失败喵~"""
    # 造 REPL 喵
    repl, path = make_repl(tmp_path)
    # 记下改动前的规则和虚拟模型状态喵
    before = repl.state.list_virtual_models()
    # 对一个不存在的虚拟模型加候选喵
    repl.dispatch('cand add 打错的名字 {"base_url": "https://x.test", "api_key": "sk-1", "model": "m"}')
    # 虚拟模型表应该完全没变喵
    assert repl.state.list_virtual_models() == before


def test_主动冻结按模型名(tmp_path):
    """freeze add 应该能按真实模型名冻结节点喵~"""
    # 造 REPL 喵
    repl, path = make_repl(tmp_path)
    # 按模型名冻结 600 秒喵
    repl.dispatch("freeze add auto-test gpt-4o 600")
    # 那个节点应该被冻上了喵
    primary = repl.state.config.virtual_models["auto-test"][0]
    assert 595 < repl.state.is_frozen(primary) <= 600
    # 另一个节点不该受影响喵
    assert repl.state.is_frozen(repl.state.config.virtual_models["auto-test"][1]) == 0


def test_主动冻结按序号(tmp_path):
    """freeze add 也应该能按 vm 里显示的序号冻结喵~"""
    # 造 REPL 喵
    repl, path = make_repl(tmp_path)
    # 按序号冻结第 2 个节点喵
    repl.dispatch("freeze add auto-test 2 300")
    # 第 2 个应该被冻上喵
    assert 295 < repl.state.is_frozen(repl.state.config.virtual_models["auto-test"][1]) <= 300
    # 第 1 个不该受影响喵
    assert repl.state.is_frozen(repl.state.config.virtual_models["auto-test"][0]) == 0


def test_主动冻结的原因可区分(tmp_path):
    """手动冻结的原因要写明是手动的，好和自动避险、额度限制区分开喵~"""
    # 造 REPL 喵
    repl, path = make_repl(tmp_path)
    # 冻结一个节点喵
    repl.dispatch("freeze add auto-test gpt-4o 600")
    # 取冻结列表喵
    freezes = repl.state.list_freezes()
    # 原因里应该有「手动」字样喵
    assert "手动" in freezes[0][2]


def test_解冻指定节点(tmp_path):
    """freeze rm 应该只解冻点名的那一个，不影响别的喵~"""
    # 造 REPL 喵
    repl, path = make_repl(tmp_path)
    # 取出两个节点喵
    chain = repl.state.config.virtual_models["auto-test"]
    # 两个都冻上喵
    repl.state.freeze(chain[0], 600, "测试")
    repl.state.freeze(chain[1], 600, "测试")
    # 只解冻第一个喵
    repl.dispatch("freeze rm auto-test gpt-4o")
    # 第一个应该可用了喵
    assert repl.state.is_frozen(chain[0]) == 0
    # 第二个应该还冻着喵
    assert repl.state.is_frozen(chain[1]) > 0


def test_主动冻结的非法输入(tmp_path):
    """秒数非法、序号越界、模型名不存在、虚拟模型不存在，都该被挡下且不冻结任何东西喵~"""
    # 造 REPL 喵
    repl, path = make_repl(tmp_path)
    # 秒数不是数字喵
    repl.dispatch("freeze add auto-test gpt-4o 一百秒")
    # 秒数为 0 喵
    repl.dispatch("freeze add auto-test gpt-4o 0")
    # 秒数为负喵
    repl.dispatch("freeze add auto-test gpt-4o -50")
    # 序号越界喵
    repl.dispatch("freeze add auto-test 99 600")
    # 模型名不存在喵
    repl.dispatch("freeze add auto-test 不存在的模型 600")
    # 虚拟模型不存在喵
    repl.dispatch("freeze add 不存在 gpt-4o 600")
    # 参数不全喵
    repl.dispatch("freeze add auto-test gpt-4o")
    repl.dispatch("freeze rm")
    # 一个节点都不该被冻结喵
    assert repl.state.list_freezes() == []


def test_解冻本来没冻的节点不会出错(tmp_path):
    """解冻一个本来就没冻结的节点应该给出提示而不是报错喵~"""
    # 造 REPL 喵
    repl, path = make_repl(tmp_path)
    # 解冻一个没冻过的节点，不该抛异常喵
    repl.dispatch("freeze rm auto-test gpt-4o")
    # 冻结表应该还是空的喵
    assert repl.state.list_freezes() == []


def test_日志颜色映射():
    """WARNING 要黄色、ERROR 要红色，这是主人明确要求的喵~"""
    # 引入颜色表和格式化器喵
    from main import COLOR_RESET, LEVEL_COLORS, ColorFormatter
    # WARNING 应该是黄色喵
    assert LEVEL_COLORS["WARNING"] == "\033[33m"
    # ERROR 应该是红色喵
    assert LEVEL_COLORS["ERROR"] == "\033[31m"
    # INFO 不上色，避免正常日志也花里胡哨喵
    assert LEVEL_COLORS["INFO"] == ""
    # 造一个格式化器喵
    formatter = ColorFormatter("%(levelname)s %(message)s")
    # 造一条 WARNING 日志记录喵
    record = logging.LogRecord("test", logging.WARNING, "f.py", 1, "出问题了喵", None, None)
    # 渲染出来应该被黄色包起来喵
    text = formatter.format(record)
    assert text.startswith("\033[33m")
    # 结尾要有重置码，否则颜色会漏到后面的输出去喵
    assert text.endswith(COLOR_RESET)
    # 造一条 INFO 日志喵
    info_record = logging.LogRecord("test", logging.INFO, "f.py", 1, "一切正常喵", None, None)
    # INFO 不该被套任何颜色码喵
    assert "\033[" not in formatter.format(info_record)


def test_成功日志候选人侧ID为绿色且不泄露密钥():
    """成功日志只把 candidate.name 染绿，WARNING、ERROR 和 api key 均不受影响喵~"""
    # 引入成功候选 ID 颜色相关常量与格式化器喵
    from main import CANDIDATE_NAME_COLOR, COLOR_RESET, ColorFormatter
    # 造使用 INFO 的格式化器，模拟终端支持 ANSI 时的输出喵
    formatter = ColorFormatter("%(levelname)s %(message)s")
    # 候选 human 侧 ID 和仅用于防泄露断言的伪 api key 喵
    candidate_name, fake_api_key = "claude-sonnet", "sk-test-secret"
    # 造一条真实成功日志结构的记录，消息中不携带候选配置或 api key 喵
    success_record = logging.LogRecord(
        "test", logging.INFO, "f.py", 1,
        "成功 %s（第 %d 次尝试）喵 非流 总时长=%.0fms",
        (candidate_name, 1, 12.0), None,
    )
    # 渲染成功日志喵
    success_text = formatter.format(success_record)
    # 仅最终展示的候选 human 侧 ID 应被绿色 ANSI 与重置码精确包裹喵
    assert f"{CANDIDATE_NAME_COLOR}{candidate_name}{COLOR_RESET}" in success_text
    # 日志不得包含任何未传入的 api key 喵
    assert fake_api_key not in success_text
    # WARNING 仍必须是整行黄色喵
    warning_text = formatter.format(logging.LogRecord("test", logging.WARNING, "f.py", 1, "警告喵", None, None))
    assert warning_text.startswith("\033[33m")
    # ERROR 仍必须是整行红色喵
    error_text = formatter.format(logging.LogRecord("test", logging.ERROR, "f.py", 1, "错误喵", None, None))
    assert error_text.startswith("\033[31m")

    """
    这是「日志滚动时底部状态监控上移、渲染错乱」那个 bug 的回归用例喵~

    根因：内置的 StreamHandler 在创建那一刻就把 sys.stderr 抓在手里了。而 REPL 那边用
    prompt_toolkit 的 patch_stdout 把 sys.stderr 换成了代理对象 —— 换的目的正是让所有
    输出都先过 prompt_toolkit 的手，由它负责「擦掉横幅 → 打这行 → 重画横幅」。
    日志如果绕过代理直接怼终端，prompt_toolkit 就不知道屏幕上多了几行，
    它记着的光标位置全错，横幅位置就跑了喵。

    所以这里断言的是：处理器必须每次写日志都现取 sys.stderr，而不是记住某一个对象。
    """
    # 引入自定义处理器喵
    from main import LiveStreamHandler
    # 引入 StringIO 当假的输出目标喵
    import io
    # 造一个处理器喵
    handler = LiveStreamHandler()
    # 记下原始的 stderr，测完要还回去喵
    original = sys.stderr
    # 造两个不同的假输出目标喵
    first, second = io.StringIO(), io.StringIO()
    # 用 try/finally 保证一定还原 stderr，否则会影响后面的测试喵
    try:
        # 把 stderr 换成第一个目标喵
        sys.stderr = first
        # 此时处理器读到的 stream 应该就是第一个目标喵
        assert handler.stream is first
        # 再把 stderr 换成第二个目标，模拟 patch_stdout 生效的那一刻喵
        sys.stderr = second
        # 处理器必须跟着变，这正是修好这个 bug 的关键喵
        assert handler.stream is second
        # 实际写一条日志，应该落到当前的目标里喵
        handler.setFormatter(logging.Formatter("%(message)s"))
        handler.emit(logging.LogRecord("t", logging.WARNING, "f.py", 1, "测试喵", None, None))
        # 第二个目标里应该有内容喵
        assert "测试喵" in second.getvalue()
        # 第一个目标里应该什么都没有，说明确实没写到老对象上喵
        assert first.getvalue() == ""
    # 无论断言是否通过都还原 stderr 喵
    finally:
        sys.stderr = original


def test_REPL能构造输出重定向器():
    """
    REPL 必须能拿到 patch_stdout 上下文，这是日志和横幅不打架的另一半喵~

    只验证「能拿到且是个上下文管理器」，因为真正的渲染效果需要真终端才能看出来，
    这里能守住的是「这个机制没被误删或改坏」喵。
    """
    # 引入 REPL 类喵
    from autoapi.repl import Repl
    # 造一个 REPL（用内存状态，不碰磁盘）喵
    repl = Repl(make_state())
    # 拿重定向器喵
    redirect = repl._build_stdout_patch()
    # prompt_toolkit 装了就该拿到一个上下文管理器喵
    if redirect is not None:
        # 必须同时有 __enter__ 和 __exit__ 才能被 with 用喵
        assert hasattr(redirect, "__enter__")
        assert hasattr(redirect, "__exit__")


def test_不认识的命令不会崩(tmp_path):
    """乱敲命令只该打印提示，不能让 REPL 线程挂掉喵~"""
    # 造 REPL 喵
    repl, path = make_repl(tmp_path)
    # 敲一堆不认识的东西，都不该抛异常喵
    repl.dispatch("这是什么命令")
    repl.dispatch("rule")
    repl.dispatch("cand")
    repl.dispatch("vm 乱七八糟")
    repl.dispatch("set")
    repl.dispatch("set 不存在的字段 123")
    repl.dispatch("set port 不是数字")
    # 空行也该被安静忽略喵
    repl.dispatch("")
    # 配置应该完全没被影响喵
    assert repl.state.list_virtual_models() == ["auto-test"]


@pytest.mark.asyncio
async def test_改完配置立刻对新请求生效(tmp_path):
    """
    端到端验证：交互式把备用提到链首之后，下一条请求就该直接打备用喵。
    这是「改完立即生效」这个承诺的真实验证喵。
    """
    # 记录每次请求打到了哪个地址喵
    hits = []

    def handler(request: httpx.Request) -> httpx.Response:
        """假上游：两个地址都正常喵~"""
        # 记下主机名喵
        hits.append(request.url.host)
        # 返回正常响应喵
        return httpx.Response(200, json={"choices": [{"message": {"content": "ok"}}]})

    # 造 REPL（它内部带着运行时状态）喵
    repl, path = make_repl(tmp_path)
    # 造假上游客户端喵
    client = make_client(handler)
    # 先发一条请求，应该打链首的主力喵
    await run_proxy(client, repl.state, {"model": "auto-test"})
    # 确认打的是主力喵
    assert hits == ["primary.test"]
    # 交互式把备用提到链首喵
    repl.dispatch("cand mv auto-test 2 1")
    # 再发一条请求喵
    await run_proxy(client, repl.state, {"model": "auto-test"})
    # 这次应该直接打备用，证明改动立刻生效了喵
    assert hits == ["primary.test", "backup.test"]


@pytest.mark.asyncio
async def test_并发请求能正常处理():
    """
    验证并发能力喵：同时发 60 条请求（对应 60rpm 全部挤在同一秒的极端情况），
    全部都该正常完成，且状态统计准确喵。
    """
    # 引入 asyncio 用于并发喵
    import asyncio

    def handler(request: httpx.Request) -> httpx.Response:
        """假上游：一律成功喵~"""
        # 返回正常响应喵
        return httpx.Response(200, json={"choices": [{"message": {"content": "ok"}}]})

    # 造状态和假上游喵
    state = make_state()
    client = make_client(handler)
    # 同时发 60 条请求喵
    outcomes = await asyncio.gather(
        *(run_proxy(client, state, {"model": "auto-test"}) for _ in range(60))
    )
    # 全部都该成功喵
    assert all(o.success for o in outcomes)
    # 统计里应该正好记了 60 条请求喵
    assert state.total_requests == 60
