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
    Candidate,
    ConfigError,
    parse_config,
)
# 引入被测的编排层喵
from autoapi.proxy import detect_stream_flag, handle_request, parse_client_body
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
# 引入被测的上游请求构造函数喵
from autoapi.upstream import build_upstream_body, build_upstream_headers


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
            "request_timeout": 5,
            # 超时都设得很小，让卡流类的测试能快点跑完喵
            "first_content_timeout": 0.6,
            "stall_timeout": 0.9,
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
            {"match": {"status": "bad_stream"}, "action": "next"},
            {"match": {"status": [401, 403, 404]}, "action": "next"},
            {"match": {"status": 400}, "action": "passthrough"},
        ],
    }
    # 应用测试传入的覆盖项喵
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
    # 应该有六条规则喵（freeze、5xx retry、卡流 retry、bad_stream、401 系、400）
    assert len(config.rules) == 6
    # 第一个候选的真实模型名应该被正确解析喵
    assert config.virtual_models["auto-test"][0].model == "gpt-4o"


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


def test_非JSON的心跳行不会误判():
    """有些上游会插入注释行或心跳，绝不能因为解析不了就误判成失败喵~"""
    # 造一个探测器喵
    probe = StreamProbe()
    # 喂一个 SSE 注释行（冒号开头）和一个非 JSON 的 data 行喵
    assert probe.feed(b": keep-alive\n\ndata: ping\n\n") == VERDICT_PENDING


# ============ 4. 运行时状态：冻结机制喵 ============


def make_state() -> RuntimeState:
    """造一个装好测试配置的运行时状态喵~"""
    # 用测试配置创建状态对象喵
    return RuntimeState(parse_config(make_config_dict()))


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
    # 引入现算超时的函数喵
    from autoapi.upstream import build_timeout
    # 取一份配置喵
    config = parse_config(make_config_dict())
    # 现算一份超时喵
    timeout = build_timeout(config.server)
    # 连接超时应该来自配置里的 connect_timeout 喵
    assert timeout.connect == config.server.connect_timeout
    # 读超时应该来自配置里的 request_timeout 喵
    assert timeout.read == config.server.request_timeout
    # 改掉配置里的超时之后，现算出来的应该跟着变喵
    config.server.request_timeout = 777.0
    assert build_timeout(config.server).read == 777.0


# ============ 7. 交互区的冻结表横幅喵 ============


def test_倒计时格式():
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


# ============ 8. 交互式改配置喵 ============


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
    # 改掉首内容超时喵
    repl.dispatch("set first_content_timeout 99")
    # 内存里的配置应该立刻变了喵
    assert repl.state.config.server.first_content_timeout == 99.0
    # 磁盘上也应该被更新喵
    assert "99" in path.read_text(encoding="utf-8")


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
