"""
SSE 流内容探测模块喵~

这是整个项目最关键的一块，所以先把思路讲清楚喵：

要解决的问题：
    很多上游（尤其是各种中转站）会先回一个漂亮的 200 OK 并且开始吐 SSE 流，然后在流里
    塞一个 error 事件，或者一个字都不吐就直接 [DONE] 收尾。如果代理拿到 200 就立刻把
    字节转给客户端，那这种「假成功」就无法故障转移了 —— 因为字节已经出门，收不回来喵。

    更阴险的一种：上游先吐一两个字符（甚至只吐个标点），然后就一直挂着不再吐东西，
    连接也不断。这种流如果只要求「有一个字符」就放行，会被它骗过去，字节一出门就再也
    换不了候选，客户端只能干等到超时喵。

采取的策略：
    代理先自己读上游的流，把原始字节暂存在缓冲区里，同时同步解析里面的 data: 负载，
    直到满足下面任一条件才把缓冲区连同后续字节一起放给客户端：
        1. 累积的内容字符数达到门槛（默认 10 个）—— 说明上游真的在正常输出
        2. 收到流结束标记且已经吐过内容 —— 说明这是个正常的短回答，不该干等 10 个字
        3. 收到结构上已经证明健康的事件（工具调用增量、finish_reason）—— 这类事件本身
           不带文字，但足以说明模型在正常工作，不能因为「字符数不够」而误判喵
    在放行之前发现 error、空流、或者一直等不到结论（卡流），都能默默换候选或原地重发，
    客户端完全感知不到喵。

为什么要同时留「原始字节」和「解析后的文本」两份：
    放行时必须把原始字节一字节不差地replay给客户端（SSE 的空行分隔、事件名、上游自定义
    字段都得保留，客户端 SDK 才认）；而判断有没有内容又必须解析 JSON。所以两条线并行走，
    解析只用于「做判断」，从不用于「改内容」喵。

同时兼容两种协议的流格式：
    OpenAI 风格： data: {"choices":[{"delta":{"content":"你"}}]}  ... 最后 data: [DONE]
    Anthropic 风格：event: content_block_delta / data: {"type":"content_block_delta",...}

边界条件：JSON 解析失败的行直接忽略（有些上游会插心跳注释行）、跨 chunk 被切断的行会
        缓存到下一个 chunk 再拼、空字符串内容不算有效内容喵。
"""

# 引入注解特性喵
from __future__ import annotations

# json 用来解析 SSE 的 data 负载喵
import json
# Any 用于标注解析出来的任意 JSON 结构喵
from typing import Any

# 探测结论：还没看出结果，需要继续读更多数据喵
VERDICT_PENDING = "pending"
# 探测结论：确认这条流健康，可以放行给客户端喵
VERDICT_CONTENT = "content"
# 探测结论：流已经结束了，但从头到尾没有任何有效内容，属于假成功喵
VERDICT_DONE_EMPTY = "done_empty"
# 探测结论：流里明确带了 error 事件喵
VERDICT_ERROR = "error"

# 单条 data 负载的种类：看不出什么，继续读喵
KIND_PENDING = "pending"
# 单条 data 负载的种类：带文字内容，需要把字数累加进门槛计数喵
KIND_TEXT = "text"
# 单条 data 负载的种类：结构上已经证明这条流健康，不带文字也立即放行喵。
# 典型是工具调用增量（content 为空但模型确实在干活）和 finish_reason（这一轮正常收尾）喵
KIND_SUFFICIENT = "sufficient"
# 单条 data 负载的种类：流正常结束（[DONE] 或 message_stop）喵
KIND_DONE = "done"
# 单条 data 负载的种类：流里明确报错喵
KIND_ERROR = "error"


def _inspect_openai(obj: dict[str, Any]) -> tuple[str, str]:
    """
    看一个 OpenAI 风格的 chunk 里有什么喵~

    输出：(种类, 文字)
        (KIND_TEXT, "你好")   带文字内容，文字用于累加字数
        (KIND_SUFFICIENT, "") 不带文字但结构上已证明健康（工具调用 / finish_reason）
        (KIND_PENDING, "")    没有有效内容（比如只有 role 的占位首包）

    为什么 tool_calls 和 finish_reason 要单独归成 SUFFICIENT：
        模型调工具时 content 一直是空的，字数永远凑不满 10 个；finish_reason 出现时
        这一轮已经收尾了。这两种情况如果按字数门槛判，会被误判成「卡流」，
        所以必须让它们绕过字数门槛直接放行喵。
    """
    # 取出 choices 数组，不是列表就说明不是标准 OpenAI chunk 喵
    choices = obj.get("choices")
    # 喵~防御：choices 缺失或不是列表时判为没内容，不抛异常喵
    if not isinstance(choices, list):
        return KIND_PENDING, ""
    # 把所有 choice 里的文字拼起来，流式并行采样时可能有多个 choice 喵
    collected = ""
    # 逐个检查每个 choice 喵
    for choice in choices:
        # 喵~防御：数组元素可能不是字典，跳过它喵
        if not isinstance(choice, dict):
            continue
        # finish_reason 非空说明这一轮已正常收尾，直接放行喵
        if choice.get("finish_reason"):
            return KIND_SUFFICIENT, ""
        # 取出增量对象，流式响应的内容都在 delta 里喵
        delta = choice.get("delta")
        # 有些上游在流式里错误地用了非流式的 message 字段，一并兼容喵
        if not isinstance(delta, dict):
            delta = choice.get("message")
        # 喵~防御：两个字段都不是字典就跳过这个 choice 喵
        if not isinstance(delta, dict):
            continue
        # 模型在调用工具时 content 为空但流是健康的，直接放行喵
        if delta.get("tool_calls"):
            return KIND_SUFFICIENT, ""
        # 累加普通文字内容和推理内容（推理模型会先吐思维链）喵
        for key in ("content", "reasoning_content"):
            # 取出字段值喵
            value = delta.get(key)
            # 只累加非空字符串，空串是很多上游的占位首包，不算内容喵
            if isinstance(value, str) and value != "":
                collected += value
    # 收集到文字就报 TEXT，否则报 PENDING 喵
    return (KIND_TEXT, collected) if collected else (KIND_PENDING, "")


def _inspect_anthropic(obj: dict[str, Any]) -> tuple[str, str]:
    """
    看一个 Anthropic 风格的事件里有什么喵~

    Anthropic 的流是按事件类型区分的，真正带内容的是 content_block_delta，
    它的 delta 里可能是 text（普通文字）、thinking（思维链）或 partial_json（工具参数）喵。

    输出：和 _inspect_openai 一样是 (种类, 文字)
    """
    # 只有 content_block_delta 事件才携带内容，其他事件（message_start 等）都是元信息喵
    if obj.get("type") != "content_block_delta":
        return KIND_PENDING, ""
    # 取出增量对象喵
    delta = obj.get("delta")
    # 喵~防御：delta 不是字典就判为没内容喵
    if not isinstance(delta, dict):
        return KIND_PENDING, ""
    # 工具参数增量不带可读文字，但说明模型在正常干活，直接放行喵
    partial = delta.get("partial_json")
    # 非空字符串才算喵
    if isinstance(partial, str) and partial != "":
        return KIND_SUFFICIENT, ""
    # 累加普通文字和思维链文字喵
    collected = ""
    # 依次检查两种承载可读文字的字段喵
    for key in ("text", "thinking"):
        # 取出字段值喵
        value = delta.get(key)
        # 只累加非空字符串喵
        if isinstance(value, str) and value != "":
            collected += value
    # 收集到文字就报 TEXT，否则报 PENDING 喵
    return (KIND_TEXT, collected) if collected else (KIND_PENDING, "")


def classify_payload(payload: str) -> tuple[str, str]:
    """
    对一条 SSE 的 data: 负载做分类喵~

    输入：data: 后面那一段原始文本（已去掉前缀和首尾空白）
    输出：(种类, 附带信息)
        种类取 KIND_* 五个常量之一；附带信息在 KIND_TEXT 时是文字本身（用来累加字数），
        在 KIND_DONE / KIND_ERROR 时是给人看的说明文本，其余情况是空串。
    边界条件：非 JSON 的负载（心跳、注释、上游乱塞的东西）一律返回 KIND_PENDING，
            也就是「看不出问题，继续读」，绝不因为解析不了就误判成失败喵。
    """
    # 喵~防御：空负载是 SSE 的合法心跳，继续等就好喵
    if not payload:
        return KIND_PENDING, ""
    # OpenAI 协议用 [DONE] 表示流正常结束喵
    if payload == "[DONE]":
        # 这里只报「流结束了」，至于结束时算健康还是空流，由探测器结合累计字数来判喵
        return KIND_DONE, "上游返回了 [DONE]"
    # 尝试把负载解析成 JSON 喵
    try:
        obj = json.loads(payload)
    # 喵~防御：解析失败不代表流坏了（可能是心跳或上游自定义行），当作 pending 继续读喵
    except (json.JSONDecodeError, ValueError):
        return KIND_PENDING, ""
    # 喵~防御：JSON 顶层不是字典（比如是数组或裸数字）时看不出问题，继续读喵
    if not isinstance(obj, dict):
        return KIND_PENDING, ""
    # 检查有没有 error 字段，两种协议出错时都会带这个字段喵
    error = obj.get("error")
    # error 存在就说明上游在流里明确报错了喵
    if error:
        # 错误可能是字典（标准格式）也可能是字符串（简化格式），分别提取消息喵
        message = error.get("message") if isinstance(error, dict) else str(error)
        # 返回错误种类，消息为空时用兜底文案喵
        return KIND_ERROR, str(message or "上游在流中返回了 error")
    # Anthropic 的错误事件用 type=error 表示，单独判一下喵
    if obj.get("type") == "error":
        # 从整个负载里截一段当说明喵
        return KIND_ERROR, f"上游流中返回 error 事件：{payload[:200]}"
    # Anthropic 用 message_stop 表示流结束喵
    if obj.get("type") == "message_stop":
        # 同样只报「结束了」，健康与否交给探测器判喵
        return KIND_DONE, "上游返回了 message_stop"
    # 看 OpenAI 风格的内容喵
    kind, text = _inspect_openai(obj)
    # 有结果就直接返回喵
    if kind != KIND_PENDING:
        return kind, text
    # 再看 Anthropic 风格的内容喵
    return _inspect_anthropic(obj)


class StreamProbe:
    """
    SSE 流的增量探测器喵~

    用法：把上游读到的每个原始字节块喂给 feed()，它会告诉你当前结论；读完整条流后
    调用 finish() 拿最终结论。探测器本身不改动也不持有原始字节，缓冲原始字节是
    调用方（upstream 模块）的事，这样职责清晰、也方便单独测试喵。

    放行条件（满足任一即判为健康）：
        1. 累积内容字符数 >= min_content_chars（默认 10）
        2. 收到流结束标记，且此前已经吐过至少 1 个字符（正常的短回答）
        3. 收到结构上已证明健康的事件（工具调用增量、finish_reason）

    为什么需要「增量」：
        上游一次 TCP 读取给到的字节块，边界跟 SSE 的行边界毫无关系。一行 data: 可能被
        切成两半，一个中文字的 3 个字节也可能被切开。所以内部维护两级缓冲：
            1. 增量 UTF-8 解码器：处理被切断的多字节字符，绝不会解出乱码喵
            2. 文本行缓冲：处理被切断的行，不完整的尾行留到下一块再拼喵
    """

    def __init__(self, min_content_chars: int = 10) -> None:
        """
        创建一个全新的探测器喵~

        输入：min_content_chars 是放行前需要累积的最少内容字符数。
             设成 1 就退回「有一个字符就放行」的老行为喵。
        """
        # 引入 codecs 拿增量解码器，放在方法内导入是为了让模块顶部保持清爽喵
        import codecs
        # 增量 UTF-8 解码器，能记住上一块末尾没解完的字节喵
        self._decoder = codecs.getincrementaldecoder("utf-8")(errors="replace")
        # 还没拼成完整一行的文本尾巴喵
        self._pending_line = ""
        # 最终结论，一旦确定就不再变化喵
        self._verdict = VERDICT_PENDING
        # 结论的说明文本，失败时作为错误原因往上传喵
        self._detail = ""
        # 放行门槛，至少为 1，防止传 0 进来导致门槛失效喵
        self._min_chars = max(1, min_content_chars)
        # 到目前为止累积的内容字符数喵
        self._content_chars = 0

    @property
    def verdict(self) -> str:
        """当前结论喵~"""
        # 直接返回内部记录的结论喵
        return self._verdict

    @property
    def detail(self) -> str:
        """当前结论的说明文本喵~"""
        # 直接返回内部记录的说明喵
        return self._detail

    @property
    def content_chars(self) -> int:
        """到目前为止累积了多少个内容字符，写日志和判卡流原因时要用喵~"""
        # 直接返回计数喵
        return self._content_chars

    def feed(self, chunk: bytes) -> str:
        """
        喂进一个原始字节块，返回当前结论喵~

        输入：从上游 socket 读到的原始字节块
        输出：VERDICT_* 之一，返回 pending 表示还看不出结果、请继续读
        """
        # 结论已经定了就不再做任何解析，省 CPU 也保证结论稳定喵
        if self._verdict != VERDICT_PENDING:
            return self._verdict
        # 喵~防御：空字节块直接返回当前结论，不做无意义的解码喵
        if not chunk:
            return self._verdict
        # 用增量解码器把字节转成文本，被切断的多字节字符会自动留到下次喵
        text = self._decoder.decode(chunk)
        # 把上次剩下的行尾巴拼到前面，组成完整的待处理文本喵
        text = self._pending_line + text
        # 按换行切分，SSE 用 \n 分行；先统一把 \r\n 换成 \n 以兼容个别上游喵
        lines = text.replace("\r\n", "\n").split("\n")
        # 最后一段可能是不完整的行（没等到换行符），留到下一块再处理喵
        self._pending_line = lines.pop()
        # 逐行检查喵
        for line in lines:
            # 处理这一行，一旦得出结论就立即返回，不必读完剩下的行喵
            if self._consume_line(line) != VERDICT_PENDING:
                return self._verdict
        # 所有完整行都处理完了还没结论，继续等下一块喵
        return self._verdict

    def _consume_line(self, line: str) -> str:
        """处理一整行文本，更新并返回结论喵~"""
        # 去掉首尾空白，SSE 的 data: 后面通常有个空格喵
        stripped = line.strip()
        # 判断这一行是否带标准 SSE data: 前缀喵
        has_data_prefix = stripped.startswith("data:")
        # 兼容真实中转站把 JSON error 裸放在 SSE 流里的异常格式喵
        is_bare_json = stripped.startswith("{") and stripped.endswith("}")
        # 标准 SSE 只关心 data: 行，其他框架行通常直接跳过喵
        if not has_data_prefix and not is_bare_json:
            return self._verdict
        # 去掉 data: 前缀并再次去空白，得到真正的负载喵
        payload = stripped[5:].strip() if has_data_prefix else stripped
        # 交给分类函数看这条负载是什么种类喵
        kind, info = classify_payload(payload)
        # 裸 JSON 只有明确表示错误时才允许影响探测结论，避免误把普通框架数据当内容喵
        if not has_data_prefix and kind != KIND_ERROR:
            return self._verdict
        # 带文字的负载：把字数累加进计数，够门槛才放行喵
        if kind == KIND_TEXT:
            # 累加这条负载带来的字符数喵
            self._content_chars += len(info)
            # 还没够门槛就继续读，这正是防「吐一两个字然后卡死」的关键喵
            if self._content_chars < self._min_chars:
                return self._verdict
            # 够门槛了，判定这条流健康喵
            self._verdict = VERDICT_CONTENT
            # 返回新结论喵
            return self._verdict
        # 结构上已证明健康（工具调用、finish_reason）：绕过字数门槛直接放行喵
        if kind == KIND_SUFFICIENT:
            # 判定健康喵
            self._verdict = VERDICT_CONTENT
            # 返回新结论喵
            return self._verdict
        # 流结束了：结合累计字数判断是正常短回答还是空流喵
        if kind == KIND_DONE:
            # 吐过至少一个字符就是正常的短回答，照常放行，绝不让客户端干等门槛喵
            if self._content_chars > 0:
                # 判定健康喵
                self._verdict = VERDICT_CONTENT
            # 一个字符都没有，那就是假成功喵
            else:
                # 判定为空流喵
                self._verdict = VERDICT_DONE_EMPTY
                # 记录说明，供上层写日志和做规则匹配喵
                self._detail = f"{info} 但整条流没有任何内容"
            # 返回新结论喵
            return self._verdict
        # 流里明确报错喵
        if kind == KIND_ERROR:
            # 判定为错误喵
            self._verdict = VERDICT_ERROR
            # 记录上游给的错误消息喵
            self._detail = info
            # 返回新结论喵
            return self._verdict
        # KIND_PENDING：这行看不出问题，保持原状继续读喵
        return self._verdict

    def finish(self) -> str:
        """
        上游的流已经读完时调用，给出最终结论喵~

        边界条件：
            吐过字符但没凑够门槛，然后流就结束了 —— 判为健康并放行。因为流都结束了，
            这几个字符就是全部内容，再等门槛只会白等；而且有些上游本来就不发 [DONE]，
            这时候判失败会误伤它们喵。
            一个字符都没有 —— 判为空流，这是真正的假成功喵。
        """
        # 已经有明确结论就直接返回它喵
        if self._verdict != VERDICT_PENDING:
            return self._verdict
        # 处理最后那段没有换行符结尾的残行，有些上游最后一行不带换行喵
        if self._pending_line.strip():
            # 走一遍行处理逻辑，可能从中得出结论喵
            self._consume_line(self._pending_line)
            # 清空残行，防止重复处理喵
            self._pending_line = ""
        # 再检查一次，残行里可能已经给出结论了喵
        if self._verdict != VERDICT_PENDING:
            return self._verdict
        # 流结束了且吐过内容（只是没凑够门槛），当成正常的短回答放行喵
        if self._content_chars > 0:
            # 判定健康喵
            self._verdict = VERDICT_CONTENT
            # 返回最终结论喵
            return self._verdict
        # 到这里说明流结束了但一个有效内容都没有，判定为空流喵
        self._verdict = VERDICT_DONE_EMPTY
        # 记录说明，指出是连接提前结束喵
        self._detail = "上游的流已结束，但整条流没有任何有效内容"
        # 返回最终结论喵
        return self._verdict
