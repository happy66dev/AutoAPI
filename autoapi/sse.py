"""
SSE 流内容探测模块喵~

这是整个项目最关键的一块，所以先把思路讲清楚喵：

要解决的问题：
    很多上游（尤其是各种中转站）会先回一个漂亮的 200 OK 并且开始吐 SSE 流，然后在流里
    塞一个 error 事件，或者一个字都不吐就直接 [DONE] 收尾。如果代理拿到 200 就立刻把
    字节转给客户端，那这种「假成功」就无法故障转移了 —— 因为字节已经出门，收不回来喵。

采取的策略：
    代理先自己读上游的流，把原始字节暂存在缓冲区里，同时同步解析里面的 data: 负载，
    直到确认「至少吐出了一个有效内容字符」，才把缓冲区连同后续字节一起放给客户端。
    在放行之前发现 error 或空流，就默默换下一个候选，客户端完全感知不到喵。

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
# 探测结论：确认吐出了有效内容，这条流是健康的，可以放行给客户端喵
VERDICT_CONTENT = "content"
# 探测结论：流已经结束了，但从头到尾没有任何有效内容，属于假成功喵
VERDICT_DONE_EMPTY = "done_empty"
# 探测结论：流里明确带了 error 事件喵
VERDICT_ERROR = "error"


def _has_openai_content(obj: dict[str, Any]) -> bool:
    """
    判断一个 OpenAI 风格的 chunk 里有没有有效内容喵~

    有效内容的定义比「有文字」宽一些，因为下面这几种都说明模型确实在正常工作：
        delta.content            普通回答文字
        delta.reasoning_content  推理模型的思维链（DeepSeek-R1 等会先吐这个）
        delta.tool_calls         模型在调用工具，此时 content 是空的但流是健康的
        finish_reason            这一轮正常收尾了，也算模型给出了完整答复
    """
    # 取出 choices 数组，不是列表就说明不是标准 OpenAI chunk 喵
    choices = obj.get("choices")
    # 喵~防御：choices 缺失或不是列表时直接判定为无内容，不抛异常喵
    if not isinstance(choices, list):
        return False
    # 逐个检查每个 choice，通常只有一个但流式并行采样时会有多个喵
    for choice in choices:
        # 喵~防御：数组元素可能不是字典，跳过它喵
        if not isinstance(choice, dict):
            continue
        # finish_reason 非空说明这一轮已正常结束，算有效喵
        if choice.get("finish_reason"):
            return True
        # 取出增量对象，流式响应的内容都在 delta 里喵
        delta = choice.get("delta")
        # 有些上游在流式里错误地用了非流式的 message 字段，一并兼容喵
        if not isinstance(delta, dict):
            delta = choice.get("message")
        # 喵~防御：两个字段都不是字典就跳过这个 choice 喵
        if not isinstance(delta, dict):
            continue
        # 检查普通文字内容和推理内容两个字段喵
        for key in ("content", "reasoning_content"):
            # 取出字段值喵
            value = delta.get(key)
            # 必须是非空字符串才算有效，空串是很多上游的占位首包，不能算喵
            if isinstance(value, str) and value != "":
                return True
        # 模型在调用工具时 content 为空但流是健康的，所以 tool_calls 也算有效喵
        if delta.get("tool_calls"):
            return True
    # 所有 choice 都没有有效内容喵
    return False


def _has_anthropic_content(obj: dict[str, Any]) -> bool:
    """
    判断一个 Anthropic 风格的事件里有没有有效内容喵~

    Anthropic 的流是按事件类型区分的，真正带内容的是 content_block_delta，
    它的 delta 里可能是 text（普通文字）、thinking（思维链）或 partial_json（工具参数）喵。
    """
    # 只有 content_block_delta 事件才携带内容，其他事件（message_start 等）都是元信息喵
    if obj.get("type") != "content_block_delta":
        return False
    # 取出增量对象喵
    delta = obj.get("delta")
    # 喵~防御：delta 不是字典就判定无内容喵
    if not isinstance(delta, dict):
        return False
    # 依次检查三种可能承载内容的字段喵
    for key in ("text", "thinking", "partial_json"):
        # 取出字段值喵
        value = delta.get(key)
        # 非空字符串才算有效内容喵
        if isinstance(value, str) and value != "":
            return True
    # 三个字段都没有有效内容喵
    return False


def classify_payload(payload: str) -> tuple[str, str]:
    """
    对一条 SSE 的 data: 负载做分类喵~

    输入：data: 后面那一段原始文本（已去掉前缀和首尾空白）
    输出：(结论, 说明文本)，结论取 VERDICT_* 四个常量之一
    边界条件：非 JSON 的负载（心跳、注释、上游乱塞的东西）一律返回 pending，
            也就是「看不出问题，继续读」，绝不因为解析不了就误判成失败喵。
    """
    # 喵~防御：空负载是 SSE 的合法心跳，继续等就好喵
    if not payload:
        return VERDICT_PENDING, ""
    # OpenAI 协议用 [DONE] 表示流正常结束喵
    if payload == "[DONE]":
        # 走到这里说明在此之前没探测到任何内容，所以是「结束了但是空的」喵
        return VERDICT_DONE_EMPTY, "上游返回了 [DONE] 但整条流没有任何内容"
    # 尝试把负载解析成 JSON 喵
    try:
        obj = json.loads(payload)
    # 喵~防御：解析失败不代表流坏了（可能是心跳或上游自定义行），当作 pending 继续读喵
    except (json.JSONDecodeError, ValueError):
        return VERDICT_PENDING, ""
    # 喵~防御：JSON 顶层不是字典（比如是数组或裸数字）时看不出问题，继续读喵
    if not isinstance(obj, dict):
        return VERDICT_PENDING, ""
    # 检查有没有 error 字段，两种协议出错时都会带这个字段喵
    error = obj.get("error")
    # error 存在就说明上游在流里明确报错了喵
    if error:
        # 错误可能是字典（标准格式）也可能是字符串（简化格式），分别提取消息喵
        message = error.get("message") if isinstance(error, dict) else str(error)
        # 返回错误结论，消息为空时用兜底文案喵
        return VERDICT_ERROR, str(message or "上游在流中返回了 error")
    # Anthropic 的错误事件用 type=error 表示，单独判一下喵
    if obj.get("type") == "error":
        # 从 error 子对象里取消息，取不到就用整个负载当说明喵
        return VERDICT_ERROR, f"上游流中返回 error 事件：{payload[:200]}"
    # 检查 OpenAI 风格的内容喵
    if _has_openai_content(obj):
        return VERDICT_CONTENT, ""
    # 检查 Anthropic 风格的内容喵
    if _has_anthropic_content(obj):
        return VERDICT_CONTENT, ""
    # Anthropic 用 message_stop 表示流结束，此前没内容就是假成功喵
    if obj.get("type") == "message_stop":
        return VERDICT_DONE_EMPTY, "上游返回了 message_stop 但整条流没有任何内容"
    # 是合法 JSON 但既没内容也没结束（message_start、ping 之类的元信息事件），继续读喵
    return VERDICT_PENDING, ""


class StreamProbe:
    """
    SSE 流的增量探测器喵~

    用法：把上游读到的每个原始字节块喂给 feed()，它会告诉你当前结论；读完整条流后
    调用 finish() 拿最终结论。探测器本身不改动也不持有原始字节，缓冲原始字节是
    调用方（upstream 模块）的事，这样职责清晰、也方便单独测试喵。

    为什么需要「增量」：
        上游一次 TCP 读取给到的字节块，边界跟 SSE 的行边界毫无关系。一行 data: 可能被
        切成两半，一个中文字的 3 个字节也可能被切开。所以内部维护两级缓冲：
            1. 增量 UTF-8 解码器：处理被切断的多字节字符，绝不会解出乱码喵
            2. 文本行缓冲：处理被切断的行，不完整的尾行留到下一块再拼喵
    """

    def __init__(self) -> None:
        """创建一个全新的探测器喵~"""
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
        # 只关心 data: 开头的行，event: / id: / retry: / 空行都是框架信息，跳过喵
        if not stripped.startswith("data:"):
            return self._verdict
        # 去掉 data: 前缀并再次去空白，得到真正的负载喵
        payload = stripped[5:].strip()
        # 交给分类函数判断这条负载意味着什么喵
        verdict, detail = classify_payload(payload)
        # pending 表示这行看不出问题，保持原状继续读喵
        if verdict == VERDICT_PENDING:
            return self._verdict
        # 得出了明确结论，记录下来，之后 feed 不再解析喵
        self._verdict = verdict
        # 记录说明文本，供上层写日志和做规则匹配喵
        self._detail = detail
        # 返回新结论喵
        return self._verdict

    def finish(self) -> str:
        """
        上游的流已经读完时调用，给出最终结论喵~

        边界条件：如果读到流末尾都没探测到内容，也没看到 [DONE] 或 error，
                说明上游把连接直接断了。这同样是「假成功」，判定为空流喵。
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
        # 到这里说明流结束了但一个有效内容都没有，判定为空流喵
        self._verdict = VERDICT_DONE_EMPTY
        # 记录说明，指出是连接提前结束喵
        self._detail = "上游的流已结束，但整条流没有任何有效内容"
        # 返回最终结论喵
        return self._verdict
