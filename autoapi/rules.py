"""
规则引擎模块喵~

整体思路：
    上游返回了一个「不正常」的结果之后，代理需要决定接下来干什么。判断依据有两样：
    HTTP 状态码，以及响应体文本（有些上游一律返回 200 或 429，真正的原因只写在 body 里）。
    规则列表自上而下逐条比对，第一条命中的就是最终决策，跟防火墙规则的语义一样喵。

输入：规则列表、本次尝试的状态码、响应体文本
输出：Decision 对象，说明该 retry / next / freeze / passthrough，以及冻结多久
边界条件：没有任何规则命中时，采取最保守的默认动作 next（换下一个候选），
        因为「换一个试试」既不会放大对上游的压力，也不会把错误直接甩给客户端喵。
"""

# 引入注解特性喵
from __future__ import annotations

# dataclass 用来定义决策结果的小容器喵
from dataclasses import dataclass
# re 用来在响应体里搜正则、以及解析 Retry-After 头喵
import re

# 引入规则类型定义喵
from .config import Rule

# 没有任何规则命中时的默认动作：换下一个候选，最保守也最有用喵
DEFAULT_ACTION = "next"

# 时间单位 → 换算成秒的倍数，用于把捕获到的数字转成秒喵
UNIT_TO_SECONDS = {
    # 秒本身不用换算喵
    "seconds": 1.0,
    # 单数写法也认，容忍用户写 second 喵
    "second": 1.0,
    # 简写 s 也认喵
    "s": 1.0,
    # 分钟乘 60 喵
    "minutes": 60.0,
    # 单数写法喵
    "minute": 60.0,
    # 简写 m 喵
    "m": 60.0,
    # 小时乘 3600 喵
    "hours": 3600.0,
    # 单数写法喵
    "hour": 3600.0,
    # 简写 h 喵
    "h": 3600.0,
}


@dataclass
class Decision:
    """规则引擎给出的决策结果喵~"""

    # 决定采取的动作：retry / next / freeze / passthrough 喵
    action: str
    # action=retry 时，同一候选上总共允许尝试几次（含第一次）喵
    max_attempts: int = 1
    # action=retry 时的退避基数喵
    backoff_base: float = 1.5
    # action=freeze 时的冻结秒数喵
    freeze_seconds: float = 0.0
    # 命中的规则描述，写日志时说明「为什么这么决定」喵
    matched_by: str = "默认规则"


def _rule_matches(rule: Rule, status: int, body_text: str) -> re.Match[str] | None:
    """
    判断单条规则是否命中喵~

    返回值：命中且规则配了正则时返回正则的 Match 对象（后面要从捕获组里取冻结时长）；
           命中但规则没配正则时返回一个哨兵 Match（用空正则匹配空串得到）；
           不命中时返回 None 喵。
    """
    # 规则配了状态码集合时，状态码必须落在集合里，否则不命中喵
    if rule.status_codes and status not in rule.status_codes:
        return None
    # 规则没配正则，那么走到这里就说明只看状态码且已经匹配上了喵
    if rule.body_regex is None:
        # 用一个必然成功的空模式匹配，返回一个非 None 的 Match 当作「命中」信号喵
        return re.compile("").match("")
    # 喵~防御：body_text 可能是空字符串（比如上游返回空体），空串搜正则不会报错只会不命中喵
    return rule.body_regex.search(body_text or "")


def _resolve_freeze_seconds(rule: Rule, match: re.Match[str], retry_after: str | None) -> float:
    """
    算出这次该冻结多少秒喵~

    优先级从高到低：
        1. 规则配了 freeze_from_group，就从正则捕获组里取数字（上游明确说了几分钟恢复，最精准）
        2. 上游返回了 Retry-After 头，就按它说的秒数（HTTP 标准做法）
        3. 都没有，就用规则里的兜底 freeze_seconds

    边界条件：捕获组不存在、捕获到的不是数字、数字为 0 或负数，全部退回下一优先级喵。
    """
    # 规则要求从捕获组取时长时，先试这条路喵
    if rule.freeze_from_group is not None:
        # 用 try 包住，因为捕获组序号越界会抛 IndexError 喵
        try:
            # 按序号取出捕获到的文本喵
            captured = match.group(rule.freeze_from_group)
            # 喵~防御：捕获组可能匹配到空值（正则里是可选组），此时跳过不用它喵
            if captured:
                # 把捕获到的数字转成浮点数喵
                amount = float(captured)
                # 按规则声明的单位换算成秒，单位不认识时保守地按分钟算喵
                seconds = amount * UNIT_TO_SECONDS.get(rule.freeze_unit, 60.0)
                # 喵~防御：只有算出正数才采用，0 或负数说明上游文案异常，退回下一优先级喵
                if seconds > 0:
                    # 上游报的恢复时间通常是向下取整的，多加 5 秒缓冲避免刚解冻就又撞墙喵
                    return seconds + 5.0
        # 喵~防御：捕获组越界或内容不是数字时不让它炸掉整个请求，静默退回下一优先级喵
        except (IndexError, ValueError):
            pass
    # 其次看 Retry-After 响应头喵
    if retry_after:
        # 用 try 包住，因为 Retry-After 也可能是 HTTP 日期格式而不是秒数喵
        try:
            # 按秒数解析喵
            seconds = float(str(retry_after).strip())
            # 喵~防御：只接受正数喵
            if seconds > 0:
                # 同样加 5 秒缓冲喵
                return seconds + 5.0
        # 喵~防御：解析失败（比如是日期格式）就直接退回兜底值，不做复杂的日期解析喵
        except ValueError:
            pass
    # 前两条都没拿到，用规则里写死的兜底秒数喵
    return rule.freeze_seconds


def decide(
    rules: list[Rule],
    status: int,
    body_text: str,
    retry_after: str | None = None,
) -> Decision:
    """
    对一次失败的尝试做出决策喵~

    输入：
        rules       当前生效的规则列表，顺序即优先级
        status      本次尝试的状态码（可能是 -1 network 或 -2 bad_stream 这两个特殊值）
        body_text   上游响应体文本，用于正则匹配
        retry_after 上游的 Retry-After 响应头原文，可能为 None
    输出：Decision 对象
    """
    # 自上而下逐条比对，第一条命中即返回，跟防火墙规则一个语义喵
    for rule in rules:
        # 试着匹配这条规则喵
        match = _rule_matches(rule, status, body_text)
        # 没命中就看下一条喵
        if match is None:
            continue
        # 命中了 freeze 动作，需要额外算出冻结时长喵
        if rule.action == "freeze":
            # 组装带冻结时长的决策喵
            return Decision(
                # 动作是冻结喵
                action="freeze",
                # 算出的冻结秒数喵
                freeze_seconds=_resolve_freeze_seconds(rule, match, retry_after),
                # 记下是哪条规则做的决定喵
                matched_by=rule.describe(),
            )
        # 命中了 retry 动作，需要带上重试次数和退避基数喵
        if rule.action == "retry":
            # 组装重试决策喵
            return Decision(
                # 动作是重试喵
                action="retry",
                # 总尝试次数喵
                max_attempts=rule.max_attempts,
                # 退避基数喵
                backoff_base=rule.backoff_base,
                # 命中的规则描述喵
                matched_by=rule.describe(),
            )
        # 其余动作（next / passthrough）不需要额外参数，直接返回喵
        return Decision(action=rule.action, matched_by=rule.describe())
    # 一条都没命中，采取最保守的默认动作：换下一个候选喵
    return Decision(action=DEFAULT_ACTION, matched_by="默认规则（无规则命中，换下一个候选）")
