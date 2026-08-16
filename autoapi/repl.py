"""
交互式命令行模块喵~

职责：让主人在代理跑着的同时，随时看状态、改规则、清冻结，改完立即生效，不用重启喵。

线程模型：
    REPL 跑在一个独立的 daemon 线程里，因为 input() 是阻塞调用，绝对不能放进事件循环
    （那会把整个代理卡死）。它和事件循环共享同一个 RuntimeState，靠 RuntimeState 内部
    的 threading.Lock 保证线程安全。因为临界区里全是纯内存字典操作、不含 await，
    所以两边都能直接调用同一批方法，不必绕 run_coroutine_threadsafe 投递协程喵。

改规则的流程（这一段是本模块最关键的设计）：
    1. 从磁盘重新读一份原始 YAML 字典（不是用内存里的对象，这样能自动吸收主人在编辑器
       里的手改，不会把手改覆盖掉）
    2. 在这份字典上做改动
    3. 送进 parse_config 完整校验
    4. 只有校验通过才替换内存里的配置，并写回磁盘
    校验不通过就打印错误、什么都不改，跑着的代理继续用旧配置服务，绝不会被一条写坏的
    规则搞停摆喵。
"""

# 引入注解特性喵
from __future__ import annotations

# copy 用来深拷贝配置字典，避免改动影响到原件喵
import copy
# datetime 用来格式化最近错误发生时间喵
from datetime import datetime
# os 用来读取 NO_COLOR 环境变量喵
import os
# shutil 用来读取终端窗口宽度喵
import shutil
# json 用来解析 rule add 命令的参数、以及打印规则内容喵
import json
# threading 用来跑独立线程和退出信号喵
import threading
# Any 用于标注 YAML 解析出来的任意结构喵
from typing import Any

# PyYAML 用来读写配置文件喵
import yaml

# 引入配置校验相关喵
from .config import (
    CANDIDATE_TIMEOUT_FIELDS,
    RETIRED_SERVER_KEYS,
    ConfigError,
    parse_config,
)
# 引入运行时状态喵
from .state import HEALTH_BUCKET_SECONDS, HealthSnapshot, HealthWindow, RuntimeState

# help 命令要打印的帮助文本喵
HELP_TEXT = """
可用命令喵~

  查看类：
    vm                      列出所有虚拟模型及其候选链，标注哪个在冻结中喵
    rule ls                 列出所有规则和它们的序号喵
    freeze ls               列出当前所有冻结中的候选和剩余时间喵
    stats                   打印每个候选的成功/失败/被冻结次数喵

  改规则类（下面所有改动都是改完立即生效并写回 config.yaml）：
    rule add <JSON>         追加一条规则到列表末尾喵
    rule rm <序号>          删掉指定序号的规则喵
    rule mv <原序号> <新序号> 挪动规则位置来调整匹配优先级喵

  改候选链类：
    cand add <虚拟模型> <JSON>        给候选链末尾追加一个候选喵
    cand rm <虚拟模型> <序号>         删掉指定序号的候选喵
    cand mv <虚拟模型> <原> <新>      挪动候选优先级，第 1 位最优先喵
    cand set <虚拟模型> <序号> <字段> <值>
                            改某个候选的单个字段，位置不变喵
                            字段可选：base_url、api_key、model、name、auth_style
                            以及三个「只对这个节点生效」的专属超时喵：
                              stall_timeout      这个节点允许上游静默多少秒
                              stream_timeout     这个节点的流式请求总预算（秒）
                              nonstream_timeout  这个节点的非流式请求总预算（秒）
                            专属超时填 default 就改回跟随全局值喵
    vm add <虚拟模型名> <候选JSON>    新建虚拟模型并配上第一个候选喵
    vm rm <虚拟模型名>                删掉一整个虚拟模型喵

  改服务器配置类：
    set <字段> <值>         能改这些喵（port 要重启才生效，其余立即生效）：
                              stall_timeout          允许上游静默多少秒，超过算卡流
                              stream_timeout         流式请求总预算（秒），默认 300
                              nonstream_timeout      非流式请求总预算（秒），默认 600
                              min_content_chars      放行前要累积的内容字符数
                              auto_hedge_threshold   连续失败几次就自动避险，0=关闭
                              auto_hedge_minutes     自动避险冻结多少分钟
                              metrics_window_minutes 平均耗时统计窗口（分钟），默认 30
                              connect_timeout        连接握手超时（秒）
                              reload_poll_interval   配置热重载轮询间隔（秒），0=关闭
                              port                   监听端口

  冻结类：
    freeze add <虚拟模型> <模型名或序号> <秒数>
                            主动冻结某个节点，到期自动恢复喵
                            适合上游要维护、或想临时把流量挪走时用
    freeze rm <虚拟模型> <模型名或序号>
                            解冻某个指定节点，让它立刻可用喵
    freeze clear            立刻清空所有冻结，让所有候选马上可用喵

  目标模式（仅本次运行有效，重启后自动关闭）：
    target on               链路全失效后每 5 秒从链首重试，最长坚持 5 分钟喵
    target off              关闭目标模式，恢复链用尽即返回 502 的默认行为喵
    target status           查看目标模式是否已开启喵

  其他：
    reload                  从磁盘重新加载 config.yaml 喵
    save                    校验当前配置并重新格式化写回 config.yaml 喵
    help                    显示这份帮助喵
    quit                    退出整个代理进程喵

  JSON 参数的例子喵：
    rule add {"match": {"status": 429}, "action": "retry", "max_attempts": 3}
    rule add {"match": {"status": [500, 502]}, "action": "next"}
    cand add auto-strong {"name": "新中转", "base_url": "https://x.com", "api_key": "sk-xxx", "model": "gpt-4o"}
    vm add my-model {"base_url": "https://y.com", "api_key": "sk-yyy", "model": "gpt-4o-mini"}

  改单个字段的例子喵（不用把整个候选重新敲一遍，位置也不会变）：
    cand set auto-strong 2 api_key sk-new-key-here
    cand set auto-strong 2 base_url https://newrelay.com
    cand set auto-strong 2 model gpt-4o-2024-11-20

  给慢模型单独放宽超时的例子喵（链首是会先想很久的推理模型时特别有用）：
    cand set auto-strong 1 stream_timeout 900      这个节点的流式请求最多等 15 分钟
    cand set auto-strong 1 stall_timeout 180       这个节点允许静默 3 分钟才算卡流
    cand set auto-strong 1 stream_timeout default  改回跟随全局值

  候选的字段说明喵：
    必填：base_url、api_key、model（model 是发给上游的真实模型名）
    可选：name（显示用）、auth_style（bearer 或 x-api-key，默认 bearer）
    可选的专属超时：stall_timeout、stream_timeout、nonstream_timeout
                   不写就跟随 server 段的全局值，只有需要区别对待的节点才配喵

  三个超时的分工喵（搞清楚这个就不会配错）：
    stall_timeout      「上游还活着吗」。只要收到任何字节就重新计时，
                        所以上游发心跳、吐思维链期间都不会触发。触发了说明连接挂死了。
    stream_timeout     「等太久了吗」。从发请求算到确认流健康为止，中途不归零。
                        连接一直健康但正文迟迟不来时靠它兜住。放行之后就不再计时，
                        模型愿意写多久都不会被掐断喵。
    nonstream_timeout   非流式请求的总预算。非流式要等上游憋完整篇，天生该等更久。
""".strip()


def _ansi(text: str, code: str) -> str:
    """按终端能力给 stats 文字加 ANSI 颜色喵~"""
    # 喵~防御：NO_COLOR 或非 TTY 输出时返回纯文本，保证重定向内容可读喵
    if os.environ.get("NO_COLOR") or not getattr(__import__("sys").stdout, "isatty", lambda: False)():
        return text
    # 返回颜色码、正文和复位码组成的完整片段喵
    return f"\033[{code}m{text}\033[0m"


def _rate_color(rate: float | None) -> str:
    """按成功率返回 ANSI 颜色编号喵~"""
    # 无请求窗口没有成功率，灰色表示暂无数据喵
    if rate is None:
        return "90"
    # 85% 及以上显示绿色喵
    if rate >= 85:
        return "32"
    # 60% 到不足 85% 显示黄色喵
    if rate >= 60:
        return "33"
    # 30% 到不足 60% 显示橙色喵
    if rate >= 30:
        return "38;5;208"
    # 不足 30% 显示红色喵
    return "31"


def _format_count(value: int | float) -> str:
    """用三位分组法展示整数喵~"""
    # 喵~防御：非有限数值回退为 0，避免格式化异常污染 stats 喵
    try:
        return f"{max(0, int(value)):,}"
    except (TypeError, ValueError, OverflowError):
        return "0"


def _format_compact(value: int | float) -> str:
    """将 Token 数量压缩成 K/M/B 并保留合理精度喵~"""
    # 喵~防御：非法数值回退为 0，避免命令输出中断喵
    try:
        numeric_value = max(0.0, float(value))
    except (TypeError, ValueError, OverflowError):
        numeric_value = 0.0
    # 依次尝试十亿、百万和千单位喵
    for unit, divisor in (("B", 1_000_000_000), ("M", 1_000_000), ("K", 1_000)):
        # 大于等于单位时显示两位小数喵
        if numeric_value >= divisor:
            return f"{numeric_value / divisor:.2f}{unit}"
    # 小数不足一千时直接显示分组整数喵
    return _format_count(numeric_value)


def _format_duration(milliseconds: float | None) -> str:
    """同时展示秒和毫秒的平均耗时喵~"""
    # 没有完整请求时明确显示暂无数据喵
    if milliseconds is None:
        return "暂无数据"
    # 喵~防御：负数耗时按 0 处理，避免显示不可能的负时长喵
    safe_milliseconds = max(0.0, milliseconds)
    # 返回示例中的秒和毫秒双重展示喵
    return f"{safe_milliseconds / 1000:.2f}s({_format_count(round(safe_milliseconds))}ms)"


def _format_health_window(window: HealthWindow) -> tuple[str, str]:
    """把窗口快照格式化为成功率和吞吐统计文本喵~"""
    # 无请求时返回灰色暂无数据提示喵
    if window.total <= 0:
        return "暂无请求", "暂无请求"
    # 计算成功率百分比，分母始终是窗口总请求数喵
    rate = window.success / window.total * 100
    # 成功率保留两位小数并展示成功/总数喵
    rate_text = f"{rate:.2f}% {_format_count(window.success)}/{_format_count(window.total)}"
    # Token 和平均耗时按用户要求同时显示紧凑值与原始值喵
    token_text = f"{_format_compact(window.tokens)}({_format_count(window.tokens)})"
    duration_text = _format_duration(window.average_elapsed_ms)
    # 返回成功率文本以及尾部统计文本喵
    return rate_text, f"{token_text} 平均耗时: {duration_text}"


def _render_history(snapshot: HealthSnapshot, width: int) -> str:
    """把最近 24 小时的十分钟健康桶渲染成彩色历史条喵~"""
    # 喵~防御：终端宽度过小时仍保留至少一个历史格喵
    visible_width = max(1, width)
    # 只取最新的可容纳历史格，避免超出终端窗口喵
    buckets = snapshot.buckets[-visible_width:]
    # 每个历史格用一个半高块字符，颜色表达可用率喵
    fragments: list[str] = []
    # 逐个历史格生成彩色字符喵
    for bucket in buckets:
        # 冻结状态优先使用青色提醒喵
        if bucket.frozen:
            color_code = "36"
            block = "█"
        # 无请求历史格用灰色空槽喵
        elif bucket.total <= 0:
            color_code = "90"
            block = "·"
        else:
            # 按这格成功率选择同一套阈值颜色喵
            color_code = _rate_color(bucket.success / bucket.total * 100)
            # 根据可用率选择高度字符，颜色和高度共同表达健康程度喵
            block = "█" if bucket.success == bucket.total else ("▄" if bucket.success * 2 >= bucket.total else "▁")
        # 追加单格颜色文本喵
        fragments.append(_ansi(block, color_code))
    # 拼接历史条并返回喵
    return "".join(fragments)


def _format_cache_hit_rate(rate: float | None) -> str:
    """格式化平均缓存命中率，未上报时明确提示喵~"""
    # 没有输入 Token 或缓存字段时不伪造百分比喵
    if rate is None:
        return "未上报"
    # 喵~防御：命中率限制在 0% 到 100%，避免异常上游数据污染展示喵
    return f"{max(0.0, min(1.0, rate)) * 100:.2f}%"


def _format_error_time(timestamp: float | None) -> str:
    """格式化最近错误时间，无法取得时显示未知喵~"""
    # 没有时间戳时返回明确占位文本喵
    if timestamp is None:
        return "未知"
    # 喵~防御：时间格式化异常时不影响 stats 其他内容喵
    try:
        return datetime.fromtimestamp(timestamp).astimezone().strftime("%Y-%m-%d %H:%M:%S")
    except (OverflowError, OSError, ValueError):
        return "未知"


def format_countdown(seconds: float) -> str:
    """
    把剩余秒数格式化成「xx 分 xx 秒」喵~

    输入：剩余秒数
    输出：形如 "05 分 42 秒" 的文本；超过一小时会带上小时，写成 "01 时 05 分 42 秒"
    边界条件：负数或 0 一律当成 0 秒处理（理论上不会传进来，但防一手更稳）喵。
    """
    # 喵~防御：负数按 0 处理，避免出现「-1 分」这种怪东西喵
    total = max(0, int(seconds))
    # 算出分钟和秒喵
    minutes, secs = divmod(total, 60)
    # 超过一小时就多显示一个「时」，否则只显示分和秒喵
    if minutes >= 60:
        # 把分钟再拆成小时和分钟喵
        hours, minutes = divmod(minutes, 60)
        # 三段都补零到两位，对齐好看喵
        return f"{hours:02d} 时 {minutes:02d} 分 {secs:02d} 秒"
    # 常规情况：分和秒都补零到两位喵
    return f"{minutes:02d} 分 {secs:02d} 秒"


def render_freeze_banner(state: RuntimeState) -> list[tuple[str, str]]:
    """
    渲染冻结横幅，输出 prompt_toolkit 认的「样式片段」列表喵~

    输出格式是 [(样式字符串, 文本), ...]，prompt_toolkit 的 bottom_toolbar 直接吃这个。
    返回列表而不是纯字符串是为了能给不同部分上不同的颜色 —— 警告用黄色、倒计时用青色，
    一眼就能扫到重点喵。

    没有任何节点被冻结时显示一行「所有节点可用」，这样横幅位置固定、
    内容不会因为横幅忽然出现或消失而上下跳动喵。
    """
    # 取当前所有被冻结的节点喵
    rows = state.list_frozen_nodes()
    # 先取每个虚拟模型近 60 秒的动态 RPM/TPM，放在冻结状态上方喵
    rate_rows = state.snapshot_virtual_model_rates()
    # 目标模式开启时先显示醒目的运行期提示，避免主人忘记请求会尽量坚持配置的时长喵
    target_enabled = state.target_mode_enabled
    # 先渲染负载信息，RPM 统计成功请求，TPM 只来自上游 usage 喵
    fragments: list[tuple[str, str]] = []
    # 目标模式提示放在横幅最顶端，从配置读取实际值喵
    if target_enabled:
        config = state.config
        interval_sec = config.server.target_mode_round_interval_seconds
        max_wait_min = config.server.target_mode_max_wait_seconds / 60
        fragments.append((
            "class:warn",
            f"🎯 目标模式已开启：链路全失效时会每 {interval_sec:.0f} 秒重试，最长坚持 {max_wait_min:.0f} 分钟"
        ))
    # 按配置顺序逐个显示虚拟模型，避免横幅顺序每秒跳动喵
    for rate in rate_rows:
        # TPM 未完整上报时明确显示未知，不把未知伪装成 0 喵
        tpm_text = f"{rate.tpm}" if rate.tpm is not None else "未完整上报"
        # 平均耗时只看已完整结束的请求，没有完成请求时显示未知喵
        average_text = (
            f"{rate.average_elapsed_ms:.0f}ms"
            if rate.average_elapsed_ms is not None
            else "未完成"
        )
        # RPM=0 的模型不显示，节约状态栏空间喵
        if rate.rpm <= 0:
            continue
        # 每个有流量的虚拟模型一行，动态值会随配置窗口刷新喵
        if fragments:
            fragments.append(("", "\n"))
        # 平均耗时右侧追加缓存命中率，沿用速率窗口的完整上报语义喵
        cache_rate_text = _format_cache_hit_rate(rate.average_cache_hit_rate)
        fragments.extend([
            ("class:node", f"{rate.virtual_model}"),
            ("", f"  RPM={rate.rpm}  TPM={tpm_text}  平均耗时={average_text} 平均缓存命中率={cache_rate_text}"),
        ])
    # 一个都没有就显示「全部可用」那一行喵
    if not rows:
        # 用绿色表示一切正常喵
        fragments.append(("class:ok", "\n✓ 所有节点可用，没有节点被冻结喵~"))
        return fragments
    # 有冻结的话，先放警告标题喵
    fragments.extend([
        # 标题前换行，和上面的负载表隔开喵
        ("", "\n"),
        # 黄色加粗的警告标题，把数量也写上喵
        ("class:warn", f"⚠ 下列模型达到了配额限制或异常自动避险!（{len(rows)} 个）"),
    ])
    # 为了让倒计时对齐，先算出「虚拟模型/节点」这一段最长有多宽喵
    labels = [f"{vm}/{model}" for vm, model, _ in rows]
    # 取最长的宽度，用于后面补空格对齐喵
    width = max(len(label) for label in labels)
    # 逐行渲染每个被冻结的节点喵
    for label, (_, _, remaining) in zip(labels, rows):
        # 每行前面换行并缩进两格喵
        fragments.append(("", "\n  "))
        # 「虚拟模型/节点model」这一段，补空格到统一宽度好让倒计时对齐喵
        fragments.append(("class:node", label.ljust(width)))
        # 中间的连接词喵
        fragments.append(("", "  将在 "))
        # 倒计时用青色突出显示喵
        fragments.append(("class:countdown", format_countdown(remaining)))
        # 结尾喵
        fragments.append(("", " 后再次可用"))
    # 返回渲染好的片段列表喵
    return fragments


class Repl:
    """交互式命令行喵~"""

    def __init__(self, state: RuntimeState) -> None:
        """用运行时状态创建一个 REPL 喵~"""
        # 共享的运行时状态喵
        self.state = state
        # 收到 quit 命令后被置位，主线程据此退出进程喵
        self.should_exit = threading.Event()

    # ---------- 内部工具方法喵 ----------

    def _read_raw_yaml(self) -> dict[str, Any]:
        """
        从磁盘重新读一份原始 YAML 字典喵~

        为什么每次改规则都重读而不是缓存：这样能自动吸收主人在编辑器里的手改，
        不会拿一份过时的内存副本把手改覆盖掉喵。
        """
        # 取配置文件路径喵
        path = self.state.config.source_path
        # 喵~防御：路径为 None 说明配置是从内存字典构造的（单元测试场景），不支持改写喵
        if path is None:
            raise ConfigError("当前配置不是从文件加载的，无法改写喵")
        # 读文件文本，显式 utf-8 以正确处理中文注释喵
        text = path.read_text(encoding="utf-8")
        # 解析成 Python 结构喵
        data = yaml.safe_load(text)
        # 喵~防御：顶层必须是字典喵
        if not isinstance(data, dict):
            raise ConfigError("配置文件顶层不是字典，无法改写喵")
        # 返回深拷贝，后续改动不影响这次读到的原件喵
        return copy.deepcopy(data)

    def _apply(self, data: dict[str, Any]) -> None:
        """
        校验并应用一份改好的配置字典喵~

        关键顺序：先完整校验，只有通过才替换内存配置。校验失败会抛 ConfigError，
        由调用方捕获打印，跑着的代理继续用旧配置服务喵。
        """
        # 完整走一遍校验，顺便保留原来的文件路径喵
        new_config = parse_config(data, source_path=self.state.config.source_path)
        # 校验通过，整体替换内存配置喵
        self.state.replace_config(new_config)

    def _save(self, data: dict[str, Any]) -> None:
        """把配置字典写回磁盘喵~"""
        # 取配置文件路径喵
        path = self.state.config.source_path
        # 喵~防御：没有路径就没法保存喵
        if path is None:
            raise ConfigError("当前配置不是从文件加载的，无法保存喵")
        # 主人注意：PyYAML 不保留注释，所以 save 会把 config.yaml 里的中文注释冲掉。
        # 想保住注释的话，建议手改文件再用 reload 命令，而不是用 rule add / save 喵。
        text = yaml.safe_dump(data, allow_unicode=True, sort_keys=False, default_flow_style=False)
        # 写回文件，显式 utf-8 保证中文正常喵
        path.write_text(text, encoding="utf-8")

    def _mutate_config(self, mutator) -> None:
        """
        改配置的统一流程：重读 → 改 → 校验 → 应用 → 保存喵~

        输入：mutator 是个函数，接收整份配置字典并原地改动它，返回一段描述改了什么的文本
        说明：把「重读、校验、应用、保存」这四步公共流程收在这里，各个命令只关心怎么改。
             顺序上先校验后写盘很关键 —— 校验失败时磁盘上的文件还完全没被动过，
             运行中的代理也继续用旧配置，绝不会被一次手滑搞坏喵。
        """
        # 从磁盘重读原始配置喵
        data = self._read_raw_yaml()
        # 交给具体命令去改动，并拿回一段描述文本喵
        summary = mutator(data)
        # 先校验并应用，失败会抛异常，此时磁盘上的文件还没被动过，最安全喵
        self._apply(data)
        # 校验通过了才写盘喵
        self._save(data)
        # 打印改动摘要喵
        print(f"{summary}，已生效并写回配置文件喵~")

    def _mutate_rules(self, mutator) -> None:
        """
        改规则的流程，是 _mutate_config 的一层薄封装喵~

        输入：mutator 接收规则列表并原地改动它，返回一段描述文本
        """

        def wrapper(data: dict[str, Any]) -> str:
            """把整份配置里的规则列表取出来交给上层的 mutator 喵~"""
            # 取出规则列表，没有就当空列表喵
            rules = data.get("rules") or []
            # 喵~防御：规则段必须是列表，否则后面的下标操作会炸喵
            if not isinstance(rules, list):
                raise ConfigError("配置里的 rules 不是列表，无法改写喵")
            # 交给上层改动喵
            summary = mutator(rules)
            # 把改好的列表写回字典喵
            data["rules"] = rules
            # 在摘要后面补上现在共几条规则喵
            return f"{summary}，现在共 {len(rules)} 条规则"

        # 走统一的改配置流程喵
        self._mutate_config(wrapper)

    def _get_chain_raw(self, data: dict[str, Any], vm_name: str) -> list:
        """
        从原始配置字典里取出某个虚拟模型的候选链喵~

        输入：原始配置字典、虚拟模型名
        输出：候选链列表（就是字典里那个列表本身，改它等于改配置）
        边界条件：虚拟模型不存在时抛 ConfigError 并列出所有已有的名字帮主人改喵。
        """
        # 取出虚拟模型表喵
        vms = data.get("virtual_models")
        # 喵~防御：虚拟模型表必须是字典喵
        if not isinstance(vms, dict):
            raise ConfigError("配置里的 virtual_models 不是字典，无法改写喵")
        # 喵~防御：虚拟模型不存在时把已有的名字都列出来，方便主人看清是不是打错了喵
        if vm_name not in vms:
            available = "、".join(vms.keys()) or "（一个都没有）"
            raise ConfigError(f"虚拟模型 {vm_name!r} 不存在喵，现有的是：{available}")
        # 取出候选链喵
        chain = vms[vm_name]
        # 喵~防御：候选链必须是列表喵
        if not isinstance(chain, list):
            raise ConfigError(f"虚拟模型 {vm_name} 的候选链不是列表，无法改写喵")
        # 返回列表本身，调用方原地改动它即可喵
        return chain

    # ---------- 查看类命令喵 ----------

    def cmd_vm(self) -> None:
        """列出所有虚拟模型及其候选链喵~"""
        # 取当前配置快照喵
        config = self.state.config
        # 喵~防御：一个虚拟模型都没有时给提示，而不是打印一片空白喵
        if not config.virtual_models:
            print("还没配任何虚拟模型喵~")
            return
        # 逐个虚拟模型打印喵
        for name, chain in config.virtual_models.items():
            # 打印虚拟模型名和候选数量喵
            print(f"\n虚拟模型 {name}（{len(chain)} 个候选，按优先级排列）喵：")
            # 逐个候选打印，序号从 1 开始喵
            for i, candidate in enumerate(chain, start=1):
                # 查这个候选还剩多少秒冻结喵
                remaining = self.state.is_frozen(candidate)
                # 冻结中的标出剩余秒数，可用的标「可用」喵
                mark = f"[冻结中 剩{remaining:.0f}秒]" if remaining > 0 else "[可用]"
                # 打印一行候选信息喵
                print(f"  {i}. {mark} {candidate.label}")
                # 这个节点配了专属超时的话单独标一行喵。
                # 不标的话主人改了全局超时会困惑「为什么这个节点没跟着变」喵
                if candidate.timeout_note:
                    # 缩进对齐到上一行的内容位置喵
                    print(f"      {candidate.timeout_note}")

    def cmd_rule_ls(self) -> None:
        """列出所有规则及其序号喵~"""
        # 取当前规则列表喵
        rules = self.state.config.rules
        # 喵~防御：没有规则时说明所有失败都走默认动作喵
        if not rules:
            print("还没配任何规则，所有失败都会走默认动作（换下一个候选）喵~")
            return
        # 打印表头喵
        print(f"\n共 {len(rules)} 条规则，自上而下匹配、第一条命中即生效喵：")
        # 逐条打印，序号从 1 开始喵
        for i, rule in enumerate(rules, start=1):
            # 用 Rule 自己的 describe 渲染成一行人话喵
            print(f"  {i}. {rule.describe()}")

    def cmd_freeze_ls(self) -> None:
        """列出当前所有冻结中的候选喵~"""
        # 取冻结列表，已过期的会被顺手清理掉喵
        freezes = self.state.list_freezes()
        # 喵~防御：没有冻结时给个明确的好消息喵
        if not freezes:
            print("当前没有任何候选被冻结，全都可用喵~")
            return
        # 打印表头喵
        print(f"\n当前有 {len(freezes)} 个候选在冻结中喵：")
        # 逐条打印，按剩余时间从短到长喵
        for label, remaining, reason in freezes:
            # 打印候选标签和剩余秒数喵
            print(f"  剩 {remaining:6.0f} 秒  {label}")
            # 缩进打印冻结原因，截断到 120 字符保持终端整洁喵
            print(f"                原因：{reason[:120]}")

    def cmd_stats(self) -> None:
        """打印彩色的上游与虚拟模型健康统计喵~"""
        # 取累计候选统计快照喵
        stats = self.state.snapshot_stats()
        # 取当前配置快照，用于把身份串反查成候选和虚拟模型喵
        config = self.state.config
        # 打印总体累计计数，保留原有 stats 信息喵
        print(
            f"\n累计处理 {_format_count(self.state.total_requests)} 条请求，其中 "
            f"{_format_count(self.state.total_exhausted)} 条把整条候选链都用尽了喵"
        )
        # 没有任何统计时仍打印虚拟模型标题，方便主人确认命令正常工作喵
        if not stats and not config.virtual_models:
            print("还没有任何候选被使用过喵~")
            return
        # 按用户要求打印上游统计标题喵
        print(_ansi("\n各上游统计喵：", "33"))
        # 逐个配置候选打印，即使暂时没有请求也能看到暂无数据喵
        known_candidates = {
            candidate.identity: (virtual_model, candidate)
            for virtual_model, chain in config.virtual_models.items()
            for candidate in chain
        }
        # 统计表中已删除的历史候选不再展示，避免热更新后输出失效上游喵
        # 逐个候选输出完整健康信息喵
        for identity, (virtual_model, candidate) in known_candidates.items():
            # 从配置候选或身份串取得展示字段喵
            parts = identity.split("|")
            model_id = candidate.model if candidate is not None else (parts[2] if len(parts) >= 3 else identity)
            base_url = candidate.base_url if candidate is not None else (parts[0] if parts else identity)
            # 没有累计行时建立默认计数对象喵
            row = stats.get(identity)
            if row is None:
                from .state import CandidateStats
                row = CandidateStats()
            # 打印模型 ID、灰色连接符和地址喵
            print(f"  {_ansi(model_id, '36')} {_ansi('@', '90')} {_ansi(base_url, '90')}")
            # 按指定颜色展示累计计数喵
            print(
                f"    {_ansi('成功', '32')} {_format_count(row.success)} 次 / "
                f"{_ansi('失败', '31')} {_format_count(row.failure)} 次 / "
                f"{_ansi('被冻结', '36')} {_format_count(row.frozen_times)} 次"
            )
            # 展示五个成功率窗口喵
            snapshot = self.state.snapshot_candidate_health(candidate) if candidate is not None else None
            if snapshot is not None:
                print(f"    {_ansi('成功率:', '33')}")
                for window_name in ("所有时间", "近6小时", "近1小时", "近30分钟", "近10分钟"):
                    # 从快照中按用户指定顺序取窗口喵
                    window = snapshot.all_time if window_name == "所有时间" else snapshot.windows.get(window_name)
                    # 缺少窗口时按暂无请求处理，避免热重载期间异常喵
                    if window is None:
                        window = HealthWindow(0, 0, 0, None, None)
                    # 计算窗口百分比并选择阈值颜色喵
                    rate = window.success / window.total * 100 if window.total else None
                    rate_text, _ = _format_health_window(window)
                    print(f"      {_ansi(window_name, '33')}: {_ansi(rate_text, _rate_color(rate))}")
                # 历史条宽度扣除缩进和说明文字，剩余空间用于 144 格历史喵
                terminal_width = shutil.get_terminal_size(fallback=(120, 24)).columns
                print(f"      {_ansi('渲染图:', '33')} {_render_history(snapshot, max(1, terminal_width - 18))}")
                # 追加上游实际尝试的资源统计，时间从大到小并使用上游完成耗时喵
                for window_name, window in (("所有时间", snapshot.all_time), ("近6小时", snapshot.windows.get("近6小时")), ("近1小时", snapshot.windows.get("近1小时")), ("近30分钟", snapshot.windows.get("近30分钟")), ("近10分钟", snapshot.windows.get("近10分钟"))):
                    # 喵~防御：热重载期间快照缺少窗口时按无请求展示，避免 stats 命令报错喵
                    if window is None:
                        window = HealthWindow(0, 0, 0, None, None)
                    # 同时显示紧凑和完整 Token 数，方便终端浏览与精确核对喵
                    token_text = f"{_format_compact(window.tokens)}({_format_count(window.tokens)})"
                    print(
                        f"    {_ansi(window_name, '33')} 共 {_format_count(window.total)} 个请求 "
                        f"总Token数量: {token_text} 平均耗时: {_format_duration(window.average_elapsed_ms)} "
                        f"平均缓存命中率: {_format_cache_hit_rate(window.average_cache_hit_rate)}"
                    )
            # 展示连续失败与自动避险提示喵
            threshold = self.state.config.server.auto_hedge_threshold
            if row.consecutive_failures > 0:
                if threshold > 0:
                    remaining_failures = max(0, threshold - row.consecutive_failures)
                    print(
                        f"    {_ansi('连续失败', '31')} {row.consecutive_failures}/{threshold} 次"
                        f"（再失败 {remaining_failures} 次就自动避险）"
                    )
                else:
                    print(f"    {_ansi('连续失败', '31')} {row.consecutive_failures} 次（自动避险已关闭）")
            # 展示最近错误原文和报错时间，不修改上游返回内容喵
            if row.last_error:
                print(f"    {_ansi('最近错误:', '33')}")
                print(f"      {_ansi('返回的内容:', '33')} {row.last_error}")
                print(f"      {_ansi('报错时间:', '33')} {_format_error_time(row.last_error_at)}")
        # 打印虚拟模型统计标题喵
        print(_ansi("\n虚拟模型统计喵：", "33"))
        # 逐个配置虚拟模型输出客户端请求口径的统计喵
        for virtual_model in config.virtual_models:
            # 取虚拟模型健康快照喵
            snapshot = self.state.snapshot_virtual_model_health(virtual_model)
            # 打印模型名并复用同一套成功率和历史条格式喵
            print(f"  {_ansi(virtual_model, '36')}")
            print(f"    {_ansi('成功率:', '33')}")
            for window_name, window in (("所有时间", snapshot.all_time), *snapshot.windows.items()):
                # 计算窗口百分比并选择阈值颜色喵
                rate = window.success / window.total * 100 if window.total else None
                rate_text, _ = _format_health_window(window)
                print(f"      {_ansi(window_name, '33')}: {_ansi(rate_text, _rate_color(rate))}")
            # 历史条统一使用最近 24 小时十分钟格喵
            terminal_width = shutil.get_terminal_size(fallback=(120, 24)).columns
            print(f"      {_ansi('渲染图:', '33')} {_render_history(snapshot, max(1, terminal_width - 18))}")
            # 追加用户要求的五个吞吐与平均耗时窗口，时间从大到小喵
            for window_name, window in (("所有时间", snapshot.all_time), ("近6小时", snapshot.windows.get("近6小时")), ("近1小时", snapshot.windows.get("近1小时")), ("近30分钟", snapshot.windows.get("近30分钟")), ("近10分钟", snapshot.windows.get("近10分钟"))):
                # 近十分钟直接使用状态层的十分钟窗口，避免历史格边界误差喵
                # 缺少窗口时按暂无请求处理，避免热重载期间异常喵
                if window is None:
                    window = HealthWindow(0, 0, 0, None, None)
                # 只展示请求数、Token 数和平均耗时，缺 usage 按 0 体现喵
                token_text = f"{_format_compact(window.tokens)}({_format_count(window.tokens)})"
                print(
                    f"    {_ansi(window_name, '33')} 共 {_format_count(window.total)} 个请求 "
                    f"总Token数量: {token_text} 平均耗时: {_format_duration(window.average_elapsed_ms)} "
                    f"平均缓存命中率: {_format_cache_hit_rate(window.average_cache_hit_rate)}"
                )

    # ---------- 改规则类命令喵 ----------

    def cmd_rule_add(self, json_text: str) -> None:
        """追加一条规则到列表末尾喵~"""
        # 喵~防御：参数为空时提示正确用法喵
        if not json_text.strip():
            print('要带上规则的 JSON 喵，例如：rule add {"match": {"status": 429}, "action": "next"}')
            return
        # 解析 JSON 参数喵
        try:
            rule_obj = json.loads(json_text)
        # 喵~防御：JSON 写错时打印具体原因，不改任何东西喵
        except json.JSONDecodeError as exc:
            print(f"JSON 解析失败喵：{exc}")
            return
        # 喵~防御：规则必须是字典喵
        if not isinstance(rule_obj, dict):
            print("规则必须是一个 JSON 对象喵~")
            return
        # 定义改动函数：把新规则追加到列表末尾喵
        def mutator(rules: list) -> str:
            # 追加到末尾，也就是优先级最低喵
            rules.append(rule_obj)
            # 返回改动描述喵
            return f"已追加规则到第 {len(rules)} 位"
        # 走统一的改规则流程喵
        self._mutate_rules(mutator)

    def cmd_rule_rm(self, index_text: str) -> None:
        """删掉指定序号的规则喵~"""
        # 喵~防御：序号必须能转成整数喵
        try:
            index = int(index_text)
        except ValueError:
            print(f"序号 {index_text!r} 不是数字喵~")
            return
        # 定义改动函数：删掉指定位置的规则喵
        def mutator(rules: list) -> str:
            # 喵~防御：序号越界时抛 ConfigError，由外层统一捕获打印，且不会改动任何文件喵
            if not (1 <= index <= len(rules)):
                raise ConfigError(f"序号要在 1~{len(rules)} 之间喵")
            # 列表下标从 0 开始所以减 1喵
            removed = rules.pop(index - 1)
            # 返回改动描述，把删掉的内容也打出来方便主人确认喵
            return f"已删掉第 {index} 条规则：{json.dumps(removed, ensure_ascii=False)}"
        # 走统一的改规则流程喵
        self._mutate_rules(mutator)

    def cmd_rule_mv(self, from_text: str, to_text: str) -> None:
        """挪动规则位置来调整匹配优先级喵~"""
        # 喵~防御：两个序号都必须能转成整数喵
        try:
            src, dst = int(from_text), int(to_text)
        except ValueError:
            print("两个序号都得是数字喵~")
            return
        # 定义改动函数：把规则从原位置挪到目标位置喵
        def mutator(rules: list) -> str:
            # 喵~防御：两个序号都必须在有效范围内喵
            if not (1 <= src <= len(rules)) or not (1 <= dst <= len(rules)):
                raise ConfigError(f"序号要在 1~{len(rules)} 之间喵")
            # 先从原位置摘出来喵
            item = rules.pop(src - 1)
            # 再插到目标位置喵
            rules.insert(dst - 1, item)
            # 返回改动描述喵
            return f"已把第 {src} 条规则挪到第 {dst} 位"
        # 走统一的改规则流程喵
        self._mutate_rules(mutator)

    # ---------- 改候选链类命令喵 ----------

    def cmd_cand_add(self, vm_name: str, json_text: str) -> None:
        """给某个虚拟模型的候选链末尾追加一个候选喵~"""
        # 喵~防御：参数为空时提示正确用法，把必填字段都列出来喵
        if not json_text.strip():
            print(
                "要带上候选的 JSON 喵，例如：\n"
                '  cand add auto-strong {"name": "新中转", "base_url": "https://x.com", '
                '"api_key": "sk-xxx", "model": "gpt-4o"}\n'
                "  必填字段：base_url、api_key、model；可选：name、auth_style（bearer 或 x-api-key）喵"
            )
            return
        # 解析 JSON 参数喵
        try:
            candidate_obj = json.loads(json_text)
        # 喵~防御：JSON 写错时打印具体原因，不改任何东西喵
        except json.JSONDecodeError as exc:
            print(f"JSON 解析失败喵：{exc}")
            return
        # 喵~防御：候选必须是字典喵
        if not isinstance(candidate_obj, dict):
            print("候选必须是一个 JSON 对象喵~")
            return

        def mutator(data: dict[str, Any]) -> str:
            """把新候选追加到指定虚拟模型的链尾喵~"""
            # 取出候选链，虚拟模型不存在会在这里抛错喵
            chain = self._get_chain_raw(data, vm_name)
            # 追加到末尾，也就是优先级最低、最后才会被尝试喵
            chain.append(candidate_obj)
            # 返回改动描述喵
            return f"已给虚拟模型 {vm_name} 追加候选到第 {len(chain)} 位（共 {len(chain)} 个候选）"

        # 走统一的改配置流程喵
        self._mutate_config(mutator)

    def cmd_cand_rm(self, vm_name: str, index_text: str) -> None:
        """从某个虚拟模型的候选链里删掉一个候选喵~"""
        # 喵~防御：序号必须能转成整数喵
        try:
            index = int(index_text)
        except ValueError:
            print(f"序号 {index_text!r} 不是数字喵~")
            return

        def mutator(data: dict[str, Any]) -> str:
            """删掉指定位置的候选喵~"""
            # 取出候选链喵
            chain = self._get_chain_raw(data, vm_name)
            # 喵~防御：序号必须在有效范围内喵
            if not (1 <= index <= len(chain)):
                raise ConfigError(f"序号要在 1~{len(chain)} 之间喵")
            # 喵~防御：候选链不能删空，空链意味着这个虚拟模型永远无法服务，
            # 而且会在校验阶段被拒；在这里提前拦住能给出更清楚的提示喵
            if len(chain) == 1:
                raise ConfigError(
                    f"虚拟模型 {vm_name} 只剩这一个候选了，删掉它就没法服务了喵。"
                    f"想彻底不要这个虚拟模型的话用 vm rm {vm_name} 喵~"
                )
            # 删掉指定位置的候选喵
            removed = chain.pop(index - 1)
            # 取出被删候选的名字用于展示，没有 name 就用 base_url 代替喵
            shown = removed.get("name") or removed.get("base_url") if isinstance(removed, dict) else str(removed)
            # 返回改动描述喵
            return f"已从虚拟模型 {vm_name} 删掉第 {index} 个候选 {shown!r}（还剩 {len(chain)} 个）"

        # 走统一的改配置流程喵
        self._mutate_config(mutator)

    def cmd_cand_set(self, vm_name: str, index_text: str, field: str, value: str) -> None:
        """
        改某个候选节点的单个字段喵~

        用来改上游地址、api key、真实模型名这些。比「删了重加」好用得多：
        不用把整个候选重新敲一遍，而且节点在链里的位置（也就是优先级）不会变喵。
        """
        # 允许改的字段，以及各自的中文说明喵
        allowed_fields = {
            # 上游根地址喵
            "base_url": "上游根地址",
            # 该上游的 api key 喵
            "api_key": "api key",
            # 发给上游的真实模型名喵
            "model": "真实模型名",
            # 显示用的名字喵
            "name": "显示名字",
            # 鉴权头风格喵
            "auth_style": "鉴权风格（bearer 或 x-api-key）",
            # 这个节点专属的静默上限，覆盖 server 段的全局值喵
            "stall_timeout": "本节点专属：允许上游静默多少秒（留空恢复全局值）",
            # 这个节点专属的流式总预算喵
            "stream_timeout": "本节点专属：流式请求总预算秒数（留空恢复全局值）",
            # 这个节点专属的非流式总预算喵
            "nonstream_timeout": "本节点专属：非流式请求总预算秒数（留空恢复全局值）",
        }
        # 喵~防御：字段名不认识时列出所有能改的字段和它们的含义喵
        if field not in allowed_fields:
            print("能改的字段是喵：")
            # 逐个打印字段名和说明喵
            for key, desc in allowed_fields.items():
                print(f"  {key:18} {desc}")
            return
        # 喵~防御：序号必须能转成整数喵
        try:
            index = int(index_text)
        except ValueError:
            print(f"序号 {index_text!r} 不是数字喵~")
            return
        # 这个字段是不是超时覆盖类的，它们的取值规则和别的字段不一样喵
        is_timeout_field = field in CANDIDATE_TIMEOUT_FIELDS
        # 超时覆盖允许被「清掉」，用 -、default 或 none 表示恢复跟随全局值喵
        clearing = is_timeout_field and value.strip().lower() in ("-", "default", "none", "")
        # 喵~防御：非超时字段的值不能是空字符串，空的 base_url 或 api_key 会让节点必然失败喵
        if not value.strip() and not is_timeout_field:
            print(f"{field} 不能设成空的喵~")
            return
        # 超时字段且不是在清值，那就必须是一个大于 0 的数字喵
        if is_timeout_field and not clearing:
            # 喵~防御：转不成数字就明确提示，并说明怎么清掉这个覆盖喵
            try:
                seconds = float(value.strip())
            except ValueError:
                print(f"{field} 要填秒数喵，{value.strip()!r} 转不成数字~（想恢复全局值就填 default）")
                return
            # 喵~防御：非正数会让请求一发出就判超时，肯定不是主人想要的喵
            if seconds <= 0:
                print(f"{field} 要大于 0 喵~（想恢复全局值就填 default）")
                return

        def mutator(data: dict[str, Any]) -> str:
            """改指定候选的指定字段喵~"""
            # 取出候选链喵
            chain = self._get_chain_raw(data, vm_name)
            # 喵~防御：序号必须在有效范围内喵
            if not (1 <= index <= len(chain)):
                raise ConfigError(f"序号要在 1~{len(chain)} 之间喵")
            # 取出要改的那个候选喵
            candidate = chain[index - 1]
            # 喵~防御：候选必须是字典才能改字段喵
            if not isinstance(candidate, dict):
                raise ConfigError(f"第 {index} 个候选不是字典，无法改字段喵")
            # 记下原值用于展示喵
            old = candidate.get(field, "（未设置）")
            # api key 属于敏感信息，展示时脱敏，避免在终端上打出完整的旧 key 喵
            if field == "api_key":
                # 旧 key 太短就整体打码，否则只留头尾喵
                old = "***" if len(str(old)) <= 11 else f"{str(old)[:6]}***{str(old)[-4:]}"
            # 清掉超时覆盖：把这个键整个删掉，让它回到「跟随全局值」的状态喵
            if clearing:
                # 喵~防御：本来就没配过时不报错，只说明现状，保持命令幂等喵
                if field not in candidate:
                    return f"虚拟模型 {vm_name} 第 {index} 个候选本来就没配 {field}，现在依然跟随全局值"
                # 删掉这个键喵
                del candidate[field]
                # 返回改动描述喵
                return f"已清掉虚拟模型 {vm_name} 第 {index} 个候选的 {field}（改回跟随全局值）"
            # 超时字段写成数字而不是字符串，这样 YAML 里是干净的数值喵
            if is_timeout_field:
                # 写入浮点秒数喵
                candidate[field] = float(value.strip())
                # 返回改动描述喵
                return (
                    f"已把虚拟模型 {vm_name} 第 {index} 个候选的 {field} "
                    f"从 {old} 改成 {float(value.strip()):.0f} 秒（只对这个节点生效）"
                )
            # 写入新值，去掉首尾空白防止复制粘贴带进空格喵
            candidate[field] = value.strip()
            # 返回改动描述，新值同样对 key 做脱敏喵
            shown = "（已更新，为安全不回显）" if field == "api_key" else repr(value.strip())
            # 拼成人话喵
            return f"已把虚拟模型 {vm_name} 第 {index} 个候选的 {field} 从 {old} 改成 {shown}"

        # 走统一的改配置流程喵
        self._mutate_config(mutator)

    def cmd_cand_mv(self, vm_name: str, from_text: str, to_text: str) -> None:
        """挪动候选的位置来调整优先级喵~"""
        # 喵~防御：两个序号都必须能转成整数喵
        try:
            src, dst = int(from_text), int(to_text)
        except ValueError:
            print("两个序号都得是数字喵~")
            return

        def mutator(data: dict[str, Any]) -> str:
            """把候选从原位置挪到目标位置喵~"""
            # 取出候选链喵
            chain = self._get_chain_raw(data, vm_name)
            # 喵~防御：两个序号都必须在有效范围内喵
            if not (1 <= src <= len(chain)) or not (1 <= dst <= len(chain)):
                raise ConfigError(f"序号要在 1~{len(chain)} 之间喵")
            # 先从原位置摘出来喵
            item = chain.pop(src - 1)
            # 再插到目标位置喵
            chain.insert(dst - 1, item)
            # 返回改动描述，顺便说明第 1 位就是最优先喵
            return f"已把虚拟模型 {vm_name} 的第 {src} 个候选挪到第 {dst} 位（第 1 位最优先）"

        # 走统一的改配置流程喵
        self._mutate_config(mutator)

    def cmd_vm_add(self, vm_name: str, json_text: str) -> None:
        """新建一个虚拟模型，同时给它配上第一个候选喵~"""
        # 喵~防御：参数为空时提示用法。必须同时给候选，因为空链的虚拟模型是非法配置喵
        if not json_text.strip():
            print(
                "新建虚拟模型要同时给第一个候选喵（空候选链是非法配置），例如：\n"
                '  vm add my-model {"name": "主力", "base_url": "https://x.com", '
                '"api_key": "sk-xxx", "model": "gpt-4o"}'
            )
            return
        # 解析 JSON 参数喵
        try:
            candidate_obj = json.loads(json_text)
        # 喵~防御：JSON 写错时打印原因喵
        except json.JSONDecodeError as exc:
            print(f"JSON 解析失败喵：{exc}")
            return
        # 喵~防御：候选必须是字典喵
        if not isinstance(candidate_obj, dict):
            print("候选必须是一个 JSON 对象喵~")
            return

        def mutator(data: dict[str, Any]) -> str:
            """在虚拟模型表里加一项喵~"""
            # 取出虚拟模型表，没有就新建一个空字典喵
            vms = data.setdefault("virtual_models", {})
            # 喵~防御：虚拟模型表必须是字典喵
            if not isinstance(vms, dict):
                raise ConfigError("配置里的 virtual_models 不是字典，无法改写喵")
            # 喵~防御：名字重复时拒绝，避免悄悄把已有的候选链整条覆盖掉喵
            if vm_name in vms:
                raise ConfigError(
                    f"虚拟模型 {vm_name!r} 已经存在了喵，"
                    f"想给它加候选请用 cand add {vm_name} <JSON> 喵~"
                )
            # 新建这个虚拟模型，候选链里先放这一个候选喵
            vms[vm_name] = [candidate_obj]
            # 返回改动描述喵
            return f"已新建虚拟模型 {vm_name}，并配上第 1 个候选"

        # 走统一的改配置流程喵
        self._mutate_config(mutator)

    def cmd_vm_rm(self, vm_name: str) -> None:
        """删掉一整个虚拟模型喵~"""

        def mutator(data: dict[str, Any]) -> str:
            """从虚拟模型表里删掉一项喵~"""
            # 取出虚拟模型表喵
            vms = data.get("virtual_models")
            # 喵~防御：虚拟模型表必须是字典喵
            if not isinstance(vms, dict):
                raise ConfigError("配置里的 virtual_models 不是字典，无法改写喵")
            # 喵~防御：要删的虚拟模型必须存在，列出现有的帮主人核对喵
            if vm_name not in vms:
                available = "、".join(vms.keys()) or "（一个都没有）"
                raise ConfigError(f"虚拟模型 {vm_name!r} 不存在喵，现有的是：{available}")
            # 喵~防御：不能把最后一个虚拟模型删掉，否则代理就没有任何可服务的模型了，
            # 而且会在校验阶段被拒；提前拦住能给出更清楚的原因喵
            if len(vms) == 1:
                raise ConfigError("这是最后一个虚拟模型了，删掉代理就没东西可服务了喵~")
            # 记下候选数量用于展示喵
            count = len(vms[vm_name]) if isinstance(vms[vm_name], list) else 0
            # 删掉这个虚拟模型喵
            del vms[vm_name]
            # 返回改动描述喵
            return f"已删掉虚拟模型 {vm_name}（连带它的 {count} 个候选）"

        # 走统一的改配置流程喵
        self._mutate_config(mutator)

    def cmd_set(self, field: str, value_text: str) -> None:
        """改 server 段的某个配置项喵~"""
        # 可以改的字段，以及各自的取值类型转换函数喵
        numeric_fields = {
            # 流式请求放行之前的总预算，单位：秒喵
            "stream_timeout": float,
            # 非流式请求的总预算，单位：秒喵
            "nonstream_timeout": float,
            # 连接握手超时，单位：秒喵
            "connect_timeout": float,
            # 配置热重载的轮询间隔，单位：秒喵
            "reload_poll_interval": float,
            # 允许上游静默多少秒，超过算卡流，单位：秒喵
            "stall_timeout": float,
            # 放行给客户端前需要累积的内容字符数喵
            "min_content_chars": int,
            # 自动避险：连续失败多少次就冻结这个节点，0 表示关闭喵
            "auto_hedge_threshold": int,
            # 自动避险的冻结时长，单位：分钟喵
            "auto_hedge_minutes": float,
            # 动态 RPM/TPM/平均耗时的滚动窗口，单位：分钟喵
            "metrics_window_minutes": float,
            # 监听端口喵
            "port": int,
        }
        # 喵~防御：主人如果敲了已经退役的配置项名，光说「不能改」会让人一头雾水，
        # 所以专门认出这些老名字并告诉主人现在该改哪一项喵
        if field in RETIRED_SERVER_KEYS:
            print(f"{field} 已经退役了喵：{RETIRED_SERVER_KEYS[field]}")
            return
        # 喵~防御：字段名不认识时列出所有能改的字段喵
        if field not in numeric_fields:
            allowed = "、".join(numeric_fields)
            print(f"不能改字段 {field!r} 喵，能改的是：{allowed}~")
            return
        # 把输入的文本转成对应的数值类型喵
        try:
            value = numeric_fields[field](value_text)
        # 喵~防御：转换失败时提示要填数字喵
        except ValueError:
            print(f"{field} 要填数字喵，{value_text!r} 转不过去~")
            return

        def mutator(data: dict[str, Any]) -> str:
            """改 server 段里的一个字段喵~"""
            # 取出 server 段，没有就新建喵
            server = data.setdefault("server", {})
            # 喵~防御：server 段必须是字典喵
            if not isinstance(server, dict):
                raise ConfigError("配置里的 server 不是字典，无法改写喵")
            # 记下原值用于展示喵
            old = server.get(field, "（未设置）")
            # 写入新值喵
            server[field] = value
            # 返回改动描述喵
            return f"已把 server.{field} 从 {old} 改成 {value}"

        # 走统一的改配置流程喵
        self._mutate_config(mutator)
        # 喵~防御：host 和 port 在 uvicorn 启动时就已经绑定了 socket，改配置不会重新绑定，
        # 所以必须明确告诉主人这两项要重启才生效，免得白等喵
        if field == "port":
            print("  注意喵：端口在启动时就已经绑好了，这项改动要重启代理才生效~")

    # ---------- 冻结与配置类命令喵 ----------

    def cmd_freeze_add(self, vm_name: str, model_or_index: str, seconds_text: str) -> None:
        """
        主动把某个节点冻结指定秒数喵~

        用途：主人已经知道某个上游要维护、或者想临时把流量从某个节点挪走时，
             不用改配置删节点，冻一段时间就行，到期自动恢复喵。

        节点可以用两种方式指定，因为两种都好用在不同场合喵：
            按真实模型名（比如 gpt-4o）—— 从日志或横幅里直接抄过来最方便
            按序号（比如 2）—— vm 命令里看到的那个序号，同名节点多时用它更准
        """
        # 喵~防御：秒数必须能转成数字喵
        try:
            seconds = float(seconds_text)
        except ValueError:
            print(f"秒数 {seconds_text!r} 不是数字喵~")
            return
        # 喵~防御：非正数冻结毫无意义，直接挡掉并提示喵
        if seconds <= 0:
            print("冻结秒数要大于 0 喵~")
            return
        # 取候选链，虚拟模型不存在会抛 ConfigError 由 dispatch 兜住喵
        chain = self.state.get_chain(vm_name)
        # 喵~防御：虚拟模型不存在时列出现有的帮主人核对喵
        if chain is None:
            available = "、".join(self.state.list_virtual_models()) or "（一个都没有）"
            print(f"虚拟模型 {vm_name!r} 不存在喵，现有的是：{available}")
            return
        # 找出要冻结的那些节点喵
        targets = []
        # 先试着把参数当序号解析喵
        if model_or_index.isdigit():
            # 转成整数喵
            index = int(model_or_index)
            # 喵~防御：序号越界时明确提示范围喵
            if not (1 <= index <= len(chain)):
                print(f"序号要在 1~{len(chain)} 之间喵~")
                return
            # 按序号取那一个节点喵
            targets = [chain[index - 1]]
        # 不是纯数字就当成真实模型名来匹配喵
        else:
            # 收集所有 model 名匹配的节点。可能有多个，因为同一个模型名可能配了多个渠道喵
            targets = [c for c in chain if c.model == model_or_index]
            # 喵~防御：一个都没匹配上时把这个虚拟模型下所有可选的模型名列出来喵
            if not targets:
                names = "、".join(sorted({c.model for c in chain}))
                print(f"虚拟模型 {vm_name} 下没有模型名为 {model_or_index!r} 的节点喵，可选的有：{names}")
                return
        # 逐个冻结匹配到的节点喵
        for candidate in targets:
            # 写入冻结表，原因里标明是手动冻结，好和自动避险、额度限制区分开喵
            self.state.freeze(candidate, seconds, f"主人手动冻结 {seconds:.0f} 秒")
            # 打印结果，带上倒计时的可读形式喵
            print(f"已冻结 {candidate.label}，{format_countdown(seconds)}后自动恢复喵~")
        # 匹配到多个时补一句说明，免得主人以为多冻了喵
        if len(targets) > 1:
            print(f"（模型名 {model_or_index} 在这个虚拟模型下有 {len(targets)} 个节点，都冻上了喵）")

    def cmd_freeze_rm(self, vm_name: str, model_or_index: str) -> None:
        """
        解冻某个指定的节点喵~

        和 freeze clear 的区别：clear 是一把清空所有冻结，这个只解冻主人点名的那一个。
        节点被自动避险冻上、但主人确认它其实已经好了的时候，用这个最合适喵。
        """
        # 取候选链喵
        chain = self.state.get_chain(vm_name)
        # 喵~防御：虚拟模型不存在时列出现有的喵
        if chain is None:
            available = "、".join(self.state.list_virtual_models()) or "（一个都没有）"
            print(f"虚拟模型 {vm_name!r} 不存在喵，现有的是：{available}")
            return
        # 找出要解冻的节点，逻辑和 freeze add 一致喵
        if model_or_index.isdigit():
            # 转成整数喵
            index = int(model_or_index)
            # 喵~防御：序号越界喵
            if not (1 <= index <= len(chain)):
                print(f"序号要在 1~{len(chain)} 之间喵~")
                return
            # 按序号取喵
            targets = [chain[index - 1]]
        else:
            # 按模型名匹配喵
            targets = [c for c in chain if c.model == model_or_index]
            # 喵~防御：没匹配上时列出可选的模型名喵
            if not targets:
                names = "、".join(sorted({c.model for c in chain}))
                print(f"虚拟模型 {vm_name} 下没有模型名为 {model_or_index!r} 的节点喵，可选的有：{names}")
                return
        # 记录实际解冻了几个（本来就没冻的不算）喵
        unfrozen = 0
        # 逐个解冻喵
        for candidate in targets:
            # 只有本来在冻结中的才算真的解冻了一个喵
            if self.state.is_frozen(candidate) > 0:
                # 解冻它喵
                self.state.unfreeze(candidate)
                # 计数加一喵
                unfrozen += 1
                # 打印结果喵
                print(f"已解冻 {candidate.label}，现在立刻可用喵~")
        # 喵~防御：一个都没解冻说明它们本来就没被冻结，明确告知而不是静默无反应喵
        if unfrozen == 0:
            print("这些节点本来就没被冻结喵~")

    def cmd_freeze_clear(self) -> None:
        """清空所有冻结记录喵~"""
        # 清空并拿到被清掉的条数喵
        count = self.state.clear_freezes()
        # 打印结果喵
        print(f"已清掉 {count} 条冻结记录，相关候选现在立刻可用喵~")

    def cmd_reload(self) -> None:
        """从磁盘重新加载配置喵~"""
        # 重读磁盘上的原始配置喵
        data = self._read_raw_yaml()
        # 校验并应用，失败会抛 ConfigError 由外层打印喵
        self._apply(data)
        # 取一份新配置用于打印摘要喵
        config = self.state.config
        # 打印重载结果摘要喵
        print(f"配置已重新加载喵：{len(config.virtual_models)} 个虚拟模型，{len(config.rules)} 条规则~")

    def cmd_save(self) -> None:
        """把当前磁盘配置重新格式化写回（主要用于确认配置能被正确序列化）喵~"""
        # 重读磁盘配置喵
        data = self._read_raw_yaml()
        # 先校验一遍，坏配置不给写盘喵
        self._apply(data)
        # 写回磁盘喵
        self._save(data)
        # 打印结果喵
        print("配置已校验并写回 config.yaml 喵~")

    # ---------- 命令分发喵 ----------

    def dispatch(self, line: str) -> None:
        """
        执行一行命令，并兜住其中所有异常喵~

        为什么兜底放在这一层：这样「执行一条命令永远不会抛异常出来」就是 dispatch 自己的
        保证，而不是依赖调用方记得去 try。REPL 主循环因此能保持简单，
        测试也可以直接调 dispatch 而不必自己包 try 喵。
        """
        # 执行命令，各类异常分别给出友好提示喵
        try:
            self._dispatch(line)
        # 喵~防御：配置类错误打印友好中文提示，配置和磁盘文件都保持不变喵
        except ConfigError as exc:
            print(f"操作失败喵：{exc}")
        # 喵~防御：其他任何异常都打印出来但不往上抛，保证 REPL 一直可用喵
        except Exception as exc:  # noqa: BLE001
            print(f"命令执行出错喵：{type(exc).__name__}: {exc}")

    def _dispatch(self, line: str) -> None:
        """
        解析一行输入并分发到对应的命令喵~

        输入：主人敲的一整行文本
        说明：按空格切分，但只切前几段 —— JSON 参数里有空格，
             所以必须保留剩余部分的原样，不能无脑全切喵。
        """
        # 去掉首尾空白喵
        line = line.strip()
        # 喵~防御：空行直接忽略，不打印任何东西，符合命令行习惯喵
        if not line:
            return
        # 按空格切分成词，用于识别命令名喵
        parts = line.split()
        # 第一个词是主命令，转小写以容忍大写输入喵
        head = parts[0].lower()
        # help 命令打印帮助喵
        if head in ("help", "?", "h"):
            print(HELP_TEXT)
            return
        # quit 命令置位退出信号喵
        if head in ("quit", "exit", "q"):
            # 打印告别信息喵
            print("代理要关掉了喵~ 拜拜~")
            # 置位退出事件，主线程会据此结束进程喵
            self.should_exit.set()
            return
        # vm 系列命令：不带子命令就是列出，带 add/rm 就是改虚拟模型喵
        if head == "vm":
            # 不带子命令时列出所有虚拟模型喵
            if len(parts) < 2:
                self.cmd_vm()
                return
            # 取子命令名喵
            sub = parts[1].lower()
            # vm add 新建虚拟模型，第三段是名字，剩下的全部是候选 JSON 喵
            if sub == "add":
                # 喵~防御：必须带虚拟模型名喵
                if len(parts) < 3:
                    print('要带上虚拟模型名喵，例如：vm add my-model {"base_url": "...", "api_key": "...", "model": "..."}')
                    return
                # 只切前三段，第四段保留原样当 JSON（JSON 里有空格不能切）喵
                pieces = line.split(maxsplit=3)
                # 调用新建命令，没带 JSON 时传空串让它打印用法喵
                self.cmd_vm_add(parts[2], pieces[3] if len(pieces) >= 4 else "")
                return
            # vm rm 删掉一整个虚拟模型喵
            if sub in ("rm", "remove", "del"):
                # 喵~防御：必须带虚拟模型名喵
                if len(parts) < 3:
                    print("要带上虚拟模型名喵，例如：vm rm auto-cheap")
                    return
                self.cmd_vm_rm(parts[2])
                return
            # 喵~防御：不认识的子命令给出提示喵
            print(f"不认识的 vm 子命令 {sub!r} 喵，直接敲 vm 可以列出所有虚拟模型~")
            return
        # cand 系列命令：管理某个虚拟模型的候选链喵
        if head in ("cand", "candidate"):
            # 喵~防御：至少要有子命令和虚拟模型名喵
            if len(parts) < 3:
                print("用法喵：cand add <虚拟模型> <JSON> / cand rm <虚拟模型> <序号> / cand mv <虚拟模型> <原> <新>")
                return
            # 取子命令名和虚拟模型名喵
            sub = parts[1].lower()
            vm_name = parts[2]
            # cand add 追加候选，第四段之后全是 JSON 喵
            if sub == "add":
                # 只切前三段，第四段保留原样当 JSON 喵
                pieces = line.split(maxsplit=3)
                # 调用追加命令，没带 JSON 时传空串让它打印用法喵
                self.cmd_cand_add(vm_name, pieces[3] if len(pieces) >= 4 else "")
                return
            # cand rm 删候选喵
            if sub in ("rm", "remove", "del"):
                # 喵~防御：必须带序号喵
                if len(parts) < 4:
                    print("要带上序号喵，例如：cand rm auto-strong 2")
                    return
                self.cmd_cand_rm(vm_name, parts[3])
                return
            # cand mv 挪候选优先级喵
            if sub in ("mv", "move"):
                # 喵~防御：必须带两个序号喵
                if len(parts) < 5:
                    print("要带上两个序号喵，例如：cand mv auto-strong 3 1")
                    return
                self.cmd_cand_mv(vm_name, parts[3], parts[4])
                return
            # cand set 改某个候选的单个字段（地址、key、模型名等）喵
            if sub == "set":
                # 喵~防御：需要序号、字段名、新值三样喵
                if len(parts) < 6:
                    print(
                        "用法喵：cand set <虚拟模型> <序号> <字段> <值>\n"
                        "  例如：cand set auto-strong 2 api_key sk-new-key-here\n"
                        "        cand set auto-strong 2 base_url https://newrelay.com\n"
                        "        cand set auto-strong 2 model gpt-4o-2024-11-20\n"
                        "  给单个节点配专属超时喵（只影响这一个节点）：\n"
                        "        cand set auto-strong 1 stream_timeout 600\n"
                        "        cand set auto-strong 1 stall_timeout 180\n"
                        "        cand set auto-strong 1 stream_timeout default   （改回跟随全局值）"
                    )
                    return
                # 只切前五段，第六段之后保留原样当值（值里可能有空格，比如显示名字）喵
                pieces = line.split(maxsplit=5)
                # 调用改字段命令喵
                self.cmd_cand_set(vm_name, parts[3], parts[4].lower(), pieces[5])
                return
            # 喵~防御：不认识的子命令给出提示喵
            print(f"不认识的 cand 子命令 {sub!r} 喵，支持 add / rm / mv / set~")
            return
        # set 命令改 server 段的配置项喵
        if head == "set":
            # 喵~防御：必须带字段名和值喵
            if len(parts) < 3:
                print("用法喵：set <字段> <值>，例如：set first_content_timeout 60")
                return
            self.cmd_set(parts[1].lower(), parts[2])
            return
        # stats 命令打统计喵
        if head == "stats":
            self.cmd_stats()
            return
        # rule 系列命令，需要看第二个词决定子命令喵
        if head == "rule":
            # 喵~防御：只敲了 rule 没带子命令时提示用法喵
            if len(parts) < 2:
                print("rule 后面要带子命令喵：ls / add / rm / mv~")
                return
            # 取子命令名喵
            sub = parts[1].lower()
            # rule ls 列规则喵
            if sub in ("ls", "list"):
                self.cmd_rule_ls()
                return
            # rule add 追加规则，参数是剩下的全部文本（JSON 里有空格不能切）喵
            if sub == "add":
                # 用 split(maxsplit=2) 只切前两段，第三段保留原样当 JSON 喵
                pieces = line.split(maxsplit=2)
                # 喵~防御：没带 JSON 参数时提示用法喵
                self.cmd_rule_add(pieces[2] if len(pieces) >= 3 else "")
                return
            # rule rm 删规则喵
            if sub in ("rm", "remove", "del"):
                # 喵~防御：没带序号时提示用法喵
                if len(parts) < 3:
                    print("要带上序号喵，例如：rule rm 3")
                    return
                self.cmd_rule_rm(parts[2])
                return
            # rule mv 挪规则喵
            if sub in ("mv", "move"):
                # 喵~防御：必须带两个序号喵
                if len(parts) < 4:
                    print("要带上两个序号喵，例如：rule mv 5 1")
                    return
                self.cmd_rule_mv(parts[2], parts[3])
                return
            # 喵~防御：不认识的子命令给出提示喵
            print(f"不认识的 rule 子命令 {sub!r} 喵，试试 rule ls~")
            return
        # freeze 系列命令喵
        if head == "freeze":
            # 喵~防御：只敲了 freeze 时提示用法喵
            if len(parts) < 2:
                print("freeze 后面要带子命令喵：ls / clear~")
                return
            # 取子命令名喵
            sub = parts[1].lower()
            # freeze ls 列冻结喵
            if sub in ("ls", "list"):
                self.cmd_freeze_ls()
                return
            # freeze clear 清空冻结喵
            if sub == "clear":
                self.cmd_freeze_clear()
                return
            # freeze add 主动冻结某个节点喵
            if sub == "add":
                # 喵~防御：需要虚拟模型、节点、秒数三样喵
                if len(parts) < 5:
                    print(
                        "用法喵：freeze add <虚拟模型> <模型名或序号> <秒数>\n"
                        "  例如：freeze add auto-free openai/gpt-5.6-terra 600\n"
                        "        freeze add auto-free 2 600     （2 是 vm 里看到的序号）"
                    )
                    return
                self.cmd_freeze_add(parts[2], parts[3], parts[4])
                return
            # freeze rm 解冻某个指定节点喵
            if sub in ("rm", "remove", "del"):
                # 喵~防御：需要虚拟模型和节点两样喵
                if len(parts) < 4:
                    print(
                        "用法喵：freeze rm <虚拟模型> <模型名或序号>\n"
                        "  例如：freeze rm auto-free openai/gpt-5.6-terra"
                    )
                    return
                self.cmd_freeze_rm(parts[2], parts[3])
                return
            # 喵~防御：不认识的子命令给出提示喵
            print(f"不认识的 freeze 子命令 {sub!r} 喵，支持 ls / add / rm / clear~")
            return
        # target 系列命令：只改运行期内存状态，绝不写 config.yaml 喵
        if head == "target":
            # 没带参数时视为查询当前状态喵
            sub = parts[1].lower() if len(parts) >= 2 else "status"
            # 开启目标模式喵
            if sub in ("on", "enable"):
                self.state.set_target_mode(True)
                config = self.state.config
                interval_sec = config.server.target_mode_round_interval_seconds
                max_wait_min = config.server.target_mode_max_wait_seconds / 60
                print(
                    f"🎯 目标模式已开启喵：链路全失效后会每 {interval_sec:.0f} 秒从链首重试，"
                    f"最长坚持 {max_wait_min:.0f} 分钟~"
                )
                return
            # 关闭目标模式喵
            if sub in ("off", "disable"):
                self.state.set_target_mode(False)
                print("目标模式已关闭喵：链路全失效时恢复立即返回 502 的默认行为~")
                return
            # 查看状态喵
            if sub in ("status", "ls"):
                status = "已开启" if self.state.target_mode_enabled else "未开启"
                print(f"目标模式当前{status}喵（仅本次运行有效，重启后自动关闭）")
                return
            # 喵~防御：未知子命令给出明确用法喵
            print(f"不认识的 target 子命令 {sub!r} 喵，支持 on / off / status~")
            return
        # reload 命令重载配置喵
        if head == "reload":
            self.cmd_reload()
            return
        # save 命令写回配置喵
        if head == "save":
            self.cmd_save()
            return
        # 喵~防御：完全不认识的命令，提示去看 help，而不是静默无反应喵
        print(f"不认识的命令 {head!r} 喵，敲 help 看看有哪些命令~")

    def _build_completer(self):
        """
        创建 REPL 的 Tab 补全器喵~

        单独拆成方法的原因：补全词表本身是纯逻辑，不该和 PromptSession 的终端初始化绑死。
        Windows 下在非 TTY 的测试环境创建 PromptSession 会报 NoConsoleScreenBufferError，
        但我们仍然要能直接测试「freeze add 是否在词表里」，所以把它独立出来喵。
        """
        # 在方法内导入，保持模块在没装 prompt_toolkit 时仍能被普通模式导入喵
        from prompt_toolkit.completion import WordCompleter
        # 可以 Tab 补全的命令词，覆盖所有主命令、子命令、别名和配置字段喵
        completer_words = [
            # 顶层查看与控制命令喵
            "vm", "stats", "help", "reload", "save", "quit", "exit", "q",
            # 虚拟模型管理及别名喵
            "vm add", "vm rm", "vm remove", "vm del",
            # 候选链管理及 candidate 别名喵
            "cand", "candidate",
            "cand add", "cand rm", "cand remove", "cand del",
            "cand mv", "cand move", "cand set",
            "candidate add", "candidate rm", "candidate remove", "candidate del",
            "candidate mv", "candidate move", "candidate set",
            # 规则管理及查看别名喵
            "rule", "rule ls", "rule list", "rule add", "rule rm", "rule remove",
            "rule del", "rule mv", "rule move",
            # 冻结管理：这里必须包含 freeze add / freeze rm，主人反馈的就是漏了它们喵
            "freeze", "freeze ls", "freeze list", "freeze add", "freeze rm",
            "freeze remove", "freeze del", "freeze clear",
            # 目标模式开关喵
            "target", "target on", "target off", "target status",
            # 全局 server 配置字段喵
            "set", "set stall_timeout", "set stream_timeout",
            "set nonstream_timeout", "set connect_timeout", "set min_content_chars",
            "set auto_hedge_threshold", "set auto_hedge_minutes", "set metrics_window_minutes",
            "set reload_poll_interval", "set port",
            # cand set 可修改的节点字段喵
            "cand set base_url", "cand set api_key", "cand set model", "cand set name",
            "cand set auth_style", "cand set stall_timeout", "cand set stream_timeout",
            "cand set nonstream_timeout",
        ]
        # 创建补全器，并按大小写不敏感方式匹配命令前缀喵
        return WordCompleter(
            # 传入整理好的命令词表喵
            completer_words,
            # 允许完整命令短语按输入前缀匹配喵
            sentence=True,
            # 命令大小写不敏感，和 dispatch 的行为保持一致喵
            ignore_case=True,
        )

    def _build_session(self):
        """
        创建 prompt_toolkit 的输入会话喵~

        为什么用 prompt_toolkit 而不是内置的 input()：
            主人想要一个常驻在提示符上方、每秒自动刷新的冻结表。用 input() 的话，
            刷新那一刻如果主人正在敲命令，输入的字会被重绘冲掉、光标也会错位。
            prompt_toolkit 把「输入行」和「工具栏」分开管理，刷新工具栏时输入行
            完全不受影响，还白拿命令历史（上下键）和 Tab 补全喵。

        返回值：配置好的 PromptSession；拿不到（比如 stdin 不是终端）时返回 None，
               由调用方退回到朴素的 input() 模式喵。
        """
        # 在方法内导入，这样即使 prompt_toolkit 没装，模块本身也能正常导入，
        # 单元测试和 --no-repl 模式都不受影响喵
        from prompt_toolkit import PromptSession
        from prompt_toolkit.history import InMemoryHistory
        from prompt_toolkit.styles import Style

        # 定义横幅各部分的配色喵。
        # 主人注意：这里刻意「只染文字颜色、不动背景」喵。
        # prompt_toolkit 的 bottom_toolbar 默认样式是反显的（相当于给整条加灰底），
        # 所以必须显式写 noreverse 并把背景设成 default 才能把那层底色摘掉，
        # 只留下文字本身的颜色 —— 这样横幅会融进主人自己的终端配色里，不突兀喵。
        style = Style.from_dict({
            # 工具栏本体：取消反显、背景跟随终端默认，文字用不刺眼的浅灰喵
            "bottom-toolbar": "noreverse bg:default #aaaaaa",
            # 警告标题用黄色加粗，最醒目喵
            "bottom-toolbar.warn": "noreverse bg:default #ffcc00 bold",
            # 一切正常时用绿色喵
            "bottom-toolbar.ok": "noreverse bg:default #55ff55",
            # 「虚拟模型/节点」用亮白，比周围文字重一点好读喵
            "bottom-toolbar.node": "noreverse bg:default #ffffff",
            # 倒计时用青色加粗，方便快速扫到还剩多久喵
            "bottom-toolbar.countdown": "noreverse bg:default #55ffff bold",
        })
        # 创建 Tab 补全器喵
        completer = self._build_completer()

        def bottom_toolbar():
            """每次刷新时被调用，返回当前的冻结横幅喵~"""
            # 喵~防御：渲染横幅时万一出错，绝不能让整个 REPL 崩掉，
            # 所以兜住异常并显示一行提示，主人还能继续敲命令喵
            try:
                # 把样式片段里的 class 前缀补全成工具栏专用的，prompt_toolkit 才认喵
                return [
                    # 空样式保持原样，带 class: 的补成 class:bottom-toolbar.xxx 喵
                    (style_str.replace("class:", "class:bottom-toolbar.") if style_str else "", text)
                    # 遍历渲染出来的每个片段喵
                    for style_str, text in render_freeze_banner(self.state)
                ]
            # 喵~防御：出错时降级成一行纯文本提示，不影响输入喵
            except Exception as exc:  # noqa: BLE001
                return [("", f"冻结表渲染出错喵：{type(exc).__name__}")]

        # 组装会话喵
        return PromptSession(
            # 命令历史，上下键可以翻之前敲过的命令喵
            history=InMemoryHistory(),
            # Tab 补全喵
            completer=completer,
            # 底部常驻横幅喵
            bottom_toolbar=bottom_toolbar,
            # 配色喵
            style=style,
            # 每秒重绘一次，这是倒计时能自己走动的关键喵
            refresh_interval=1.0,
        )

    def run(self) -> None:
        """
        REPL 主循环，在独立线程里跑喵~

        边界条件：
            EOFError   主人按了 Ctrl+D，或者进程的 stdin 被重定向到 /dev/null
                       （用 nohup 后台跑时就是这样），此时安静退出 REPL 循环，
                       但不结束进程 —— 代理还得继续服务喵
            KeyboardInterrupt 主人按了 Ctrl+C，提示用 quit 退出而不是直接杀掉喵
            其他异常  打印出来但不让 REPL 线程死掉，免得一个手滑的命令就没法交互了喵
        """
        # 打印欢迎语和提示喵
        print("\nautoapi 交互式命令行就绪喵~ 敲 help 看命令，敲 quit 退出~")
        # 会话对象，拿不到就退回朴素模式喵
        session = None
        # 尝试创建 prompt_toolkit 会话喵
        try:
            session = self._build_session()
        # 喵~防御：prompt_toolkit 没装、或者 stdin 不是终端（管道、nohup）时都会失败。
        # 这时候退回内置 input()，功能照常只是没有常驻横幅，绝不能因此让 REPL 用不了喵
        except Exception as exc:  # noqa: BLE001
            print(f"（常驻冻结表不可用，退回朴素模式喵：{type(exc).__name__}）")
        # 提示怎么看冻结表喵
        print("（底部会常驻显示冻结表，每秒自动更新喵~）\n" if session else "")
        # 拿到「输出重定向」的上下文管理器喵。
        #
        # 这是修「有日志滚动时下方状态监控会上移然后渲染错误」那个 bug 的另一半喵：
        #   代理的日志是在主线程（uvicorn 那边）打的，底部横幅是 REPL 线程画的。
        #   两边都往同一个终端写，谁也不知道对方写了什么 —— 日志一滚，
        #   prompt_toolkit 记着的「光标现在在第几行」就和实际情况脱节了，
        #   于是横幅位置乱跑、字符叠在一起。
        #
        #   patch_stdout 的做法是把 sys.stdout / sys.stderr 换成代理对象，
        #   所有输出都排队交给 prompt_toolkit，由它负责「先擦掉横幅 → 打这行输出 →
        #   在新的位置重画横幅」。这样两边就不再抢终端了喵。
        #
        # raw=True 是必须的：日志里带着 ANSI 颜色码（WARNING 黄、ERROR 红），
        # 非 raw 模式会把转义码当普通文本处理，颜色就没了喵。
        redirect = self._build_stdout_patch() if session else None
        # 喵~防御：拿不到重定向器也照常跑，只是日志和横幅可能互相干扰，
        # 总比因为一个显示问题就没法用交互命令行要好喵
        if redirect is None:
            # 用一个什么都不做的上下文管理器占位，让下面的 with 写法保持统一喵
            from contextlib import nullcontext
            redirect = nullcontext()
        # 在重定向生效的范围内跑主循环喵
        with redirect:
            # 真正的读命令循环喵
            self._loop(session)

    def _build_stdout_patch(self):
        """
        构造 prompt_toolkit 的输出重定向上下文喵~

        输出：patch_stdout 上下文管理器；prompt_toolkit 不可用时返回 None，
             由调用方退回到「不重定向」的模式喵。
        """
        # 喵~防御：在方法内导入，这样 prompt_toolkit 没装时不影响模块导入喵
        try:
            from prompt_toolkit.patch_stdout import patch_stdout
        # 拿不到就返回 None，让调用方走无重定向的路喵
        except Exception:  # noqa: BLE001
            return None
        # raw=True 让 ANSI 颜色码原样透传，日志的黄色红色才不会被吃掉喵
        return patch_stdout(raw=True)

    def _loop(self, session) -> None:
        """
        读命令并执行的主循环喵~

        单独拆出来是为了让 run() 那边能干净地用 with 把它包在输出重定向里，
        不用把整个循环缩进一层喵。
        """
        # 一直循环读命令，直到收到退出信号喵
        while not self.should_exit.is_set():
            # 读一行输入，各种异常都要接住喵
            try:
                # 有会话就用带横幅的输入，否则退回内置 input 喵
                line = session.prompt("autoapi> ") if session else input("autoapi> ")
            # 喵~防御：stdin 关闭（Ctrl+D 或后台运行）时退出 REPL 但保留代理服务喵
            except EOFError:
                print("\n检测到 stdin 已关闭，交互式命令行退出，代理继续在后台服务喵~")
                return
            # 喵~防御：Ctrl+C 时不直接杀进程，提示用 quit 优雅退出喵
            except KeyboardInterrupt:
                print("\n想退出的话敲 quit 喵~")
                continue
            # 喵~防御：prompt_toolkit 在某些终端下可能抛别的异常，
            # 这时候降级成朴素模式继续服务，而不是让 REPL 线程死掉喵
            except Exception as exc:  # noqa: BLE001
                print(f"（输入出错，切回朴素模式喵：{type(exc).__name__}）")
                session = None
                continue
            # 执行这条命令，异常已经由 dispatch 自己兜住，这里不用再包 try 喵
            self.dispatch(line)


def start_repl_thread(state: RuntimeState) -> Repl:
    """
    在独立的 daemon 线程里启动 REPL 喵~

    输入：运行时状态
    输出：Repl 对象，主线程可以通过它的 should_exit 事件知道主人是否敲了 quit
    说明：设成 daemon 线程，这样主进程退出时不会被这个线程卡住喵。
    """
    # 创建 REPL 对象喵
    repl = Repl(state)
    # 创建线程，target 是 REPL 主循环喵
    thread = threading.Thread(
        # 线程要跑的函数喵
        target=repl.run,
        # 线程名字，出问题时在 traceback 里能看出是哪个线程喵
        name="autoapi-repl",
        # 设为 daemon，主进程退出时不等它喵
        daemon=True,
    )
    # 启动线程喵
    thread.start()
    # 把 REPL 对象返回给主线程喵
    return repl
