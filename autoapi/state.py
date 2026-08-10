"""
运行时状态模块喵~

整体思路：
    代理跑起来之后有三样东西会随时变：当前生效的配置、候选的冻结倒计时、每个候选的
    成败统计。它们同时被两个线程访问 —— FastAPI 所在的事件循环线程、以及 REPL 所在的
    独立线程。所以全部收进 RuntimeState 这一个对象里，用一把普通的 threading.Lock 保护喵。

为什么用 threading.Lock 而不是 asyncio.Lock：
    这里所有临界区都是纯内存的字典读写，不含任何 await，执行时间是微秒级。用普通锁
    能让 REPL 线程直接调用同一批方法，不必绕 run_coroutine_threadsafe 投递协程，
    代码简单得多；而因为临界区内不会挂起，也绝不会阻塞事件循环喵。

边界条件：冻结时间到了要自动失效（惰性判断，不起后台清理任务）；候选一旦成功一次就
        立即解冻，因为成功证明它已经恢复了喵。
"""

# 引入注解特性，允许类型注解里写 X | None 喵
from __future__ import annotations

# threading 提供跨线程互斥锁喵
import threading
# time 用来取单调时钟，算冻结倒计时喵
import time
# dataclass 用来定义统计数据的小容器喵
from dataclasses import dataclass, field

# 从配置模块引入候选和配置类型喵
from .config import AppConfig, Candidate

# RPM 和 TPM 固定只统计最近 60 秒的成功请求喵~
RATE_WINDOW_SECONDS = 60.0


@dataclass
class CandidateStats:
    """单个候选的累计统计，用于 REPL 的 stats 命令喵~"""

    # 该候选成功返回给客户端的次数喵
    success: int = 0
    # 该候选失败的次数（含被规则判定为失败的所有情况）喵
    failure: int = 0
    # 该候选被冻结过的次数喵
    frozen_times: int = 0
    # 最近一次失败的简短原因，方便排查喵
    last_error: str = ""
    # 当前连续失败了多少次。成功一次就清零，达到自动避险阈值就冻结这个节点喵
    consecutive_failures: int = 0
    # 因为连续失败而被自动避险的次数喵
    hedged_times: int = 0


@dataclass
class RequestMetric:
    """一条成功请求的统计记录，速率与平均耗时使用各自窗口喵~"""

    # 成功被确认的单调时钟时刻，单位：秒喵
    at: float
    # 上游明确上报的 token 总数；None 表示上游没给 usage，绝不本地估算喵
    usage_tokens: int | None = None
    # 完整请求实际结束后的耗时，单位：毫秒；None 表示流尚未结束，不能计入平均值喵
    elapsed_ms: float | None = None


@dataclass
class VirtualModelRate:
    """某个虚拟模型近 60 秒的 RPM/TPM 快照喵~"""

    # 虚拟模型名喵
    virtual_model: str
    # 滚动窗口内的成功请求数，也就是 RPM 喵
    rpm: int
    # 有明确 usage 的请求数喵
    usage_reported_requests: int
    # 只累计上游明确给出的 token，总数未知时为 None 喵
    tpm: int | None
    # 窗口内已完整结束的请求数量，用于平均耗时分母喵
    completed_requests: int
    # 窗口内已完成请求的平均完整耗时，单位：毫秒；没有完成请求时为 None 喵
    average_elapsed_ms: float | None


@dataclass
class FreezeInfo:
    """一条冻结记录喵~"""

    # 被冻结的候选的可读标签，用于展示喵
    label: str
    # 冻结到期的单调时钟时刻，单位：秒喵
    until: float
    # 冻结原因，通常是上游返回的原始错误摘要喵
    reason: str


class RuntimeState:
    """代理的全部可变状态，线程安全喵~"""

    def __init__(self, config: AppConfig) -> None:
        """用一份初始配置创建状态对象喵~"""
        # 保护下面所有字段的互斥锁喵
        self._lock = threading.Lock()
        # 当前生效的配置对象，热重载时整体替换喵
        self._config = config
        # 冻结表：候选身份串 → 冻结记录喵
        self._freezes: dict[str, FreezeInfo] = {}
        # 统计表：候选身份串 → 统计数据喵
        self._stats: dict[str, CandidateStats] = {}
        # 虚拟模型动态负载：虚拟模型名 → 当前统计窗口内的成功请求记录喵
        self._rate_events: dict[str, list[RequestMetric]] = {}
        # 代理累计处理的客户端请求数喵
        self.total_requests = 0
        # 代理累计彻底失败（整条候选链都用尽）的请求数喵
        self.total_exhausted = 0
        # 目标模式开关：只存在内存里，重启后默认关闭，绝不写入 config.yaml 喵
        self._target_mode_enabled = False

    # ---------- 配置相关喵 ----------

    @property
    def config(self) -> AppConfig:
        """读取当前生效的配置喵~"""
        # 加锁读取，保证不会读到热重载写入一半的中间状态喵
        with self._lock:
            return self._config

    def replace_config(self, new_config: AppConfig) -> None:
        """整体替换配置，用于热重载和 REPL 改规则喵~"""
        # 加锁做整体替换，读方要么看到旧的要么看到新的，不存在撕裂喵
        with self._lock:
            self._config = new_config

    def get_chain(self, virtual_model: str) -> list[Candidate] | None:
        """按虚拟模型名取候选链，取不到返回 None 喵~"""
        # 加锁读取虚拟模型表喵
        with self._lock:
            return self._config.virtual_models.get(virtual_model)

    def list_virtual_models(self) -> list[str]:
        """列出所有可用的虚拟模型名，用于 400 错误提示和 /v1/models 接口喵~"""
        # 加锁取出所有 key 并转成列表返回喵
        with self._lock:
            return list(self._config.virtual_models.keys())

    # ---------- 目标模式相关喵 ----------

    @property
    def target_mode_enabled(self) -> bool:
        """读取目标模式当前是否开启喵~"""
        # 加锁读取运行期临时状态喵
        with self._lock:
            return self._target_mode_enabled

    def set_target_mode(self, enabled: bool) -> bool:
        """
        开启或关闭目标模式，返回变更后的状态喵~

        这是纯内存状态：不写入配置文件，也不随热重载变化。这样主人可以临时保住
        正在跑的客户端请求，又不会因为重启后忘记关掉而意外占住 5 分钟长连接喵。
        """
        # 加锁更新开关喵
        with self._lock:
            # 写入标准布尔值喵
            self._target_mode_enabled = bool(enabled)
            # 返回现在的状态喵
            return self._target_mode_enabled

    # ---------- 冻结相关喵 ----------

    def is_frozen(self, candidate: Candidate) -> float:
        """
        查询候选是否还在冻结中喵~

        返回值：剩余冻结秒数，返回 0 表示没被冻结、可以正常使用喵。
        采用惰性过期：发现已到期就顺手把记录删掉，不需要额外的清理定时任务喵。
        """
        # 加锁访问冻结表喵
        with self._lock:
            # 取出这个候选的冻结记录，没有就说明没冻过喵
            info = self._freezes.get(candidate.identity)
            # 没有记录直接返回 0 表示可用喵
            if info is None:
                return 0.0
            # 算出还剩多少秒，用单调时钟避免系统时间被改动时算错喵
            remaining = info.until - time.monotonic()
            # 喵~防御：已经到期的记录顺手删掉并返回 0，防止冻结表无限膨胀喵
            if remaining <= 0:
                del self._freezes[candidate.identity]
                return 0.0
            # 还在冻结中，返回剩余秒数喵
            return remaining

    def freeze(self, candidate: Candidate, seconds: float, reason: str) -> None:
        """把候选冻结指定秒数喵~"""
        # 喵~防御：非正数时长视为无效冻结，直接忽略，避免写入一条立刻过期的垃圾记录喵
        if seconds <= 0:
            return
        # 加锁写入冻结表和统计表喵
        with self._lock:
            # 记录到期时刻、可读标签和原因喵
            self._freezes[candidate.identity] = FreezeInfo(
                # 候选的可读标签，展示用喵
                label=candidate.label,
                # 到期时刻 = 现在 + 冻结时长喵
                until=time.monotonic() + seconds,
                # 冻结原因，截断到 200 字符防止把整个上游响应体塞进内存喵
                reason=reason[:200],
            )
            # 顺手把这个候选的「被冻结次数」加一喵
            self._stats.setdefault(candidate.identity, CandidateStats()).frozen_times += 1

    def unfreeze(self, candidate: Candidate) -> None:
        """立即解冻某个候选，候选成功一次后调用，也供 REPL 手动解冻喵~"""
        # 加锁删除冻结记录，pop 带默认值所以不存在也不会报错喵
        with self._lock:
            self._freezes.pop(candidate.identity, None)

    def clear_freezes(self) -> int:
        """清空所有冻结记录，返回被清掉的条数，供 REPL 的 freeze clear 用喵~"""
        # 加锁清空整张冻结表喵
        with self._lock:
            # 先记下数量用于返回喵
            count = len(self._freezes)
            # 清空字典喵
            self._freezes.clear()
            # 返回清理条数喵
            return count

    def list_freezes(self) -> list[tuple[str, float, str]]:
        """
        列出当前所有仍在生效的冻结，供 REPL 的 freeze ls 用喵~

        返回值：[(候选标签, 剩余秒数, 冻结原因), ...]，已过期的会被顺手清理掉喵。
        """
        # 结果列表喵
        result: list[tuple[str, float, str]] = []
        # 加锁遍历冻结表喵
        with self._lock:
            # 当前单调时钟时刻，循环内复用，避免每次都取一遍喵
            now = time.monotonic()
            # 收集已过期的 key，遍历中不能直接删字典，所以先记下来喵
            expired: list[str] = []
            # 遍历所有冻结记录喵
            for identity, info in self._freezes.items():
                # 算剩余秒数喵
                remaining = info.until - now
                # 已过期的先记下 key，稍后统一删除喵
                if remaining <= 0:
                    expired.append(identity)
                # 还有效的收进结果列表喵
                else:
                    result.append((info.label, remaining, info.reason))
            # 统一清理过期记录喵
            for identity in expired:
                del self._freezes[identity]
        # 按剩余时间从短到长排序，快恢复的排前面喵
        result.sort(key=lambda row: row[1])
        # 返回结果喵
        return result

    def list_frozen_nodes(self) -> list[tuple[str, str, float]]:
        """
        列出当前所有被冻结的节点，带上它属于哪个虚拟模型喵~

        输出：[(虚拟模型名, 节点的真实 model 名, 剩余秒数), ...]，按剩余时间从短到长排序

        和 list_freezes 的区别：list_freezes 只认识「候选身份串」，拿不到虚拟模型名，
        因为冻结表是全局的、不记录候选属于谁。这个方法反过来做 —— 遍历当前配置里的
        所有虚拟模型和它们的节点，逐个查冻结状态。这样交互区的横幅才能按
        「虚拟模型id/节点model」的格式显示出来喵。

        为什么同一个节点可能出现多次：冻结是按 (地址, key, 模型) 三元组算的，
        如果两个虚拟模型共用同一个节点，那它在两个虚拟模型下都会被列出来 ——
        这是对的，因为主人关心的是「哪个虚拟模型受影响了」喵。
        """
        # 结果列表喵
        rows: list[tuple[str, str, float]] = []
        # 先取一份配置快照，避免遍历过程中被热重载换掉喵
        config = self.config
        # 遍历每个虚拟模型喵
        for vm_name, chain in config.virtual_models.items():
            # 遍历这个虚拟模型的每个节点喵
            for candidate in chain:
                # 查这个节点还剩多少秒冻结，0 表示可用喵
                remaining = self.is_frozen(candidate)
                # 只收集还在冻结中的喵
                if remaining > 0:
                    # 记下虚拟模型名、节点的真实模型名、剩余秒数喵
                    rows.append((vm_name, candidate.model, remaining))
        # 按剩余时间从短到长排序，快恢复的排前面喵
        rows.sort(key=lambda row: row[2])
        # 返回结果喵
        return rows

    # ---------- 动态 RPM / TPM 相关喵 ----------

    def record_rate_event(self, virtual_model: str, usage_tokens: int | None = None) -> RequestMetric:
        """
        记录一条成功请求，供滚动 60 秒 RPM/TPM 使用喵~

        token 只能传上游明确上报的 usage；没有就传 None。RPM 仍然照常加一，
        TPM 则保持「未知覆盖」状态，绝不能用字符数或本地 tokenizer 猜一个数喵。
        """
        # 创建这条事件，时间用单调时钟避免系统时间调整影响滚动窗口喵
        event = RequestMetric(at=time.monotonic(), usage_tokens=usage_tokens)
        # 加锁写入该虚拟模型的事件列表喵
        with self._lock:
            # 取出列表，不存在就新建喵
            events = self._rate_events.setdefault(virtual_model, [])
            # 追加成功事件喵
            events.append(event)
            # 顺手清理过期记录，避免高流量时列表无限长喵
            self._prune_rate_events_locked(time.monotonic())
        # 把事件对象返回，流式在结束后可以补写 usage 喵
        return event

    def attach_usage_tokens(self, event: RequestMetric, usage_tokens: int | None) -> None:
        """
        给已经记进 RPM 的流式请求补上最终的上游 usage token 数喵~

        流式 usage 常出现在尾包，放行时还不知道，必须等流真正结束后才补写。
        usage 缺失时保持 None，不做任何本地估算喵。
        """
        # 没有上游 usage 时不动原记录喵
        if usage_tokens is None:
            return
        # 加锁更新事件，保证 REPL 同时读窗口时不会看到半写状态喵
        with self._lock:
            # 写入上游明确给出的 token 总数喵
            event.usage_tokens = usage_tokens

    def attach_elapsed_ms(self, event: RequestMetric, elapsed_ms: float) -> None:
        """给成功事件补上完整请求结束后的耗时，供窗口平均值使用喵~"""
        # 喵~防御：非正耗时说明调用方的计时出了问题，直接忽略避免污染平均值喵
        if elapsed_ms < 0:
            return
        # 加锁写入，避免 REPL 同时读横幅时看到撕裂状态喵
        with self._lock:
            # 记录完整生命周期耗时喵
            event.elapsed_ms = elapsed_ms

    def _prune_rate_events_locked(self, now: float) -> None:
        """在已持锁状态下删掉两个统计窗口都不再需要的请求记录喵~"""
        # 计算平均耗时配置窗口的秒数，单位：秒喵
        elapsed_window_seconds = self._config.server.metrics_window_minutes * 60.0
        # 保留较长窗口，避免清理掉仍需参与平均耗时计算的事件喵
        retention_seconds = max(RATE_WINDOW_SECONDS, elapsed_window_seconds)
        # 计算统一事件列表的清理边界，单位：单调时钟秒喵
        cutoff = now - retention_seconds
        # 逐个虚拟模型清理喵
        for virtual_model, events in list(self._rate_events.items()):
            # 保留两个统计窗口中任意一个仍可能使用的记录喵
            kept = [event for event in events if event.at >= cutoff]
            # 还有记录就替换回去喵
            if kept:
                self._rate_events[virtual_model] = kept
            # 一条都没有就删掉这个 key，保持状态表干净喵
            else:
                del self._rate_events[virtual_model]

    def snapshot_virtual_model_rates(self) -> list[VirtualModelRate]:
        """
        返回所有虚拟模型的速率与平均耗时快照喵~

        RPM/TPM 固定统计最近 60 秒；平均耗时使用配置文件中的性能统计窗口喵。
        TPM 只在速率窗口内每一条成功请求都收到上游 usage 时才显示数值；只要有一条没有，
        就返回 None，让横幅明确写「TPM 未完整上报」，避免总数看起来像是准确的喵。
        """
        # 加锁做清理和汇总喵
        with self._lock:
            # 取一次现在，整次快照统一用它做窗口边界喵
            now = time.monotonic()
            # 先清掉两个统计窗口都不再需要的记录喵
            self._prune_rate_events_locked(now)
            # 计算 RPM/TPM 固定 60 秒窗口边界喵
            rate_cutoff = now - RATE_WINDOW_SECONDS
            # 计算平均耗时配置窗口边界喵
            elapsed_cutoff = now - (self._config.server.metrics_window_minutes * 60.0)
            # 按当前配置顺序遍历，这样横幅顺序稳定喵
            rows: list[VirtualModelRate] = []
            for virtual_model in self._config.virtual_models:
                # 没有记录时用空列表喵
                events = self._rate_events.get(virtual_model, [])
                # 只保留最近 60 秒内的事件用于 RPM/TPM 喵
                rate_events = [event for event in events if event.at >= rate_cutoff]
                # 只保留配置窗口内且已结束的事件用于平均耗时喵
                elapsed_events = [
                    event for event in events
                    if event.at >= elapsed_cutoff and event.elapsed_ms is not None
                ]
                # 统计固定 60 秒窗口内的成功请求数喵
                rpm = len(rate_events)
                # 只收集固定 60 秒窗口内上游明确上报 usage 的记录喵
                known = [event.usage_tokens for event in rate_events if event.usage_tokens is not None]
                # 只有速率窗口内的请求都上报 usage 时，TPM 才可作为完整数字展示喵
                tpm = sum(known) if rpm > 0 and len(known) == rpm else None
                # 提取配置窗口内已经完整结束请求的耗时喵
                completed = [event.elapsed_ms for event in elapsed_events]
                # 结束请求越多，平均值越稳定；没有完成请求时保持未知喵
                average_elapsed_ms = sum(completed) / len(completed) if completed else None
                # 组装这一行快照喵
                rows.append(
                    VirtualModelRate(
                        virtual_model=virtual_model,
                        rpm=rpm,
                        usage_reported_requests=len(known),
                        tpm=tpm,
                        completed_requests=len(completed),
                        average_elapsed_ms=average_elapsed_ms,
                    )
                )
        # 返回拷贝出来的快照，REPL 在锁外慢慢渲染喵
        return rows

    # ---------- 统计相关喵 ----------

    def record_success(self, candidate: Candidate) -> None:
        """记录一次成功，顺手解冻这个候选并清零它的连续失败计数喵~"""
        # 加锁更新统计并删除冻结记录喵
        with self._lock:
            # 取出（或新建）该候选的统计对象喵
            stats = self._stats.setdefault(candidate.identity, CandidateStats())
            # 成功次数加一喵
            stats.success += 1
            # 连续失败计数清零 —— 这是「连续」的含义所在：中间只要成功过一次，
            # 之前攒的失败次数就不该再算进自动避险的账上喵
            stats.consecutive_failures = 0
            # 既然成功了就说明它已经恢复，立刻解冻，不用等倒计时走完喵
            self._freezes.pop(candidate.identity, None)

    def record_failure(self, candidate: Candidate, error: str, hedge_threshold: int = 0) -> int:
        """
        记录一次失败，并判断是否该触发自动避险喵~

        输入：
            candidate       失败的候选
            error           失败原因
            hedge_threshold 自动避险阈值，连续失败达到这个次数就该冻结它。传 0 表示关闭
        输出：达到阈值时返回当前的连续失败次数（调用方据此去冻结并写日志）；
             没达到就返回 0

        为什么「加一」和「判断是否达标」必须在同一个锁里做：
            并发时多条请求可能同时失败。如果先加一、解锁、再另外读一次来判断，
            两条请求可能都读到「刚好等于阈值」，于是重复触发两次避险、日志刷两遍。
            在锁内一次性完成「加一 + 判断 + 清零」才能保证阈值只被跨过一次喵。

        为什么达标后要把计数清零：
            清零之后需要再攒满一轮才会再次触发。否则节点被冻结期间如果还有请求漏进来
            （比如冻结刚过期那一刻），会立刻又触发一次避险，冻结时间被无意义地延长喵。
        """
        # 加锁更新统计并判断喵
        with self._lock:
            # 取出（或新建）该候选的统计对象喵
            stats = self._stats.setdefault(candidate.identity, CandidateStats())
            # 失败次数加一喵
            stats.failure += 1
            # 连续失败次数也加一喵
            stats.consecutive_failures += 1
            # 记下最近一次失败原因，截断到 200 字符防止内存膨胀喵
            stats.last_error = error[:200]
            # 阈值为 0 表示关闭自动避险，直接返回不触发喵
            if hedge_threshold <= 0:
                return 0
            # 还没攒够就不触发喵
            if stats.consecutive_failures < hedge_threshold:
                return 0
            # 达到阈值了，记下当前次数用于返回和日志喵
            reached = stats.consecutive_failures
            # 自动避险次数加一，供 stats 命令展示喵
            stats.hedged_times += 1
            # 计数清零，这样需要再攒满一轮才会再次触发喵
            stats.consecutive_failures = 0
            # 返回触发时的连续失败次数喵
            return reached

    def snapshot_stats(self) -> dict[str, CandidateStats]:
        """拷贝一份统计快照给 REPL 打印，避免 REPL 长时间持锁喵~"""
        # 加锁做浅拷贝，CandidateStats 本身是只读展示用，浅拷贝够了喵
        with self._lock:
            return dict(self._stats)

