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
# dataclass 用来定义统计数据的小容器喵
from dataclasses import dataclass
# time 用来取墙上时间和单调时钟喵
import time

# 从配置模块引入候选和配置类型喵
from .config import AppConfig, Candidate

# RPM 和 TPM 固定只统计最近 60 秒的成功请求喵~
RATE_WINDOW_SECONDS = 60.0
# 新版 stats 最多保留最近 24 小时的历史事件喵
HEALTH_HISTORY_SECONDS = 24 * 60 * 60.0
# 可用率历史条每格代表 10 分钟喵
HEALTH_BUCKET_SECONDS = 10 * 60.0
# 新版 stats 的滚动窗口，名称和秒数保持稳定，方便 REPL 与测试复用喵
HEALTH_WINDOWS_SECONDS = (
    ("近10分钟", 10 * 60.0),
    ("近30分钟", 30 * 60.0),
    ("近1小时", 60 * 60.0),
    ("近6小时", 6 * 60 * 60.0),
)


@dataclass
class HealthEvent:
    """候选尝试或虚拟模型最终请求的一条健康事件喵~"""

    # 单调时钟时间，用于窗口计算，单位：秒喵
    at: float
    # 墙上时钟时间，用于最近错误展示，单位：Unix 秒喵
    wall_time: float
    # 这次事件是否成功喵
    success: bool
    # 上游明确上报的 token 总数；缺失时新 stats 按 0 展示喵
    usage_tokens: int = 0
    # 上游明确上报的输入 Token，缺失时为 None 喵
    input_tokens: int | None = None
    # 上游明确上报的缓存读取 Token，缺失时为 None 喵
    cached_tokens: int | None = None
    # 完整请求耗时，单位：毫秒；失败或未完整结束时为空喵
    elapsed_ms: float | None = None


@dataclass(frozen=True)
class HealthWindow:
    """一个时间窗口里的成功、请求、Token 和耗时快照喵~"""

    # 窗口成功请求数喵
    success: int
    # 窗口总请求数喵
    total: int
    # 窗口 Token 总量，缺失 usage 已按 0 计入喵
    tokens: int
    # 窗口内完成请求的平均耗时，单位：毫秒喵
    average_elapsed_ms: float | None
    # 加权平均缓存命中率；没有明确缓存字段或有效输入 Token 时为 None 喵
    average_cache_hit_rate: float | None = None


@dataclass(frozen=True)
class FreezeInterval:
    """候选历史冻结区间喵~"""

    # 冻结开始的单调时刻喵
    started_at: float
    # 冻结结束的单调时刻喵
    ended_at: float


@dataclass(frozen=True)
class HealthBucket:
    """十分钟历史条的一格快照喵~"""

    # 这格成功请求数喵
    success: int
    # 这格总请求数喵
    total: int
    # 查询快照时是否仍被冻结喵
    frozen: bool


@dataclass(frozen=True)
class HealthSnapshot:
    """某个候选或虚拟模型的完整 stats 快照喵~"""

    # 所有时间窗口喵
    all_time: HealthWindow
    # 固定名称的滚动窗口，键为近15分钟等中文标签喵
    windows: dict[str, HealthWindow]
    # 从旧到新的 24 小时十分钟历史格喵
    buckets: list[HealthBucket]


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
    # 最近一次失败发生的墙上时钟时间，单位：Unix 秒喵
    last_error_at: float | None = None
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
    # 上游明确上报的输入 Token，用于缓存命中率加权分母喵
    input_tokens: int | None = None
    # 上游明确上报的缓存读取 Token，None 表示该字段未上报喵
    cached_tokens: int | None = None
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
    # 近 60 秒输入 Token 加权的缓存命中率喵
    average_cache_hit_rate: float | None = None


@dataclass
class FreezeInfo:
    """一条冻结记录喵~"""

    # 被冻结的候选的可读标签，用于展示喵
    label: str
    # 冻结到期的单调时钟时刻，单位：秒喵
    until: float
    # 本次冻结开始的单调时刻，单位：秒喵
    started_at: float
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
        # 冻结历史：候选身份串 → 曾经发生过的冻结区间喵
        self._freeze_intervals: dict[str, list[FreezeInterval]] = {}
        # 统计表：候选身份串 → 统计数据喵
        self._stats: dict[str, CandidateStats] = {}
        # 虚拟模型动态负载：虚拟模型名 → 当前统计窗口内的成功请求记录喵
        self._rate_events: dict[str, list[RequestMetric]] = {}
        # 候选健康历史：候选身份串 → 最近 24 小时的成功/失败尝试喵
        self._candidate_health_events: dict[str, list[HealthEvent]] = {}
        # 候选累计资源统计：候选身份串 → (成功数、总数、Token数、耗时总和、完成数、缓存输入数、缓存读取数)喵
        self._candidate_health_totals: dict[str, list[float]] = {}
        # 虚拟模型健康历史：虚拟模型名 → 最近 24 小时的最终请求结果喵
        self._virtual_health_events: dict[str, list[HealthEvent]] = {}
        # 虚拟模型累计统计：虚拟模型名 → (成功数、总数、Token数、耗时总和、完成数、缓存输入数、缓存读取数)喵
        self._virtual_health_totals: dict[str, list[float]] = {}
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

    def _record_freeze_interval_locked(self, identity: str, info: FreezeInfo, ended_at: float) -> None:
        """在持锁状态下保存实际结束的冻结区间，供历史条回看喵~"""
        # 冻结实际结束不能晚于原计划到期时间喵
        actual_end = min(max(info.started_at, ended_at), info.until)
        # 非正长度区间没有可展示价值，直接忽略喵
        if actual_end <= info.started_at:
            return
        # 追加本次实际冻结区间喵
        self._freeze_intervals.setdefault(identity, []).append(FreezeInterval(info.started_at, actual_end))
        # 只保留 24 小时历史可能用到的区间，防止长期运行无限增长喵
        cutoff = time.monotonic() - HEALTH_HISTORY_SECONDS
        self._freeze_intervals[identity] = [
            interval for interval in self._freeze_intervals[identity] if interval.ended_at >= cutoff
        ]

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
            # 将浮点误差压回纳秒级精度，避免刚冻结时出现 300.00000000000006 这种超出原时长的显示喵
            remaining = round(remaining, 9)
            # 喵~防御：已经到期的记录顺手删掉并返回 0，防止冻结表无限膨胀喵
            if remaining <= 0:
                # 记录自然到期的冻结区间，供历史条显示青色空闲格喵
                self._record_freeze_interval_locked(candidate.identity, info, info.until)
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
            # 冻结开始时间，避免同一条记录里多次取单调时钟造成边界不一致喵
            frozen_started_at = time.monotonic()
            # 已经存在冻结时先归档旧区间，避免重复冻结覆盖历史喵
            previous_info = self._freezes.get(candidate.identity)
            if previous_info is not None:
                self._record_freeze_interval_locked(candidate.identity, previous_info, frozen_started_at)
            # 记录到期时刻、可读标签和原因喵
            self._freezes[candidate.identity] = FreezeInfo(
                # 候选的可读标签，展示用喵
                label=candidate.label,
                # 到期时刻 = 现在 + 冻结时长喵
                until=frozen_started_at + seconds,
                # 记录冻结开始时刻，供十分钟历史格判断相交范围喵
                started_at=frozen_started_at,
                # 冻结原因，截断到 200 字符防止把整个上游响应体塞进内存喵
                reason=reason[:200],
            )
            # 顺手把这个候选的「被冻结次数」加一喵
            self._stats.setdefault(candidate.identity, CandidateStats()).frozen_times += 1

    def unfreeze(self, candidate: Candidate) -> None:
        """立即解冻某个候选，候选成功一次后调用，也供 REPL 手动解冻喵~"""
        # 加锁删除冻结记录，pop 带默认值所以不存在也不会报错喵
        with self._lock:
            # 取出并删除当前冻结记录喵
            info = self._freezes.pop(candidate.identity, None)
            # 有实际冻结记录时保留到当前为止的历史区间喵
            if info is not None:
                self._record_freeze_interval_locked(candidate.identity, info, time.monotonic())

    def clear_freezes(self) -> int:
        """清空所有冻结记录，返回被清掉的条数，供 REPL 的 freeze clear 用喵~"""
        # 加锁清空整张冻结表喵
        with self._lock:
            # 先记下数量用于返回喵
            count = len(self._freezes)
            # 逐条归档当前冻结区间，避免 freeze clear 丢失历史图信息喵
            clear_time = time.monotonic()
            for identity, info in self._freezes.items():
                self._record_freeze_interval_locked(identity, info, clear_time)
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
            # 统一清理过期记录并归档冻结区间喵
            for identity in expired:
                info = self._freezes.pop(identity)
                self._record_freeze_interval_locked(identity, info, info.until)
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

    # ---------- 健康历史相关喵 ----------

    def _prune_health_events_locked(self, now: float) -> None:
        """在持锁状态下清理超过 24 小时的健康历史喵~"""
        # 计算 24 小时历史的最早保留时刻喵
        cutoff = now - HEALTH_HISTORY_SECONDS
        # 逐个清理候选和虚拟模型事件喵
        for event_map in (self._candidate_health_events, self._virtual_health_events):
            # 遍历副本，允许删除已经没有事件的键喵
            for event_key, events in list(event_map.items()):
                # 只保留历史窗口内的事件喵
                kept_events = [event for event in events if event.at >= cutoff]
                # 有事件就写回清理后的列表喵
                if kept_events:
                    event_map[event_key] = kept_events
                # 没有事件就删除键，避免空表持续增长喵
                else:
                    del event_map[event_key]

    def record_candidate_health(
        self,
        candidate: Candidate,
        success: bool,
        usage_tokens: int | None = None,
        elapsed_ms: float | None = None,
        input_tokens: int | None = None,
        cached_tokens: int | None = None,
        at: float | None = None,
        wall_time: float | None = None,
    ) -> None:
        """记录一次候选实际尝试的终态，供 stats 画出上游资源历史喵~"""
        # 喵~防御：只接受非负整数 Token，避免异常上游数据污染聚合结果喵
        valid_usage_tokens = usage_tokens if isinstance(usage_tokens, int) and not isinstance(usage_tokens, bool) and usage_tokens >= 0 else 0
        # 喵~防御：输入 Token 非法时不参与缓存命中率分母喵
        valid_input_tokens = input_tokens if isinstance(input_tokens, int) and not isinstance(input_tokens, bool) and input_tokens >= 0 else None
        # 喵~防御：缓存 Token 非法时视为未上报，不伪造命中率喵
        valid_cached_tokens = cached_tokens if isinstance(cached_tokens, int) and not isinstance(cached_tokens, bool) and cached_tokens >= 0 else None
        # 使用调用方传入的上游事件时刻，缺失时才回退到当前时钟喵
        event_at = at if isinstance(at, (int, float)) else time.monotonic()
        # 使用调用方传入的墙上时间，缺失时才回退到当前时间喵
        event_wall_time = wall_time if isinstance(wall_time, (int, float)) else time.time()
        # 组装一条候选尝试事件喵
        event = HealthEvent(
            at=float(event_at),
            wall_time=float(event_wall_time),
            success=bool(success),
            usage_tokens=valid_usage_tokens,
            input_tokens=valid_input_tokens,
            cached_tokens=valid_cached_tokens,
            elapsed_ms=max(0.0, float(elapsed_ms)) if isinstance(elapsed_ms, (int, float)) and not isinstance(elapsed_ms, bool) else None,
        )
        # 加锁追加健康历史、更新累计资源统计并清理过期事件喵
        with self._lock:
            # 候选身份作为健康历史的稳定键喵
            events = self._candidate_health_events.setdefault(candidate.identity, [])
            # 追加一条候选实际尝试事件喵
            events.append(event)
            # 累计字段依次为成功数、总数、Token数、耗时总和、完成数、缓存输入数、缓存读取数喵
            totals = self._candidate_health_totals.setdefault(candidate.identity, [0.0] * 7)
            # 成功数按布尔值累计喵
            totals[0] += int(event.success)
            # 每次真实上游尝试都计入总数喵
            totals[1] += 1
            # 缺失 usage 按零累计，保持与 stats 展示约定一致喵
            totals[2] += event.usage_tokens
            # 只有完整结束的尝试才进入平均耗时分母喵
            if event.elapsed_ms is not None:
                totals[3] += event.elapsed_ms
                totals[4] += 1
            # 只有同时明确上报输入和缓存字段且输入大于零才进入缓存加权统计喵
            if event.input_tokens is not None and event.input_tokens > 0 and event.cached_tokens is not None:
                totals[5] += event.input_tokens
                totals[6] += event.cached_tokens
            # 顺手清理 24 小时之外的历史喵
            self._prune_health_events_locked(event.at)

    def record_virtual_model_health(self, virtual_model: str, success: bool, usage_tokens: int | None = None, elapsed_ms: float | None = None, input_tokens: int | None = None, cached_tokens: int | None = None) -> None:
        """记录一次虚拟模型最终请求结果，供 stats 按客户端请求统计喵~"""
        # 喵~防御：空虚拟模型名不写入统计，避免产生无法展示的垃圾分组喵
        if not isinstance(virtual_model, str) or not virtual_model.strip():
            return
        # 喵~防御：非法 usage 按未上报处理，避免字符串转换异常打断请求喵
        valid_usage_tokens = usage_tokens if isinstance(usage_tokens, int) and not isinstance(usage_tokens, bool) and usage_tokens >= 0 else 0
        # 喵~防御：非法输入 Token 不参与缓存命中率分母喵
        valid_input_tokens = input_tokens if isinstance(input_tokens, int) and not isinstance(input_tokens, bool) and input_tokens >= 0 else None
        # 喵~防御：非法缓存 Token 按未上报处理，避免伪造命中率喵
        valid_cached_tokens = cached_tokens if isinstance(cached_tokens, int) and not isinstance(cached_tokens, bool) and cached_tokens >= 0 else None
        # 喵~防御：非法耗时不进入平均耗时分母喵
        valid_elapsed_ms = max(0.0, float(elapsed_ms)) if isinstance(elapsed_ms, (int, float)) and not isinstance(elapsed_ms, bool) else None
        # 取当前单调时钟和墙上时间，分别服务窗口计算和调试展示喵
        event = HealthEvent(
            at=time.monotonic(),
            wall_time=time.time(),
            success=bool(success),
            usage_tokens=valid_usage_tokens,
            # 虚拟模型事件明确保存输入与缓存 Token 喵
            input_tokens=valid_input_tokens,
            cached_tokens=valid_cached_tokens,
            # 完整耗时使用已经校验过的数值喵
            elapsed_ms=valid_elapsed_ms,
        )
        # 加锁追加虚拟模型健康事件并更新累计总计喵
        with self._lock:
            # 去掉首尾空格，保证配置名和请求名统一喵
            model_name = virtual_model.strip()
            # 取得该模型的历史列表喵
            events = self._virtual_health_events.setdefault(model_name, [])
            # 追加一条最终请求结果喵
            events.append(event)
            # 累计字段按成功数、总数、Token数、耗时总和、完成数、缓存输入数、缓存读取数排列喵
            totals = self._virtual_health_totals.setdefault(model_name, [0.0] * 7)
            # 成功数按布尔值加一喵
            totals[0] += int(event.success)
            # 总请求数每次最终结果加一喵
            totals[1] += 1
            # 新版 stats 明确把缺失 usage 当作 0 喵
            totals[2] += event.usage_tokens
            # 只有有耗时的完整请求才加入平均值分子喵
            if event.elapsed_ms is not None:
                totals[3] += event.elapsed_ms
                totals[4] += 1
            # 只有明确输入和缓存字段且输入大于零时才累计永久缓存分子分母喵
            if event.input_tokens is not None and event.input_tokens > 0 and event.cached_tokens is not None:
                totals[5] += event.input_tokens
                totals[6] += event.cached_tokens
            # 清理 24 小时之前的滚动历史，但不影响累计 totals 喵
            self._prune_health_events_locked(event.at)

    def _health_snapshot_locked(self, events: list[HealthEvent], now: float, freeze_intervals: list[FreezeInterval] | None = None) -> HealthSnapshot:
        """在持锁状态下按窗口与十分钟格汇总健康事件喵~"""
        # 只使用当前仍在 24 小时历史范围内的事件喵
        recent_events = [event for event in events if event.at >= now - HEALTH_HISTORY_SECONDS]
        # 喵~防御：调用方没有冻结历史时按空列表处理，方便独立测试和复用喵
        freeze_intervals = freeze_intervals or []
        # 没有任何虚拟模型请求时，冻结区间不应把空白图染成青色喵
        if not recent_events:
            freeze_intervals = []
        # 汇总一个事件列表为窗口快照喵
        def summarize(selected_events: list[HealthEvent]) -> HealthWindow:
            # 统计成功事件数量喵
            success_count = sum(int(event.success) for event in selected_events)
            # 统计完整请求耗时列表喵
            elapsed_values = [event.elapsed_ms for event in selected_events if event.elapsed_ms is not None]
            # 计算平均耗时，没有完成事件时返回未知喵
            average_elapsed = sum(elapsed_values) / len(elapsed_values) if elapsed_values else None
            # 统计有明确缓存字段且输入 Token 有效的请求喵
            cache_events = [
                event for event in selected_events
                if event.input_tokens is not None and event.input_tokens > 0 and event.cached_tokens is not None
            ]
            # 按输入 Token 加权计算缓存命中率，未上报字段不进入分子与分母喵
            cache_input_total = sum(event.input_tokens for event in cache_events)
            cache_hit_rate = (
                sum(event.cached_tokens or 0 for event in cache_events) / cache_input_total
                if cache_input_total > 0
                else None
            )
            # 组装窗口统计，Token 缺失已经以 0 存储喵
            return HealthWindow(
                success_count,
                len(selected_events),
                sum(event.usage_tokens for event in selected_events),
                average_elapsed,
                cache_hit_rate,
            )
        # 计算所有时间窗口，这里指当前进程保留的 24 小时历史范围喵
        all_time = summarize(recent_events)
        # 按固定顺序建立各个滚动窗口喵
        windows = {
            window_name: summarize([event for event in recent_events if event.at >= now - seconds])
            for window_name, seconds in HEALTH_WINDOWS_SECONDS
        }
        # 计算最近 24 小时的 144 个十分钟历史格，从旧到新排列喵
        bucket_count = int(HEALTH_HISTORY_SECONDS / HEALTH_BUCKET_SECONDS)
        bucket_start = now - HEALTH_HISTORY_SECONDS
        buckets: list[HealthBucket] = []
        # 逐格汇总成功数、总数以及该格是否完全落在冻结区间喵
        for bucket_index in range(bucket_count):
            start = bucket_start + bucket_index * HEALTH_BUCKET_SECONDS
            end = start + HEALTH_BUCKET_SECONDS
            bucket_events = [event for event in recent_events if start <= event.at < end]
            # 只有没有请求且时间格与冻结区间相交时，才使用青色冻结条喵
            bucket_frozen = (
                not bucket_events
                and any(start < interval.ended_at and end > interval.started_at for interval in freeze_intervals)
            )
            buckets.append(HealthBucket(sum(int(event.success) for event in bucket_events), len(bucket_events), bucket_frozen))
        # 返回完整健康快照喵
        return HealthSnapshot(all_time, windows, buckets)

    def snapshot_candidate_health(self, candidate: Candidate) -> HealthSnapshot:
        """返回一个候选的成功率和 24 小时历史快照喵~"""
        # 加锁清理并读取候选健康事件喵
        with self._lock:
            # 取当前时刻作为整份快照的统一边界喵
            now = time.monotonic()
            # 清理过期历史，降低长期运行的内存占用喵
            self._prune_health_events_locked(now)
            # 查候选事件，没有就按空历史汇总喵
            events = self._candidate_health_events.get(candidate.identity, [])
            # 当前冻结状态用于将历史条统一标成青色提醒喵
            freeze_infos = []
            current_freeze = self._freezes.get(candidate.identity)
            if current_freeze is not None:
                freeze_infos.append(FreezeInterval(current_freeze.started_at, current_freeze.until))
            # 加入历史冻结区间，当前格有请求时渲染逻辑仍会优先使用状态色喵
            freeze_infos.extend(self._freeze_intervals.get(candidate.identity, []))
            # 计算候选最近 24 小时窗口和历史条喵
            snapshot = self._health_snapshot_locked(events, now, freeze_infos)
            # 候选累计计数比 24 小时事件更适合表达「所有时间」喵
            candidate_totals = self._candidate_health_totals.get(candidate.identity, [0.0] * 7)
            # 计算候选累计平均耗时，没有完整尝试时保持未知喵
            lifetime_average = candidate_totals[3] / candidate_totals[4] if candidate_totals[4] > 0 else None
            # 计算候选累计输入 Token 加权缓存命中率，没有可用分母时保持未上报喵
            lifetime_cache_rate = candidate_totals[6] / candidate_totals[5] if candidate_totals[5] > 0 else None
            # 返回窗口和历史条快照，并保留候选进程内累计资源统计喵
            lifetime_window = HealthWindow(
                int(candidate_totals[0]),
                int(candidate_totals[1]),
                int(candidate_totals[2]),
                lifetime_average,
                lifetime_cache_rate,
            )
            # 用候选资源累计窗口替换历史窗口，其余滚动窗口仍严格限制在 24 小时内喵
            return HealthSnapshot(lifetime_window, snapshot.windows, snapshot.buckets)

    def snapshot_virtual_model_health(self, virtual_model: str) -> HealthSnapshot:
        """返回一个虚拟模型的成功率和吞吐耗时快照喵~"""
        # 加锁清理并读取虚拟模型健康事件喵
        with self._lock:
            # 取当前时刻作为整份快照的统一边界喵
            now = time.monotonic()
            # 清理过期历史，保证历史条只保留 24 小时喵
            self._prune_health_events_locked(now)
            # 取虚拟模型事件，没有就按空历史汇总喵
            events = self._virtual_health_events.get(virtual_model, [])
            # 读取进程生命周期累计值，避免清理 24 小时历史后所有时间统计归零喵
            totals = self._virtual_health_totals.get(virtual_model, [0.0] * 7)
            # 计算累计平均耗时，没有完整请求时保持未知喵
            lifetime_average = totals[3] / totals[4] if totals[4] > 0 else None
            # 计算累计缓存命中率，没有有效输入分母时保持未上报喵
            lifetime_cache_rate = totals[6] / totals[5] if totals[5] > 0 else None
            # 收集该虚拟模型每个候选的当前和历史冻结区间喵
            freeze_infos: list[FreezeInterval] = []
            for candidate in self._config.virtual_models.get(virtual_model, []):
                current_freeze = self._freezes.get(candidate.identity)
                if current_freeze is not None:
                    freeze_infos.append(FreezeInterval(current_freeze.started_at, current_freeze.until))
                freeze_infos.extend(self._freeze_intervals.get(candidate.identity, []))
            # 返回窗口和历史条快照喵
            snapshot = self._health_snapshot_locked(events, now, freeze_infos)
            # 所有时间窗口使用进程内累计成功、请求、Token 和平均耗时喵
            lifetime_window = HealthWindow(
                int(totals[0]),
                int(totals[1]),
                int(totals[2]),
                lifetime_average,
                lifetime_cache_rate,
            )
            # 保留滚动窗口和历史条，替换掉仅限 24 小时的所有时间窗口喵
            return HealthSnapshot(lifetime_window, snapshot.windows, snapshot.buckets)


    def record_rate_event(self, virtual_model: str, usage_tokens: int | None = None, input_tokens: int | None = None, cached_tokens: int | None = None) -> RequestMetric:
        """
        记录一条成功请求，供滚动 60 秒 RPM/TPM 使用喵~

        token 只能传上游明确上报的 usage；没有就传 None。RPM 仍然照常加一，
        TPM 则保持「未知覆盖」状态，绝不能用字符数或本地 tokenizer 猜一个数喵。
        """
        # 创建这条事件，时间用单调时钟避免系统时间调整影响滚动窗口喵
        event = RequestMetric(
            at=time.monotonic(),
            usage_tokens=usage_tokens,
            input_tokens=input_tokens if isinstance(input_tokens, int) and not isinstance(input_tokens, bool) and input_tokens >= 0 else None,
            cached_tokens=cached_tokens if isinstance(cached_tokens, int) and not isinstance(cached_tokens, bool) and cached_tokens >= 0 else None,
        )
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
            # 按当前配置顺序逐个虚拟模型喵
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
                # 统计速率窗口内有效输入与缓存 Token 的加权命中率喵
                cache_events = [
                    event for event in rate_events
                    if event.input_tokens is not None and event.input_tokens > 0 and event.cached_tokens is not None
                ]
                cache_input_total = sum(event.input_tokens for event in cache_events)
                average_cache_hit_rate = (
                    sum(event.cached_tokens or 0 for event in cache_events) / cache_input_total
                    if cache_input_total > 0
                    else None
                )
                # 组装这一行快照喵
                rows.append(
                    VirtualModelRate(
                        virtual_model=virtual_model,
                        rpm=rpm,
                        usage_reported_requests=len(known),
                        tpm=tpm,
                        completed_requests=len(completed),
                        average_elapsed_ms=average_elapsed_ms,
                        average_cache_hit_rate=average_cache_hit_rate,
                    )
                )
        # 返回拷贝出来的快照，REPL 在锁外慢慢渲染喵
        return rows

    # ---------- 统计相关喵 ----------

    def record_success(self, candidate: Candidate, usage_tokens: int | None = None, elapsed_ms: float | None = None) -> None:
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
            # 既然成功了就说明它已经恢复，归档并立刻解冻，不用等倒计时走完喵
            active_freeze = self._freezes.pop(candidate.identity, None)
            if active_freeze is not None:
                self._record_freeze_interval_locked(candidate.identity, active_freeze, time.monotonic())

    def record_failure(self, candidate: Candidate, error: str, hedge_threshold: int = 0, count_health: bool = True) -> int:
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
            # 失败次数只有在 count_health 为真时才进入候选 stats 喵
            if not count_health:
                return 0
            # 失败次数加一喵
            stats.failure += 1
            # 记录最近错误发生的墙上时间，供 stats 展示报错时间喵
            stats.last_error_at = time.time()
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

