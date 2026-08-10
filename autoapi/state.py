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
        # 代理累计处理的客户端请求数喵
        self.total_requests = 0
        # 代理累计彻底失败（整条候选链都用尽）的请求数喵
        self.total_exhausted = 0

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

    # ---------- 统计相关喵 ----------

    def record_success(self, candidate: Candidate) -> None:
        """记录一次成功，并顺手解冻这个候选喵~"""
        # 加锁更新统计并删除冻结记录喵
        with self._lock:
            # 成功次数加一喵
            self._stats.setdefault(candidate.identity, CandidateStats()).success += 1
            # 既然成功了就说明它已经恢复，立刻解冻，不用等倒计时走完喵
            self._freezes.pop(candidate.identity, None)

    def record_failure(self, candidate: Candidate, error: str) -> None:
        """记录一次失败和失败原因喵~"""
        # 加锁更新统计喵
        with self._lock:
            # 取出（或新建）该候选的统计对象喵
            stats = self._stats.setdefault(candidate.identity, CandidateStats())
            # 失败次数加一喵
            stats.failure += 1
            # 记下最近一次失败原因，截断到 200 字符防止内存膨胀喵
            stats.last_error = error[:200]

    def snapshot_stats(self) -> dict[str, CandidateStats]:
        """拷贝一份统计快照给 REPL 打印，避免 REPL 长时间持锁喵~"""
        # 加锁做浅拷贝，CandidateStats 本身是只读展示用，浅拷贝够了喵
        with self._lock:
            return dict(self._stats)

