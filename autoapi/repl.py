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
# json 用来解析 rule add 命令的参数、以及打印规则内容喵
import json
# threading 用来跑独立线程和退出信号喵
import threading
# Any 用于标注 YAML 解析出来的任意结构喵
from typing import Any

# PyYAML 用来读写配置文件喵
import yaml

# 引入配置校验相关喵
from .config import ConfigError, parse_config
# 引入运行时状态喵
from .state import RuntimeState

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
    vm add <虚拟模型名> <候选JSON>    新建虚拟模型并配上第一个候选喵
    vm rm <虚拟模型名>                删掉一整个虚拟模型喵

  改服务器配置类：
    set <字段> <值>         能改 request_timeout、first_content_timeout、
                            connect_timeout、reload_poll_interval、port 喵
                            （port 要重启才生效，其余立即生效）

  冻结类：
    freeze clear            立刻清空所有冻结，让所有候选马上可用喵

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

  候选的字段说明喵：
    必填：base_url、api_key、model（model 是发给上游的真实模型名）
    可选：name（显示用）、auth_style（bearer 或 x-api-key，默认 bearer）
""".strip()


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
        """打印每个候选的成败统计喵~"""
        # 取统计快照喵
        stats = self.state.snapshot_stats()
        # 打印总体计数喵
        print(f"\n累计处理 {self.state.total_requests} 条请求，其中 {self.state.total_exhausted} 条把整条候选链都用尽了喵")
        # 喵~防御：还没有任何候选被用过时给提示喵
        if not stats:
            print("还没有任何候选被使用过喵~")
            return
        # 打印表头喵
        print("\n各候选统计喵：")
        # 逐个候选打印统计喵
        for identity, row in stats.items():
            # 身份串里第一段是 base_url、第三段是模型名，取出来做展示喵
            parts = identity.split("|")
            # 喵~防御：身份串格式异常时退回打印整个串，不让展示逻辑炸掉喵
            shown = f"{parts[2]} @ {parts[0]}" if len(parts) >= 3 else identity
            # 打印成功、失败、被冻结次数喵
            print(f"  {shown}")
            print(f"    成功 {row.success} 次 / 失败 {row.failure} 次 / 被冻结 {row.frozen_times} 次")
            # 有最近错误就打印出来喵
            if row.last_error:
                # 截断到 120 字符保持整洁喵
                print(f"    最近错误：{row.last_error[:120]}")

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
            # 单次上游请求的总超时，单位：秒喵
            "request_timeout": float,
            # 等首个内容字符的超时，单位：秒喵
            "first_content_timeout": float,
            # 连接握手超时，单位：秒喵
            "connect_timeout": float,
            # 配置热重载的轮询间隔，单位：秒喵
            "reload_poll_interval": float,
            # 监听端口喵
            "port": int,
        }
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
            # 喵~防御：不认识的子命令给出提示喵
            print(f"不认识的 cand 子命令 {sub!r} 喵，支持 add / rm / mv~")
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
            # 喵~防御：不认识的子命令给出提示喵
            print(f"不认识的 freeze 子命令 {sub!r} 喵，试试 freeze ls~")
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
        print("\nautoapi 交互式命令行就绪喵~ 敲 help 看命令，敲 quit 退出~\n")
        # 一直循环读命令，直到收到退出信号喵
        while not self.should_exit.is_set():
            # 读一行输入，各种异常都要接住喵
            try:
                line = input("autoapi> ")
            # 喵~防御：stdin 关闭（Ctrl+D 或后台运行）时退出 REPL 但保留代理服务喵
            except EOFError:
                print("\n检测到 stdin 已关闭，交互式命令行退出，代理继续在后台服务喵~")
                return
            # 喵~防御：Ctrl+C 时不直接杀进程，提示用 quit 优雅退出喵
            except KeyboardInterrupt:
                print("\n想退出的话敲 quit 喵~")
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
