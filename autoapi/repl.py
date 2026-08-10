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

  改规则类（改完立即生效并写回 config.yaml）：
    rule add <JSON>         追加一条规则到列表末尾，参数是规则的 JSON 喵
    rule rm <序号>          删掉指定序号的规则喵
    rule mv <原序号> <新序号> 挪动规则位置来调整匹配优先级喵

  冻结类：
    freeze clear            立刻清空所有冻结，让所有候选马上可用喵

  其他：
    reload                  从磁盘重新加载 config.yaml 喵
    save                    把当前内存里的配置写回 config.yaml 喵
    help                    显示这份帮助喵
    quit                    退出整个代理进程喵

  rule add 的 JSON 例子喵：
    rule add {"match": {"status": 429}, "action": "retry", "max_attempts": 3}
    rule add {"match": {"status": [500, 502]}, "action": "next"}
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

    def _mutate_rules(self, mutator) -> None:
        """
        改规则的统一流程：重读 → 改 → 校验 → 应用 → 保存喵~

        输入：mutator 是个函数，接收规则列表并原地改动它，返回一段描述改了什么的文本
        说明：把「重读、校验、应用、保存」这四步公共流程收在这里，各个命令只关心怎么改，
             这样任何一个改规则的命令都不可能漏掉校验这一步喵。
        """
        # 从磁盘重读原始配置喵
        data = self._read_raw_yaml()
        # 取出规则列表，没有就当空列表喵
        rules = data.get("rules") or []
        # 喵~防御：规则段必须是列表，否则后面的下标操作会炸喵
        if not isinstance(rules, list):
            raise ConfigError("配置里的 rules 不是列表，无法改写喵")
        # 交给具体命令去改动列表，并拿回一段描述文本喵
        summary = mutator(rules)
        # 把改好的列表写回字典喵
        data["rules"] = rules
        # 先校验并应用，失败会抛异常，此时磁盘上的文件还没被动过，最安全喵
        self._apply(data)
        # 校验通过了才写盘喵
        self._save(data)
        # 打印改动摘要，并提示现在共有几条规则喵
        print(f"{summary}，现在共 {len(rules)} 条规则，已生效并写回配置文件喵~")

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
        解析一行输入并分发到对应的命令喵~

        输入：主人敲的一整行文本
        说明：按空格切分，但只切前几段 —— rule add 的 JSON 参数里有空格，
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
        # vm 命令列虚拟模型喵
        if head == "vm":
            self.cmd_vm()
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
            # 执行这条命令，命令内部的异常都在这里兜住喵
            try:
                self.dispatch(line)
            # 喵~防御：配置类错误打印友好中文提示，配置保持不变喵
            except ConfigError as exc:
                print(f"操作失败喵：{exc}")
            # 喵~防御：其他任何异常都打印出来但不让 REPL 线程挂掉，保证交互一直可用喵
            except Exception as exc:  # noqa: BLE001
                print(f"命令执行出错喵：{type(exc).__name__}: {exc}")


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
