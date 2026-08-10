"""
配置模块喵~

整体思路：
    把 config.yaml 读成一组结构清晰的数据类，并且在「读」这一步就把所有能查出来的
    错误全查出来（缺字段、类型不对、正则语法错、虚拟模型链为空、动作名拼错……），
    让问题在启动阶段就暴露，而不是等线上转发请求时才炸喵。

输入：config.yaml 的文件路径
输出：AppConfig 对象，内含 server 配置、虚拟模型表、规则列表
边界条件：文件不存在、YAML 语法错误、字段缺失或类型不符、正则编译失败，
         一律抛出 ConfigError 并附带明确的中文原因，绝不静默套默认值糊过去喵。
"""

# 引入 __future__ 注解特性，让类型注解里可以直接写 X | None 这种写法喵
from __future__ import annotations

# dataclass 用来定义只装数据的轻量类，比手写 __init__ 清爽喵
from dataclasses import dataclass, field
# re 模块用来编译和匹配规则里的 body_regex 正则喵
import re
# pathlib 提供跨平台的路径对象，比字符串拼路径安全喵
from pathlib import Path
# typing 里取 Any 用于标注「任意 YAML 值」喵
from typing import Any

# PyYAML 负责把 YAML 文本解析成 Python 的 dict 和 list 喵
import yaml


# 特殊状态码：表示网络层失败（连不上、握手超时、读超时），用负数避免和真 HTTP 码撞车喵
STATUS_NETWORK_ERROR = -1
# 特殊状态码：表示上游返回了 200 但这条 SSE 流里没有任何有效内容（假成功）喵
STATUS_BAD_STREAM = -2

# 特殊状态码：表示这条流「卡住了」喵。
# 具体指：上游确实建立了 SSE 流，可能还吐了几个字，然后就一直挂着不再吐有效内容，
# 直到探测阶段的总时限用完。和 bad_stream 的区别是 bad_stream 已经能确定这条流是坏的
# （明确收到了 error 事件或空的结束标记），而 stalled_stream 是「一直没等到结论」，
# 所以适合原地重发一次试试，而不是立刻换候选喵。
STATUS_STALLED_STREAM = -3

# YAML 里能写的状态别名 → 内部特殊状态码的映射表喵
STATUS_ALIASES = {
    # 写 network 就等于网络层失败喵
    "network": STATUS_NETWORK_ERROR,
    # 写 bad_stream 就等于 200 假成功（明确坏掉的流）喵
    "bad_stream": STATUS_BAD_STREAM,
    # 写 stalled_stream 就等于流卡住了（吐了几个字或没吐，然后一直挂着）喵
    "stalled_stream": STATUS_STALLED_STREAM,
}

# 规则引擎允许的四种动作名，拼错的话加载阶段直接报错喵
VALID_ACTIONS = {"retry", "next", "freeze", "passthrough"}

# 允许的鉴权头风格，bearer 对应 OpenAI 系，x-api-key 对应 Anthropic 系喵
VALID_AUTH_STYLES = {"bearer", "x-api-key"}


class ConfigError(Exception):
    """配置有问题时抛这个异常，携带人能看懂的中文原因喵~"""
    # 这个异常不需要额外行为，只是给错误分类用，所以函数体留空喵
    pass


@dataclass(frozen=True)
class Candidate:
    """一个候选上游，代表「用这个地址 + 这个 key + 这个模型」去打一次请求喵~"""

    # 候选的可读名字，只用于日志和 REPL 展示，不参与请求喵
    name: str
    # 上游根地址，例如 https://api.openai.com，客户端的路径会原样拼在它后面喵
    base_url: str
    # 这个上游的真实 api key 喵
    api_key: str
    # 要替换进请求体顶层 model 字段的真实模型名喵
    model: str
    # 鉴权头风格，bearer 或 x-api-key 喵
    auth_style: str = "bearer"

    @property
    def identity(self) -> str:
        """
        候选的全局唯一身份串，用作冻结表和统计表的 key 喵~

        为什么要带上 api_key：上游的额度限制是按 key 算的，同一个地址的两个 key
        应该各自独立冻结，所以身份必须包含 key 本身喵。
        """
        # 用竖线把三要素拼起来，竖线不会出现在 url/key/模型名里，所以不会歧义喵
        return f"{self.base_url}|{self.api_key}|{self.model}"

    @property
    def masked_key(self) -> str:
        """给日志和 REPL 用的脱敏 key，只留头尾，防止把真 key 打进日志喵~"""
        # 喵~防御：key 太短时不做切片，直接整体打码，避免把短 key 几乎完整泄露出去喵
        if len(self.api_key) <= 11:
            return "***"
        # 保留前 6 位和后 4 位，中间用星号代替，方便肉眼对号又不泄密喵
        return f"{self.api_key[:6]}***{self.api_key[-4:]}"

    @property
    def label(self) -> str:
        """一行式简介，日志里描述「当前正在用哪个候选」时用喵~"""
        # 拼成「名字(真实模型 @ 地址 key=脱敏)」这种紧凑格式喵
        return f"{self.name}({self.model} @ {self.base_url} key={self.masked_key})"


@dataclass
class Rule:
    """
    一条故障处理规则喵~

    语义：如果这次尝试的状态码落在 status_codes 里，并且（若配了正则）响应体能被
    body_regex 搜到，那就执行 action 指定的动作喵。
    """

    # 动作名，取值为 retry / next / freeze / passthrough 喵
    action: str
    # 命中的状态码集合，可含真 HTTP 码，也可含 -1(network) 和 -2(bad_stream) 喵
    status_codes: frozenset[int] = frozenset()
    # 对响应体做搜索的正则，None 表示不看响应体内容、只看状态码喵
    body_regex: re.Pattern[str] | None = None
    # action=retry 时，同一候选上总共尝试几次（含第一次）喵
    max_attempts: int = 3
    # action=retry 时的退避基数，第 n 次重试前等待 backoff_base 的 n 次方秒喵
    backoff_base: float = 1.5
    # action=freeze 时，从正则的第几个捕获组取冻结时长数字，None 表示不抽取喵
    freeze_from_group: int | None = None
    # 抽取到的数字的时间单位，seconds 或 minutes 或 hours 喵
    freeze_unit: str = "minutes"
    # 没能抽取到数字时的兜底冻结秒数喵
    freeze_seconds: float = 300.0
    # 规则的原始 YAML 字典，REPL 要把规则写回文件时需要它喵
    raw: dict[str, Any] = field(default_factory=dict)

    def describe(self) -> str:
        """把规则渲染成一行人话，REPL 的 rule ls 命令直接打印它喵~"""
        # 把状态码集合排序后转成字符串，特殊码换回别名以便阅读喵
        codes = ",".join(_status_to_text(c) for c in sorted(self.status_codes))
        # 正则存在就显示其模式串，否则显示一个短横线表示没配喵
        pattern = self.body_regex.pattern if self.body_regex else "-"
        # 动作是 retry 时补充显示重试次数，其他动作显示为空喵
        extra = f" x{self.max_attempts}" if self.action == "retry" else ""
        # 动作是 freeze 时补充显示冻结策略，方便一眼看出冻多久喵
        if self.action == "freeze":
            # 有捕获组就说明是从消息里抽时长，否则是固定时长喵
            src = f"组{self.freeze_from_group}({self.freeze_unit})" if self.freeze_from_group else f"{self.freeze_seconds}秒"
            # 把冻结来源拼进补充说明喵
            extra = f" 时长={src}"
        # 组装成最终的一行描述喵
        return f"status=[{codes}] 正则={pattern} → {self.action}{extra}"


@dataclass
class ServerConfig:
    """服务器与超时相关的配置喵~"""

    # 监听地址，默认只绑本地回环喵
    host: str = "127.0.0.1"
    # 监听端口喵
    port: int = 8787
    # 单次上游请求的总超时秒数喵
    request_timeout: float = 300.0
    # 等第一个有效内容字符的超时秒数。上游一个字都不吐时靠这个快速失败喵
    first_content_timeout: float = 45.0
    # 整个探测阶段的总时限秒数。上游吐了几个字然后卡住不动时靠这个兜住，
    # 超时就判定为 stalled_stream（流卡住了）喵
    stall_timeout: float = 60.0
    # 放行给客户端之前，需要先累积够这么多个内容字符。
    # 设成 10 是为了避开「上游先吐一两个字符然后卡死」这种情况 —— 只等 1 个字符的话
    # 会被这种流骗过去，字节一出门就再也没法换候选了喵。
    # 注意：如果整条流在凑够这个数之前就正常结束了（短回答），也会照常放行，不会干等喵。
    min_content_chars: int = 10
    # 建立连接的超时秒数喵
    connect_timeout: float = 15.0
    # 配置文件热重载的轮询间隔秒数，0 表示关闭喵
    reload_poll_interval: float = 2.0


@dataclass
class AppConfig:
    """整份配置的顶层容器喵~"""

    # 服务器配置喵
    server: ServerConfig
    # 虚拟模型表：虚拟模型名 → 按优先级排好的候选链喵
    virtual_models: dict[str, list[Candidate]]
    # 规则列表，顺序即优先级喵
    rules: list[Rule]
    # 配置文件所在路径，热重载和 REPL 保存时需要喵
    source_path: Path | None = None


def _status_to_text(code: int) -> str:
    """把内部状态码转回人能读的文本，特殊负数码换成别名喵~"""
    # 遍历别名表，找到数值相等的就返回它的别名喵
    for alias, value in STATUS_ALIASES.items():
        # 命中就直接返回别名字符串喵
        if value == code:
            return alias
    # 普通 HTTP 码原样转字符串喵
    return str(code)


def _parse_status(value: Any, where: str) -> frozenset[int]:
    """
    把 YAML 里的 status 字段解析成状态码集合喵~

    支持三种写法：单个整数 429、整数列表 [500, 502]、别名字符串 network，
    以及它们的混合列表 [429, "network"] 喵。
    """
    # 喵~防御：status 字段缺失时视为「匹配任意状态码」，用空集合表示，由调用方处理喵
    if value is None:
        return frozenset()
    # 统一成列表处理：单个值也包成单元素列表，省掉两套分支喵
    items = value if isinstance(value, list) else [value]
    # 收集解析结果的集合喵
    codes: set[int] = set()
    # 逐个解析列表里的元素喵
    for item in items:
        # 布尔值在 Python 里是 int 的子类，必须先排除，否则 True 会被当成 1 喵
        if isinstance(item, bool):
            raise ConfigError(f"{where} 的 status 不能写布尔值：{item!r} 喵")
        # 整数就是普通 HTTP 状态码，直接收下喵
        if isinstance(item, int):
            codes.add(item)
        # 字符串按别名表翻译喵
        elif isinstance(item, str):
            # 转成小写并去空格，容忍用户写 " Network " 这种喵
            key = item.strip().lower()
            # 喵~防御：不认识的别名直接报错，并列出所有合法别名帮用户改喵
            if key not in STATUS_ALIASES:
                allowed = "、".join(STATUS_ALIASES)
                raise ConfigError(f"{where} 的 status 出现未知别名 {item!r}，只支持：{allowed} 喵")
            # 翻译成内部特殊状态码喵
            codes.add(STATUS_ALIASES[key])
        # 既不是整数也不是字符串，说明类型写错了喵
        else:
            raise ConfigError(f"{where} 的 status 元素类型不对：{item!r} 喵")
    # 转成不可变集合返回，避免之后被误改喵
    return frozenset(codes)


def _parse_rule(raw: Any, index: int) -> Rule:
    """把 YAML 里的一条规则字典解析成 Rule 对象喵~"""
    # 用序号描述位置，报错时用户能立刻定位到第几条规则喵
    where = f"rules 第 {index + 1} 条"
    # 喵~防御：规则必须是字典，写成字符串或列表都报错喵
    if not isinstance(raw, dict):
        raise ConfigError(f"{where} 必须是字典，实际是 {type(raw).__name__} 喵")
    # 取出动作名，缺失时报错而不是猜一个默认动作喵
    action = raw.get("action")
    # 喵~防御：动作名必须在白名单里，拼错要立刻发现喵
    if action not in VALID_ACTIONS:
        allowed = "、".join(sorted(VALID_ACTIONS))
        raise ConfigError(f"{where} 的 action={action!r} 不合法，只支持：{allowed} 喵")
    # 取出 match 子字典，没写就当成空字典喵
    match = raw.get("match") or {}
    # 喵~防御：match 必须是字典喵
    if not isinstance(match, dict):
        raise ConfigError(f"{where} 的 match 必须是字典喵")
    # 解析状态码集合喵
    status_codes = _parse_status(match.get("status"), where)
    # 取出正则模式串，可能没配喵
    pattern_text = match.get("body_regex")
    # 正则对象默认为空，只有配了才编译喵
    body_regex: re.Pattern[str] | None = None
    # 配了正则就编译，编译失败要给出明确原因喵
    if pattern_text is not None:
        # 喵~防御：正则必须是字符串喵
        if not isinstance(pattern_text, str):
            raise ConfigError(f"{where} 的 body_regex 必须是字符串喵")
        # 尝试编译正则，忽略大小写以容忍上游文案大小写变化喵
        try:
            body_regex = re.compile(pattern_text, re.IGNORECASE)
        # 喵~防御：正则语法错误时明确指出是哪条规则的哪个模式串坏了喵
        except re.error as exc:
            raise ConfigError(f"{where} 的 body_regex 编译失败：{exc} 喵") from exc
    # 喵~防御：既没有状态码也没有正则的规则会匹配一切，属于危险配置，直接拒绝喵
    if not status_codes and body_regex is None:
        raise ConfigError(f"{where} 既没写 status 也没写 body_regex，会匹配所有情况，请补全喵")
    # 组装 Rule 对象，数值字段做类型转换并给出安全下限喵
    return Rule(
        # 动作名喵
        action=action,
        # 状态码集合喵
        status_codes=status_codes,
        # 编译好的正则喵
        body_regex=body_regex,
        # 重试次数至少为 1（也就是至少尝试一次），避免配成 0 导致一次都不打喵
        max_attempts=max(1, int(raw.get("max_attempts", 3))),
        # 退避基数至少为 1.0，小于 1 会让等待时间越来越短，没有退避意义喵
        backoff_base=max(1.0, float(raw.get("backoff_base", 1.5))),
        # 捕获组序号，None 表示不从消息里抽时长喵
        freeze_from_group=raw.get("freeze_from_group"),
        # 时间单位，默认按分钟解释喵
        freeze_unit=str(raw.get("freeze_unit", "minutes")).strip().lower(),
        # 兜底冻结秒数，至少 1 秒防止配成 0 导致冻结形同虚设喵
        freeze_seconds=max(1.0, float(raw.get("freeze_seconds", 300.0))),
        # 保留原始字典，REPL 保存配置时按原样写回喵
        raw=raw,
    )


def _parse_candidate(raw: Any, vm_name: str, index: int) -> Candidate:
    """把 YAML 里的一个候选字典解析成 Candidate 对象喵~"""
    # 描述位置，报错时能定位到哪个虚拟模型的第几个候选喵
    where = f"虚拟模型 {vm_name} 的第 {index + 1} 个候选"
    # 喵~防御：候选必须是字典喵
    if not isinstance(raw, dict):
        raise ConfigError(f"{where} 必须是字典，实际是 {type(raw).__name__} 喵")
    # 三个必填字段，缺一个都没法发请求喵
    for required in ("base_url", "api_key", "model"):
        # 喵~防御：必填字段缺失或为空字符串都要报错，空 key 打过去只会拿到 401 喵
        if not raw.get(required):
            raise ConfigError(f"{where} 缺少必填字段 {required} 喵")
    # 取鉴权风格，默认 bearer 喵
    auth_style = str(raw.get("auth_style", "bearer")).strip().lower()
    # 喵~防御：鉴权风格必须在白名单里，否则请求头会拼错导致全部 401 喵
    if auth_style not in VALID_AUTH_STYLES:
        allowed = "、".join(sorted(VALID_AUTH_STYLES))
        raise ConfigError(f"{where} 的 auth_style={auth_style!r} 不合法，只支持：{allowed} 喵")
    # 组装 Candidate 对象喵
    return Candidate(
        # 名字没写就用「虚拟模型名#序号」自动生成一个，保证日志里可区分喵
        name=str(raw.get("name") or f"{vm_name}#{index + 1}"),
        # 去掉 base_url 末尾的斜杠，避免拼路径时出现双斜杠喵
        base_url=str(raw["base_url"]).strip().rstrip("/"),
        # api key 去掉首尾空白，防止复制粘贴时带进空格导致鉴权失败喵
        api_key=str(raw["api_key"]).strip(),
        # 真实模型名喵
        model=str(raw["model"]).strip(),
        # 鉴权风格喵
        auth_style=auth_style,
    )


def parse_config(data: Any, source_path: Path | None = None) -> AppConfig:
    """
    把已经解析成 Python 对象的 YAML 数据转成 AppConfig 喵~

    单独拆出这个函数是为了让单元测试可以直接喂字典，不必真的落一个文件喵。
    """
    # 喵~防御：顶层必须是字典，空文件会被 yaml 解析成 None，也在这里挡掉喵
    if not isinstance(data, dict):
        raise ConfigError("配置文件顶层必须是字典（是不是文件空了喵？）")
    # 取 server 段，没写就用空字典走全默认值喵
    server_raw = data.get("server") or {}
    # 喵~防御：server 段必须是字典喵
    if not isinstance(server_raw, dict):
        raise ConfigError("server 段必须是字典喵")
    # 构造服务器配置，各数值字段都做类型转换与安全下限处理喵
    server = ServerConfig(
        # 监听地址，默认只绑本地喵
        host=str(server_raw.get("host", "127.0.0.1")),
        # 监听端口喵
        port=int(server_raw.get("port", 8787)),
        # 总超时至少 1 秒，防止配成 0 导致请求瞬间被掐断喵
        request_timeout=max(1.0, float(server_raw.get("request_timeout", 300.0))),
        # 首内容超时至少 1 秒，同理喵
        first_content_timeout=max(1.0, float(server_raw.get("first_content_timeout", 45.0))),
        # 探测阶段总时限至少 1 秒喵
        stall_timeout=max(1.0, float(server_raw.get("stall_timeout", 60.0))),
        # 放行门槛至少 1 个字符。设成 0 或负数等于不设门槛，收到任何内容就放行，
        # 那样就完全失去了防「吐一两个字然后卡死」的能力，所以这里压到 1 喵
        min_content_chars=max(1, int(server_raw.get("min_content_chars", 10))),
        # 连接超时至少 1 秒喵
        connect_timeout=max(1.0, float(server_raw.get("connect_timeout", 15.0))),
        # 热重载轮询间隔，允许为 0 表示彻底关闭该功能喵
        reload_poll_interval=max(0.0, float(server_raw.get("reload_poll_interval", 2.0))),
    )
    # 喵~防御：端口必须在合法范围内，否则 uvicorn 启动会报难懂的底层错误喵
    if not (1 <= server.port <= 65535):
        raise ConfigError(f"server.port={server.port} 不在 1~65535 范围内喵")
    # 取虚拟模型表喵
    vms_raw = data.get("virtual_models")
    # 喵~防御：虚拟模型表必须存在且是字典，否则代理没有任何可服务的模型喵
    if not isinstance(vms_raw, dict) or not vms_raw:
        raise ConfigError("virtual_models 必须是非空字典，至少配一个虚拟模型喵")
    # 存放解析结果的字典喵
    virtual_models: dict[str, list[Candidate]] = {}
    # 逐个虚拟模型解析它的候选链喵
    for vm_name, chain_raw in vms_raw.items():
        # 喵~防御：候选链必须是非空列表，空链意味着这个虚拟模型永远无法服务喵
        if not isinstance(chain_raw, list) or not chain_raw:
            raise ConfigError(f"虚拟模型 {vm_name} 的候选链必须是非空列表喵")
        # 逐个解析候选并按原顺序保留，顺序就是优先级喵
        virtual_models[str(vm_name)] = [
            _parse_candidate(item, str(vm_name), i) for i, item in enumerate(chain_raw)
        ]
    # 取规则列表，没写就当空列表喵
    rules_raw = data.get("rules") or []
    # 喵~防御：规则段必须是列表喵
    if not isinstance(rules_raw, list):
        raise ConfigError("rules 必须是列表喵")
    # 逐条解析规则，顺序即匹配优先级喵
    rules = [_parse_rule(item, i) for i, item in enumerate(rules_raw)]
    # 组装并返回顶层配置对象喵
    return AppConfig(server=server, virtual_models=virtual_models, rules=rules, source_path=source_path)


def load_config(path: str | Path) -> AppConfig:
    """从磁盘读取并解析配置文件喵~"""
    # 统一转成 Path 对象，后面调用更方便喵
    config_path = Path(path)
    # 喵~防御：文件不存在时给出明确提示，并告诉用户可以从模板复制喵
    if not config_path.is_file():
        raise ConfigError(f"配置文件 {config_path} 不存在，可以先复制 config.example.yaml 喵")
    # 读取文件文本，显式指定 utf-8 避免 Windows 下按 GBK 解码中文注释报错喵
    try:
        text = config_path.read_text(encoding="utf-8")
    # 喵~防御：权限不足或磁盘读取失败时包装成 ConfigError，保持异常类型统一喵
    except OSError as exc:
        raise ConfigError(f"读取配置文件 {config_path} 失败：{exc} 喵") from exc
    # 解析 YAML 文本，safe_load 不会执行任意 Python 对象构造，比 load 安全喵
    try:
        data = yaml.safe_load(text)
    # 喵~防御：YAML 语法错误时把 PyYAML 的定位信息透出来，方便改喵
    except yaml.YAMLError as exc:
        raise ConfigError(f"配置文件 {config_path} YAML 语法错误：{exc} 喵") from exc
    # 交给纯逻辑函数完成校验和组装喵
    return parse_config(data, source_path=config_path)
