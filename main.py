"""
autoapi 启动入口喵~

做的事：解析命令行参数 → 加载配置 → 建运行时状态 → 起 REPL 线程 → 跑 uvicorn。

线程安排：
    主线程跑 uvicorn（也就是事件循环），REPL 跑在独立的 daemon 线程里。
    因为 input() 是阻塞调用，绝对不能放进事件循环，否则整个代理会被卡死喵。

用法喵：
    python main.py                      用默认的 config.yaml 启动
    python main.py -c 别的配置.yaml       指定配置文件
    python main.py --no-repl            不开交互式命令行（适合用 nohup 后台跑）
"""

# 引入注解特性喵
from __future__ import annotations

# argparse 用来解析命令行参数喵
import argparse
# logging 用来配置全局日志格式喵
import logging
# os 用来读 NO_COLOR 环境变量，判断主人是否明确要求不上色喵
import os
# sys 用来控制退出码喵
import sys
# pathlib 用来算脚本自己所在的目录，好让配置文件不依赖当前工作目录喵
from pathlib import Path
# threading 用来在 REPL 敲 quit 后停掉 uvicorn 喵
import threading
# time 用来等 uvicorn 真正启动完成，避免跳过它的停机流程喵
import time

# uvicorn 是跑 FastAPI 的 ASGI 服务器喵
import uvicorn

# 引入配置加载和异常类型喵
from autoapi.config import ConfigError, load_config
# 引入 REPL 启动函数喵
from autoapi.repl import start_repl_thread
# 引入应用工厂喵
from autoapi.server import create_app
# 引入运行时状态喵
from autoapi.state import RuntimeState


# 各日志级别对应的 ANSI 颜色码喵
LEVEL_COLORS = {
    # DEBUG 用暗灰，存在但不抢眼喵
    "DEBUG": "\033[90m",
    # INFO 用默认色，不上色以免正常日志也花里胡哨喵
    "INFO": "",
    # WARNING 用黄色，主人要求的喵
    "WARNING": "\033[33m",
    # ERROR 用红色，主人要求的喵
    "ERROR": "\033[31m",
    # CRITICAL 用红底白字，比 ERROR 还要跳一级喵
    "CRITICAL": "\033[41;97m",
}
# 重置颜色的 ANSI 码，每行结尾都要补上它，否则颜色会漏到后面的输出去喵
COLOR_RESET = "\033[0m"


def supports_color() -> bool:
    """
    判断当前终端能不能显示颜色喵~

    为什么必须判断：ANSI 转义码在不支持的地方会原样显示成一堆 ESC[33m 这样的乱码。
    最典型的就是主人用 `python main.py > autoapi.log` 把日志重定向到文件时 ——
    文件里混进转义码会让日志难读、也不好被别的工具解析喵。

    Windows 上还要额外做一件事：老式的 console 默认不认 ANSI，需要主动开启
    「虚拟终端处理」模式。开成功了才算支持颜色喵。
    """
    # 喵~防御：stdout 不是终端（被重定向到文件或管道）时一律不上色喵
    if not sys.stdout.isatty():
        return False
    # 有些 CI 环境会设这个变量明确要求不上色，尊重它喵
    if os.environ.get("NO_COLOR"):
        return False
    # 非 Windows 平台（Linux、macOS）的终端基本都认 ANSI，直接返回支持喵
    if sys.platform != "win32":
        return True
    # Windows 上尝试开启虚拟终端处理模式喵
    try:
        # 引入 ctypes 调 Windows API 喵
        import ctypes
        # 拿到 kernel32 库喵
        kernel32 = ctypes.windll.kernel32
        # -11 是 STD_OUTPUT_HANDLE，也就是标准输出的句柄喵
        handle = kernel32.GetStdHandle(-11)
        # 用来接收当前控制台模式的变量喵
        mode = ctypes.c_uint32()
        # 先读出当前模式，读失败说明这不是个正常的控制台喵
        if not kernel32.GetConsoleMode(handle, ctypes.byref(mode)):
            return False
        # 0x0004 是 ENABLE_VIRTUAL_TERMINAL_PROCESSING，开了才认 ANSI 转义码喵
        # 用「或」的方式加上这个标志，保留原有的其他标志不动喵
        return bool(kernel32.SetConsoleMode(handle, mode.value | 0x0004))
    # 喵~防御：任何一步失败都保守地认为不支持颜色，宁可没颜色也不要满屏乱码喵
    except Exception:  # noqa: BLE001
        return False


class ColorFormatter(logging.Formatter):
    """给日志按级别上色的格式化器喵~"""

    def format(self, record: logging.LogRecord) -> str:
        """
        把一条日志渲染成带颜色的文本喵~

        做法：先用父类渲染出正常的日志文本，再整行套上这个级别对应的颜色。
        整行上色而不是只染级别名，是因为一条 WARNING 的重点往往在后面的消息里
        （比如「自动避险 xxx：连续失败 3 次」），整行黄色扫一眼就能定位到喵。
        """
        # 先让父类按格式串渲染出朴素文本喵
        text = super().format(record)
        # 取这个级别对应的颜色码，没有对应色就用空串（也就是不上色）喵
        color = LEVEL_COLORS.get(record.levelname, "")
        # 没有颜色就原样返回，避免白白拼接两个空串喵
        if not color:
            return text
        # 前面加颜色码、后面补重置码，防止颜色漏到后续输出喵
        return f"{color}{text}{COLOR_RESET}"


class LiveStreamHandler(logging.StreamHandler):
    """
    每次写日志时都重新去取当前的 sys.stderr 的处理器喵~

    为什么需要它（这修的是「日志滚动时下方状态监控上移并渲染错乱」那个 bug）：
        内置的 StreamHandler 在创建的那一刻就把 sys.stderr 这个对象抓在手里了，
        之后 sys.stderr 被换成别的东西，它也照旧往老的那个写。

        而 REPL 那边为了让日志和底部冻结表和平共处，用了 prompt_toolkit 的
        patch_stdout —— 它的做法正是「把 sys.stdout / sys.stderr 换成一个代理对象」，
        让所有输出都先交给 prompt_toolkit，由它先把底部横幅擦掉、打完日志再重画。

        两件事一撞：日志绕过代理直接怼进终端，prompt_toolkit 不知道屏幕上多了几行，
        它记着的光标位置就全错了，于是横幅位置往上跑、内容糊在一起。

        改成每次 emit 都现取 sys.stderr，日志就会走进 patch_stdout 的代理，
        prompt_toolkit 能感知到每一行输出，横幅始终稳稳待在最下面喵。
    """

    @property
    def stream(self):
        """读的时候现取当前的 sys.stderr 喵~"""
        # 不缓存、不记住，每次都取最新的那个对象喵
        return sys.stderr

    @stream.setter
    def stream(self, value) -> None:
        """
        父类的 __init__ 会往 self.stream 赋值，这里必须接住喵~

        故意什么都不做：这个处理器的 stream 永远由上面的 getter 决定，
        谁也别想把它固定成某个具体对象，否则就退回到 bug 之前的行为了喵。
        """
        # 喵~防御：静默忽略赋值。不抛异常是因为父类 __init__ 和 setStream 都会赋值，
        # 抛出来会让处理器压根建不起来，日志全没了反而更糟喵
        return


def setup_logging() -> None:
    """配置全局日志格式喵~"""
    # 日志格式：时间 级别 来源 消息，时间精确到秒足够排查了喵
    fmt = "%(asctime)s %(levelname)-7s %(name)-15s %(message)s"
    # 时间格式，只留时分秒让日志行短一点喵
    datefmt = "%H:%M:%S"
    # 建一个输出到「当前」stderr 的处理器，好和 REPL 的 patch_stdout 配合喵
    handler = LiveStreamHandler()
    # 终端支持颜色就用彩色格式化器，否则用朴素的，避免日志文件里混进转义码喵
    if supports_color():
        handler.setFormatter(ColorFormatter(fmt, datefmt=datefmt))
    else:
        handler.setFormatter(logging.Formatter(fmt, datefmt=datefmt))
    # 设成 INFO 级别，能看到每条请求走了哪个候选，又不会被 DEBUG 噪音淹没喵
    logging.basicConfig(level=logging.INFO, handlers=[handler])
    # 把 httpx 自己的日志压到 WARNING，否则每条请求它都会打一行，太吵喵
    logging.getLogger("httpx").setLevel(logging.WARNING)
    # httpcore 是 httpx 的底层，日志更细碎，一并压掉喵
    logging.getLogger("httpcore").setLevel(logging.WARNING)


# 这个脚本所在的目录喵。配置文件默认就放在它旁边，而不是「当前工作目录」里
# 主人注意：这一行是为了解决「从别的目录或 IDE 里启动就说 config.yaml 不存在」的问题喵。
# 相对路径 "config.yaml" 是相对当前工作目录解析的，所以 cd 到别处再跑就找不到了；
# 用脚本自己的目录当基准，无论从哪儿启动都能找到同一份配置喵。
SCRIPT_DIR = Path(__file__).resolve().parent


def resolve_config_path(raw: str) -> Path:
    """
    把命令行给的配置路径解析成一个确定的绝对路径喵~

    查找顺序：
        1. 如果给的是绝对路径，就直接用它
        2. 否则先在「当前工作目录」下找 —— 这样主人在项目目录里跑时行为和以前完全一致
        3. 再在「脚本所在目录」下找 —— 这样从任何地方启动都能找到项目里那份配置
    输出：找到的话返回那个存在的路径；都没找到就返回脚本目录下的候选路径，
         好让后面的报错信息指向最可能正确的位置喵。
    """
    # 转成 Path 对象喵
    given = Path(raw)
    # 绝对路径就按主人指定的来，不做任何猜测喵
    if given.is_absolute():
        return given
    # 先看当前工作目录下有没有喵
    cwd_candidate = Path.cwd() / given
    # 有就用它，保持和以前一致的行为喵
    if cwd_candidate.is_file():
        return cwd_candidate
    # 再看脚本旁边有没有喵
    script_candidate = SCRIPT_DIR / given
    # 有就用它，这是从别的目录启动时的救星喵
    if script_candidate.is_file():
        return script_candidate
    # 两处都没有，返回脚本目录下的候选路径，让报错指向最可能正确的位置喵
    return script_candidate


def parse_args() -> argparse.Namespace:
    """解析命令行参数喵~"""
    # 创建解析器，描述文本会出现在 -h 的输出里喵
    parser = argparse.ArgumentParser(description="autoapi —— LLM API 故障转移代理喵~")
    # -c/--config 指定配置文件路径，默认 config.yaml 喵
    parser.add_argument(
        "-c",
        "--config",
        default="config.yaml",
        help="配置文件路径，默认 config.yaml（相对路径会先在当前目录找、再在脚本旁边找）喵",
    )
    # --no-repl 关掉交互式命令行，后台跑时用得上喵
    parser.add_argument(
        "--no-repl",
        action="store_true",
        help="不启动交互式命令行，适合用 nohup 之类的方式后台运行喵",
    )
    # 解析并返回喵
    return parser.parse_args()


def main() -> int:
    """
    主入口喵~

    输出：进程退出码，0 表示正常退出，1 表示配置有问题没能启动
    """
    # 先把日志配好，这样后面所有环节的日志格式才统一喵
    setup_logging()
    # 解析命令行参数喵
    args = parse_args()
    # 把配置路径解析成确定的绝对路径，这样从任何目录启动都能找到喵
    config_path = resolve_config_path(args.config)
    # 加载配置喵
    try:
        config = load_config(config_path)
    # 喵~防御：配置有问题时打印明确的中文原因并以退出码 1 结束，不带一堆 traceback 吓人喵
    except ConfigError as exc:
        # 先说清楚是哪个文件出了问题喵
        print(f"启动失败喵：{exc}", file=sys.stderr)
        # 喵~防御：如果是「文件不存在」，额外给出可以直接照抄的修复命令，
        # 并且用真实存在的模板文件名（模板已改名为 config.example）喵
        if not config_path.is_file():
            # 找出脚本目录下实际存在的模板文件名喵
            template = next(
                (name for name in ("config.example", "config.example.yaml") if (SCRIPT_DIR / name).is_file()),
                None,
            )
            # 只有相对路径才会去两个地方找，绝对路径是主人明确指定的、没有猜测空间喵
            if not Path(args.config).is_absolute():
                # 算出两个候选位置喵
                cwd_candidate = (Path.cwd() / args.config).resolve()
                script_candidate = (SCRIPT_DIR / args.config).resolve()
                # 两个位置不同才有必要都列出来，相同时列两遍只会让人困惑喵
                if cwd_candidate != script_candidate:
                    print("  这两个位置都找过了喵：", file=sys.stderr)
                    print(f"    当前工作目录：{cwd_candidate}", file=sys.stderr)
                    print(f"    脚本所在目录：{script_candidate}", file=sys.stderr)
            # 有模板就给出照抄即可的命令喵
            if template:
                print(f"  可以这样创建喵：", file=sys.stderr)
                print(f"    cd {SCRIPT_DIR}", file=sys.stderr)
                print(f"    cp {template} config.yaml", file=sys.stderr)
                print(f"  然后把里面的 api_key 换成主人自己的真 key 喵~", file=sys.stderr)
        # 用退出码 1 表示启动失败喵
        return 1
    # 喵~防御：配置里用了已退役的配置项时，在最显眼的位置提醒一次。
    # 这类问题不会让代理起不来，但会让主人以为某项超时配置生效了、实际压根没读，
    # 所以必须在启动摘要之前就说清楚喵
    for warning in config.warnings:
        print(f"⚠ 配置提醒喵：{warning}", file=sys.stderr)
    # 用配置创建运行时状态喵
    state = RuntimeState(config)
    # 创建 FastAPI 应用喵
    app = create_app(state)
    # 打印启动摘要，让主人一眼看到监听地址和配置概况喵
    print(
        f"\nautoapi 启动喵~\n"
        f"  监听地址：http://{config.server.host}:{config.server.port}\n"
        f"  虚拟模型：{len(config.virtual_models)} 个 → {', '.join(config.virtual_models)}\n"
        f"  故障规则：{len(config.rules)} 条\n"
        f"  超时预算：流式 {config.server.stream_timeout:.0f} 秒 / "
        f"非流式 {config.server.nonstream_timeout:.0f} 秒 / "
        f"静默上限 {config.server.stall_timeout:.0f} 秒\n"
        f"  配置热重载：{'每 ' + str(config.server.reload_poll_interval) + ' 秒检查一次' if config.server.reload_poll_interval > 0 else '已关闭'}\n"
    )
    # 喵~防御：绑定到非回环地址时大声警告，因为代理里存着所有上游的真 api key，
    # 一旦对外暴露且没有鉴权，等于把 key 免费送人喵
    if config.server.host not in ("127.0.0.1", "localhost", "::1"):
        print(
            f"  ⚠ 警告喵：现在绑定的是 {config.server.host}，不是本地回环地址！\n"
            f"    这个代理不校验客户端身份，任何能访问到这个端口的人都能免费用掉你所有上游的额度，\n"
            f"    而且能通过它把请求发到任意上游。除非外层已经有防火墙或反向代理鉴权，\n"
            f"    否则强烈建议改回 127.0.0.1 喵。\n"
        )
    # 构造 uvicorn 配置喵
    uvicorn_config = uvicorn.Config(
        # 要跑的应用对象喵
        app=app,
        # 监听地址喵
        host=config.server.host,
        # 监听端口喵
        port=config.server.port,
        # 关掉 uvicorn 自己的访问日志，我们在 proxy 层已经打了更有用的日志喵
        access_log=False,
        # 日志级别跟全局保持一致喵
        log_level="info",
    )
    # 创建服务器对象，这样才能在 REPL 敲 quit 后主动停掉它喵
    server = uvicorn.Server(uvicorn_config)
    # 需要开 REPL 时启动它，并挂一个看门线程负责在 quit 后停服务喵
    if not args.no_repl:
        # 启动 REPL 线程，拿到 repl 对象喵
        repl = start_repl_thread(state)

        def watch_exit() -> None:
            """
            等 REPL 的退出信号，收到就通知 uvicorn 优雅停机喵~

            为什么要先等 server.started：
                uvicorn 的 _serve() 里是这么写的 ——
                    await self.startup()
                    if self.should_exit: return     # 直接返回，跳过了 shutdown()
                    await self.main_loop()
                    await self.shutdown()
                也就是说，如果在 startup 刚做完的那一刻 should_exit 已经是 True，
                uvicorn 会直接返回而不调用 shutdown()。而 shutdown() 才是通知 lifespan
                收尾的那一步，跳过它就意味着我们在 lifespan 里写的清理逻辑
                （关掉 httpx 客户端、取消热重载任务）根本不会执行，
                同时 lifespan 任务会因为收不到关闭消息而被硬取消，在终端上吐一串
                CancelledError 的 traceback 喵。

                手敲命令时人的手速不可能这么快，所以平时看不见；但用管道把命令喂进来
                （比如脚本化测试）会稳定踩中。所以这里先等服务真正起来再置标志喵。
            """
            # 阻塞等待 REPL 的退出事件被置位喵
            repl.should_exit.wait()
            # 等 uvicorn 真正完成启动，最多等 10 秒（100 次 × 0.1 秒）喵
            for _ in range(100):
                # started 为 True 说明 startup 已经做完，此时置标志会走完整的停机流程喵
                if server.started:
                    break
                # 还没起来就再等一小会喵
                time.sleep(0.1)
            # 置位 uvicorn 的退出标志，它会在当前请求处理完后优雅停机喵
            # 喵~防御：即使上面等超时了也照样置位，免得主人敲了 quit 却怎么都退不出去喵
            server.should_exit = True

        # 起一个 daemon 线程专门等这个信号，不能放在 REPL 线程里因为那边要继续读输入喵
        threading.Thread(target=watch_exit, name="autoapi-exit-watch", daemon=True).start()
    # 跑起来，这一行会阻塞直到服务停机喵
    try:
        server.run()
    # 喵~防御：主线程收到 Ctrl+C 时正常退出，不打印一堆 traceback 喵
    except KeyboardInterrupt:
        print("\n收到 Ctrl+C，代理已停止喵~")
    # 正常退出码喵
    return 0


# 只有直接运行这个文件时才启动，被 import 时不执行喵
if __name__ == "__main__":
    # 用 main 的返回值当进程退出码喵
    sys.exit(main())
