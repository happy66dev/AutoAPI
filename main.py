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
# sys 用来控制退出码喵
import sys
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


def setup_logging() -> None:
    """配置全局日志格式喵~"""
    # 设成 INFO 级别，能看到每条请求走了哪个候选，又不会被 DEBUG 噪音淹没喵
    logging.basicConfig(
        # 日志级别喵
        level=logging.INFO,
        # 日志格式：时间 级别 来源 消息，时间精确到秒足够排查了喵
        format="%(asctime)s %(levelname)-7s %(name)-15s %(message)s",
        # 时间格式，只留时分秒让日志行短一点喵
        datefmt="%H:%M:%S",
    )
    # 把 httpx 自己的日志压到 WARNING，否则每条请求它都会打一行，太吵喵
    logging.getLogger("httpx").setLevel(logging.WARNING)
    # httpcore 是 httpx 的底层，日志更细碎，一并压掉喵
    logging.getLogger("httpcore").setLevel(logging.WARNING)


def parse_args() -> argparse.Namespace:
    """解析命令行参数喵~"""
    # 创建解析器，描述文本会出现在 -h 的输出里喵
    parser = argparse.ArgumentParser(description="autoapi —— LLM API 故障转移代理喵~")
    # -c/--config 指定配置文件路径，默认 config.yaml 喵
    parser.add_argument(
        "-c",
        "--config",
        default="config.yaml",
        help="配置文件路径，默认 config.yaml 喵",
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
    # 加载配置喵
    try:
        config = load_config(args.config)
    # 喵~防御：配置有问题时打印明确的中文原因并以退出码 1 结束，不带一堆 traceback 吓人喵
    except ConfigError as exc:
        print(f"启动失败喵：{exc}", file=sys.stderr)
        return 1
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
