#!/app/.venv/bin/python3
# -*- coding: utf-8 -*-

"""
在小智官方 MCP WebSocket 接入点和 Home Assistant MCP 服务之间转发 JSON-RPC。

支持两种 Home Assistant MCP 传输方式：
  - 旧版 SSE：例如 /mcp_server/sse
  - 新版 Streamable HTTP：例如 HA-MCP 的 /private_xxx 或 /api/mcp
"""

import asyncio
import websockets
import subprocess
import json
import logging
import os
import signal
import sys
import random

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('MCP_PIPE')

# Reconnection settings
INITIAL_BACKOFF = 1  # Initial wait time in seconds
MAX_BACKOFF = 60  # Maximum wait time in seconds
reconnect_attempt = 0
backoff = INITIAL_BACKOFF
APP_OPTION_KEYS = (
    "XIAOZHI_MCP_ENDPOINT",
    "HA_MCP_ENDPOINT",
    "API_ACCESS_TOKEN",
    "MCP_TRANSPORT",
)


def load_app_options(path: str = "/data/options.json") -> dict[str, str]:
    """读取 Home Assistant App 页面保存的配置。"""
    try:
        with open(path, encoding="utf-8") as options_file:
            raw_options = json.load(options_file)
    except FileNotFoundError:
        # 本程序也支持普通 Docker 运行，此时可以只使用环境变量。
        return {}
    except (OSError, json.JSONDecodeError) as error:
        logger.warning("Unable to read Home Assistant app options: %s", error)
        return {}

    if not isinstance(raw_options, dict):
        logger.warning("Home Assistant app options must be a JSON object")
        return {}

    return {
        key: str(raw_options[key])
        for key in APP_OPTION_KEYS
        if raw_options.get(key) is not None
    }


def apply_app_options(options: dict[str, str]) -> None:
    """将 App 配置覆盖到环境变量，供桥接程序和 mcp-proxy 使用。"""
    for key, value in options.items():
        # 空值保留 Docker/普通 Docker 运行时的环境变量回退值。
        if value.strip():
            os.environ[key] = value


def normalize_endpoint(value: str, name: str) -> str:
    """清理配置中的首尾空白，避免空格被编码成 %20 导致 404。"""
    endpoint = value.strip()
    if endpoint != value:
        logger.warning("%s contained surrounding whitespace; it has been removed", name)
    return endpoint


def resolve_transport(endpoint: str) -> str:
    """根据环境变量或 URL 自动选择 MCP 传输协议。"""
    configured = os.environ.get("MCP_TRANSPORT", "auto").strip().lower()
    if configured in {"sse", "streamablehttp"}:
        return configured
    if configured != "auto":
        raise ValueError("MCP_TRANSPORT must be auto, sse, or streamablehttp")

    # 旧版 HA 官方 MCP 使用 SSE 路径；新版 HA-MCP 默认使用 Streamable HTTP。
    path = endpoint.rstrip("/").lower()
    if path.endswith("/sse") or "/mcp_server/sse" in path:
        return "sse"
    return "streamablehttp"


def build_proxy_command(endpoint: str) -> list[str]:
    """构造 mcp-proxy 命令，并保留 API_ACCESS_TOKEN 环境变量认证。"""
    transport = resolve_transport(endpoint)
    command = ["mcp-proxy", "--transport", transport, endpoint]
    logger.info("Starting mcp-proxy with %s transport", transport)
    return command

async def connect_with_retry(uri):
    """Connect to WebSocket server with retry mechanism"""
    global reconnect_attempt, backoff
    while True:  # Infinite reconnection
        try:
            if reconnect_attempt > 0:
                wait_time = backoff * (1 + random.random() * 0.1)  # Add some random jitter
                logger.info(f"Waiting {wait_time:.2f} seconds before reconnection attempt {reconnect_attempt}...")
                await asyncio.sleep(wait_time)

            # Attempt to connect
            await connect_to_server(uri)

        except Exception as e:
            reconnect_attempt += 1
            logger.warning(f"Connection closed (attempt: {reconnect_attempt}): {e}")
            # Calculate wait time for next reconnection (exponential backoff)
            backoff = min(backoff * 2, MAX_BACKOFF)

async def connect_to_server(uri):
    """Connect to WebSocket server and establish bidirectional communication with `mcp_script`"""
    global reconnect_attempt, backoff
    try:
        logger.info(f"Connecting to WebSocket server...")
        async with websockets.connect(uri) as websocket:
            logger.info(f"Successfully connected to WebSocket server")

            # Reset reconnection counter if connection closes normally
            reconnect_attempt = 0
            backoff = INITIAL_BACKOFF

            # 启动 MCP 代理；子进程会继承 API_ACCESS_TOKEN 等环境变量。
            process = subprocess.Popen(
                build_proxy_command(mcp_script),
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True  # Use text mode
            )
            logger.info("Started mcp-proxy process")

            # 任意一侧断开都要取消另外两个任务，避免外层 WebSocket 假在线。
            tasks = {
                asyncio.create_task(pipe_websocket_to_process(websocket, process)),
                asyncio.create_task(pipe_process_to_websocket(process, websocket)),
                asyncio.create_task(pipe_process_stderr_to_terminal(process)),
            }
            done, pending = await asyncio.wait(
                tasks, return_when=asyncio.FIRST_COMPLETED
            )
            for task in pending:
                task.cancel()
            await asyncio.gather(*pending, return_exceptions=True)

            for task in done:
                exception = task.exception()
                if exception:
                    raise exception
            raise RuntimeError("MCP proxy process or pipe ended")
    except websockets.exceptions.ConnectionClosed as e:
        logger.error(f"WebSocket connection closed: {e}")
        raise  # Re-throw exception to trigger reconnection
    except Exception as e:
        logger.error(f"Connection error: {e}")
        raise  # Re-throw exception
    finally:
        # Ensure the child process is properly terminated
        if 'process' in locals():
            logger.info("Terminating mcp-proxy process")
            try:
                process.terminate()
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
            logger.info("mcp-proxy process terminated")

async def pipe_websocket_to_process(websocket, process):
    """Read data from WebSocket and write to process stdin"""
    try:
        while True:
            # Read message from WebSocket
            message = await websocket.recv()
            logger.debug(f"<< {message[:120]}...")

            # Write to process stdin (in text mode)
            if isinstance(message, bytes):
                message = message.decode('utf-8')
            process.stdin.write(message + '\n')
            process.stdin.flush()
    except Exception as e:
        logger.error(f"Error in WebSocket to process pipe: {e}")
        raise  # Re-throw exception to trigger reconnection
    finally:
        # Close process stdin
        if not process.stdin.closed:
            process.stdin.close()

async def pipe_process_to_websocket(process, websocket):
    """Read data from process stdout and send to WebSocket"""
    try:
        while True:
            # Read data from process stdout
            data = await asyncio.get_event_loop().run_in_executor(
                None, process.stdout.readline
            )

            if not data:  # If no data, the process may have ended
                logger.info("Process has ended output")
                break

            # Send data to WebSocket
            logger.debug(f">> {data[:120]}...")
            # In text mode, data is already a string, no need to decode
            await websocket.send(data)
    except Exception as e:
        logger.error(f"Error in process to WebSocket pipe: {e}")
        raise  # Re-throw exception to trigger reconnection

async def pipe_process_stderr_to_terminal(process):
    """Read data from process stderr and print to terminal"""
    try:
        while True:
            # Read data from process stderr
            data = await asyncio.get_event_loop().run_in_executor(
                None, process.stderr.readline
            )

            if not data:  # If no data, the process may have ended
                logger.info("Process has ended stderr output")
                break

            # Print stderr data to terminal (in text mode, data is already a string)
            sys.stderr.write(data)
            sys.stderr.flush()
    except Exception as e:
        logger.error(f"Error in process stderr pipe: {e}")
        raise  # Re-throw exception to trigger reconnection

def signal_handler(sig, frame):
    """Handle interrupt signals"""
    logger.info("Received interrupt signal, shutting down...")
    sys.exit(0)

if __name__ == "__main__":
    # Register signal handler
    signal.signal(signal.SIGINT, signal_handler)

    # Home Assistant App 的 UI 配置保存在 /data/options.json，不会自动注入环境变量。
    apply_app_options(load_app_options())

    # mcp_script
    ha_endpoint_url = os.environ.get('HA_MCP_ENDPOINT')
    if not ha_endpoint_url :
        logger.error("Please set the `HA_MCP_ENDPOINT` environment variable")
        sys.exit(1)

    # Get token from environment variable or command line arguments
    endpoint_url = os.environ.get('XIAOZHI_MCP_ENDPOINT')
    if not endpoint_url:
        logger.error("Please set the `XIAOZHI_MCP_ENDPOINT` environment variable")
        sys.exit(1)

    # 配置值末尾的空格会变成 URL 中的 %20，必须在启动前清理。
    mcp_script = normalize_endpoint(ha_endpoint_url, "HA_MCP_ENDPOINT")
    endpoint_url = normalize_endpoint(endpoint_url, "XIAOZHI_MCP_ENDPOINT")

    if not mcp_script or not endpoint_url:
        logger.error("MCP endpoint cannot be empty")
        sys.exit(1)

    # mcp-proxy 0.7+ 支持 Streamable HTTP；旧 SSE 地址仍可通过 auto 自动识别。
    try:
        transport = resolve_transport(mcp_script)
    except ValueError as error:
        logger.error(str(error))
        sys.exit(1)
    logger.info("HA MCP transport selected: %s", transport)

    if os.environ.get("API_ACCESS_TOKEN"):
        logger.info("Home Assistant API token is configured")
    elif transport == "streamablehttp" and "/private_" not in mcp_script:
        logger.warning(
            "Streamable HTTP endpoint is configured without API_ACCESS_TOKEN; "
            "authentication may fail"
        )
    # Start main loop
    try:
        asyncio.run(connect_with_retry(endpoint_url))
    except KeyboardInterrupt:
        logger.info("Program interrupted by user")
    except Exception as e:
        logger.error(f"Program execution error: {e}")
