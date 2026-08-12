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
import re

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
    "MCP_TOOL_PAGE_MAX_BYTES",
)

LOCAL_TOOL_CURSOR_PREFIX = "xiaozhi-page:"
# 小智服务器目前可稳定接受约 32 KiB 的单条工具列表消息。
DEFAULT_TOOL_PAGE_MAX_BYTES = 32 * 1024
MAX_TOOL_PAGE_MAX_BYTES = 32 * 1024
MIN_TOOL_PAGE_MAX_BYTES = 4 * 1024
FALLBACK_TOOL_DESCRIPTION_MAX_CHARS = 1024
FALLBACK_PARAMETER_DESCRIPTION_MAX_CHARS = 512
MINIMAL_SCHEMA_KEYS = {
    "$defs",
    "$ref",
    "additionalProperties",
    "allOf",
    "anyOf",
    "const",
    "enum",
    "items",
    "oneOf",
    "properties",
    "required",
    "type",
}
MAX_ENUM_ITEMS = 32
SEARCH_RESULT_MAX_BYTES = 24 * 1024
ROUTER_SEARCH_TOOL = "ha_search_tools"
ROUTER_CALL_TOOL = "ha_call_tool"

ROUTER_TOOLS = [
    {
        "name": ROUTER_SEARCH_TOOL,
        "description": (
            "在执行 Home Assistant 任务前先调用此工具。根据用户目标搜索全部 HA 工具目录，"
            "返回最匹配工具的准确名称、完整用途、参数说明和必填字段。query 应描述要完成的动作和对象，"
            "例如‘查询实体状态’、‘创建自动化’、‘管理插件’。不要猜测真实工具名。"
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": "要完成的 Home Assistant 任务，可使用中文或英文并包含动作与对象。",
                },
                "limit": {
                    "type": "integer",
                    "description": "返回候选数量，通常使用 3；复杂任务最多 8。",
                    "minimum": 1,
                    "maximum": 8,
                    "default": 3,
                },
            },
            "required": ["query"],
            "additionalProperties": False,
        },
    },
    {
        "name": ROUTER_CALL_TOOL,
        "description": (
            "执行 ha_search_tools 返回的真实 Home Assistant 工具。tool_name 必须逐字使用搜索结果中的名称，"
            "arguments 必须遵循该结果的 inputSchema。除非已经知道准确名称和参数，否则先搜索再执行；"
            "一个任务选定工具后不要继续尝试无关工具。"
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "tool_name": {
                    "type": "string",
                    "description": "ha_search_tools 返回的真实 HA 工具名称。",
                },
                "arguments": {
                    "type": "object",
                    "description": "传给真实 HA 工具的参数对象，字段必须符合搜索结果中的 inputSchema。",
                    "additionalProperties": True,
                },
            },
            "required": ["tool_name", "arguments"],
            "additionalProperties": False,
        },
    },
]

SEARCH_ALIASES = {
    "灯": "light switch entity state turn on off",
    "开关": "switch entity state turn on off",
    "插座": "switch outlet entity state turn on off",
    "空调": "climate temperature thermostat entity state",
    "温度": "temperature climate sensor entity state",
    "状态": "get entity state query",
    "实体": "entity state device",
    "设备": "device entity state",
    "区域": "area floor",
    "楼层": "floor area",
    "自动化": "automation config",
    "脚本": "script config",
    "场景": "scene config",
    "插件": "addon manage supervisor",
    "蓝图": "blueprint import",
    "日历": "calendar event",
    "摄像头": "camera image",
    "仪表盘": "dashboard config",
    "能源": "energy prefs",
    "助手": "helper config",
    "删除": "remove delete",
    "创建": "create set config",
    "修改": "update set config manage",
    "查询": "get list query",
    "打开": "turn on enable",
    "关闭": "turn off disable",
}


class ToolListPager:
    """缓存完整 HA 工具目录，并向小智提供轻量工具路由。"""

    def __init__(self) -> None:
        self.pages: list[list[dict]] = []
        self.tools: list[dict] = []
        self.tools_by_name: dict[str, dict] = {}
        self.max_bytes = self._read_page_limit()

    @staticmethod
    def _read_page_limit() -> int:
        """读取单页上限，并强制限制在小智可接受的范围内。"""
        raw_value = os.environ.get(
            "MCP_TOOL_PAGE_MAX_BYTES",
            str(DEFAULT_TOOL_PAGE_MAX_BYTES),
        )
        try:
            requested = int(raw_value)
            if requested > MAX_TOOL_PAGE_MAX_BYTES:
                logger.warning(
                    "MCP_TOOL_PAGE_MAX_BYTES=%d is too large; clamping to %d",
                    requested,
                    MAX_TOOL_PAGE_MAX_BYTES,
                )
            return min(
                MAX_TOOL_PAGE_MAX_BYTES,
                max(MIN_TOOL_PAGE_MAX_BYTES, requested),
            )
        except (TypeError, ValueError):
            logger.warning(
                "Invalid MCP_TOOL_PAGE_MAX_BYTES=%r; using %d",
                raw_value,
                DEFAULT_TOOL_PAGE_MAX_BYTES,
            )
            return DEFAULT_TOOL_PAGE_MAX_BYTES

    def reset(self) -> None:
        """开始一次新的 tools/list 发现流程。"""
        self.pages = []
        self.tools = []
        self.tools_by_name = {}

    @staticmethod
    def _searchable_text(value: object) -> str:
        """提取工具 schema 中有助于检索的字段名和说明。"""
        if isinstance(value, dict):
            parts: list[str] = []
            for key, item in value.items():
                if key in {"description", "properties", "required"}:
                    parts.append(str(key))
                    parts.append(ToolListPager._searchable_text(item))
            return " ".join(parts)
        if isinstance(value, list):
            return " ".join(ToolListPager._searchable_text(item) for item in value)
        return str(value)

    @staticmethod
    def _expand_query(query: str) -> str:
        """补充常见中文智能家居词汇对应的 HA 英文检索词。"""
        expanded = [query.lower()]
        for chinese, english in SEARCH_ALIASES.items():
            if chinese in query:
                expanded.append(english)
        return " ".join(expanded)

    def search_tools(self, query: str, limit: int = 3) -> list[dict]:
        """从完整目录中按名称、用途及参数语义检索真实工具。"""
        expanded = self._expand_query(query.strip())
        tokens = {
            token
            for token in re.findall(r"[\w.-]+|[\u4e00-\u9fff]+", expanded)
            if len(token) > 1
        }
        ranked: list[tuple[int, str, dict]] = []
        for tool in self.tools:
            name = str(tool.get("name", "")).lower()
            description = str(tool.get("description", "")).lower()
            schema_text = self._searchable_text(tool.get("inputSchema", {})).lower()
            score = 0
            for token in tokens:
                if token in name:
                    score += 12
                if token in description:
                    score += 5
                if token in schema_text:
                    score += 2
            if expanded and expanded in f"{name} {description}":
                score += 20
            if score:
                ranked.append((score, name, tool))

        ranked.sort(key=lambda item: (-item[0], item[1]))
        return [tool for _, _, tool in ranked[:limit]]

    @staticmethod
    def call_result(request_id: object, payload: object, is_error: bool = False) -> dict:
        """构造标准 MCP tools/call 文本结果。"""
        text = payload if isinstance(payload, str) else json.dumps(
            payload, ensure_ascii=False, separators=(",", ":")
        )
        return {
            "jsonrpc": "2.0",
            "id": request_id,
            "result": {
                "content": [{"type": "text", "text": text}],
                "isError": is_error,
            },
        }

    def search_response(self, request_id: object, arguments: object) -> dict:
        """本地执行工具搜索，并控制返回消息大小。"""
        if not isinstance(arguments, dict) or not str(arguments.get("query", "")).strip():
            return self.call_result(request_id, "query 不能为空", True)

        query = str(arguments["query"]).strip()
        try:
            limit = min(8, max(1, int(arguments.get("limit", 3))))
        except (TypeError, ValueError):
            limit = 3
        matches = self.search_tools(query, limit)
        if not matches:
            return self.call_result(
                request_id,
                {"query": query, "matches": [], "hint": "请换用更具体的动作或对象关键词。"},
            )

        result_tools: list[dict] = []
        for tool in matches:
            candidate = result_tools + [self.prepare_tool(tool)]
            payload = {"query": query, "matches": candidate}
            if len(json.dumps(payload, ensure_ascii=False).encode("utf-8")) > SEARCH_RESULT_MAX_BYTES:
                break
            result_tools = candidate

        if not result_tools:
            result_tools = [self.minimal_tool(matches[0])]
        logger.info(
            "Tool search %r returned %d candidate(s): %s",
            query,
            len(result_tools),
            ", ".join(str(tool.get("name", "")) for tool in result_tools),
        )
        return self.call_result(
            request_id,
            {
                "query": query,
                "matches": result_tools,
                "instruction": "选择一个最匹配工具，然后用 ha_call_tool 执行。",
            },
        )

    @staticmethod
    def compact_tool(tool: object) -> dict:
        """保留工具语义，仅删除会异常膨胀工具列表的元数据。"""
        if not isinstance(tool, dict):
            return {}

        # 小智选工具只需要名称、用途和输入参数；输出 schema、图标等元数据
        # 不参与调用决策，避免它们挤占 WebSocket 消息空间。
        compacted = {
            key: tool[key]
            for key in ("name", "description", "inputSchema")
            if key in tool
        }
        # 实体 ID 等动态枚举可能包含数百项，是工具消息过大的主要原因。
        # 保留工具与参数说明，只移除超长枚举、示例和重复标题。
        if isinstance(compacted.get("inputSchema"), dict):
            compacted["inputSchema"] = ToolListPager._compact_schema(
                compacted["inputSchema"]
            )
        return compacted

    @staticmethod
    def _compact_schema(value: object) -> object:
        """递归保留参数语义，只移除高体积且不影响选工具的信息。"""
        if isinstance(value, dict):
            return {
                key: ToolListPager._compact_schema(item)
                for key, item in value.items()
                if key not in {
                    "title",
                    "examples",
                }
                and not (
                    key == "enum"
                    and isinstance(item, list)
                    and len(item) > MAX_ENUM_ITEMS
                )
            }
        if isinstance(value, list):
            return [ToolListPager._compact_schema(item) for item in value]
        return value

    @staticmethod
    def _truncate_schema_descriptions(value: object) -> object:
        """单个工具过大时缩短参数说明，仍保留参数用途和调用结构。"""
        if isinstance(value, dict):
            compacted: dict[str, object] = {}
            for key, item in value.items():
                if key in {"title", "examples"}:
                    continue
                if key == "enum" and isinstance(item, list) and len(item) > MAX_ENUM_ITEMS:
                    continue
                if key == "description" and isinstance(item, str):
                    compacted[key] = item[:FALLBACK_PARAMETER_DESCRIPTION_MAX_CHARS].rstrip()
                    continue
                compacted[key] = ToolListPager._truncate_schema_descriptions(item)
            return compacted
        if isinstance(value, list):
            return [ToolListPager._truncate_schema_descriptions(item) for item in value]
        return value

    @staticmethod
    def _minimal_schema(value: object) -> object:
        """生成极简 schema，让全部工具能在小智单页限制内发布。"""
        if isinstance(value, dict):
            compacted: dict[str, object] = {}
            for key, item in value.items():
                if key not in MINIMAL_SCHEMA_KEYS:
                    continue
                if key == "enum" and isinstance(item, list) and len(item) > MAX_ENUM_ITEMS:
                    # 超长实体枚举会占满工具列表；调用时仍由 HA 校验实际值。
                    continue
                compacted[key] = ToolListPager._minimal_schema(item)
            return compacted
        if isinstance(value, list):
            return [ToolListPager._minimal_schema(item) for item in value]
        return value

    @classmethod
    def minimal_tool(cls, tool: object) -> dict:
        """只保留工具调用必需字段，不改变工具名称或实际转发。"""
        if not isinstance(tool, dict):
            return {}
        minimal: dict[str, object] = {"name": str(tool.get("name", ""))}
        schema = tool.get("inputSchema")
        minimal["inputSchema"] = cls._minimal_schema(
            schema if isinstance(schema, dict) else {"type": "object"}
        )
        description = tool.get("description")
        if isinstance(description, str):
            minimal["description"] = description[:FALLBACK_TOOL_DESCRIPTION_MAX_CHARS].rstrip()
        return minimal

    def prepare_tool(self, tool: object) -> dict:
        """为单个工具选择信息最完整、同时不超过消息上限的表示。"""
        semantic = self.compact_tool(tool)
        if len(self._encode_response(None, [semantic])) <= self.max_bytes:
            return semantic

        # 极少数工具自身包含很大的 schema；先缩短说明，不影响其他工具。
        fallback = dict(semantic)
        description = fallback.get("description")
        if isinstance(description, str):
            fallback["description"] = description[:FALLBACK_TOOL_DESCRIPTION_MAX_CHARS].rstrip()
        schema = fallback.get("inputSchema")
        if isinstance(schema, dict):
            fallback["inputSchema"] = self._truncate_schema_descriptions(schema)
        if len(self._encode_response(None, [fallback])) <= self.max_bytes:
            logger.warning(
                "Tool %s exceeded the page limit; parameter descriptions were shortened",
                fallback.get("name", "<unnamed>"),
            )
            return fallback

        # 最后兜底只影响这个超大工具，正常工具仍保留完整语义。
        minimal = self.minimal_tool(tool)
        if len(self._encode_response(None, [minimal])) <= self.max_bytes:
            logger.warning(
                "Tool %s still exceeded the page limit; using minimal schema for this tool only",
                minimal.get("name", "<unnamed>"),
            )
            return minimal

        # 理论上的异常兜底：确保任何单个工具都不会再次触发 WebSocket 1009。
        emergency = {
            "name": minimal.get("name", ""),
            "description": str(minimal.get("description", ""))[:256],
            "inputSchema": {"type": "object", "additionalProperties": True},
        }
        logger.warning(
            "Tool %s has an exceptionally large schema; using an open schema for this tool only",
            emergency["name"],
        )
        return emergency

    def _encode_response(
        self,
        request_id: object,
        tools: list[dict],
        next_cursor: str | None = None,
    ) -> bytes:
        result: dict[str, object] = {"tools": tools}
        if next_cursor:
            result["nextCursor"] = next_cursor
        response = {"jsonrpc": "2.0", "id": request_id, "result": result}
        return json.dumps(response, ensure_ascii=False, separators=(",", ":")).encode(
            "utf-8"
        )

    def cache_tools(self, tools: list[object]) -> None:
        """保存 HA 完整目录；小智只需发现两个路由工具。"""
        self.tools = [tool for tool in tools if isinstance(tool, dict)]
        self.tools_by_name = {
            str(tool.get("name")): tool
            for tool in self.tools
            if tool.get("name")
        }
        self.pages = [[dict(tool) for tool in ROUTER_TOOLS]]
        logger.info(
            "Tool router ready: %d HA tools cached, exposing %d router tools to XiaoZhi",
            len(self.tools),
            len(ROUTER_TOOLS),
        )

    def response_for(self, request_id: object, cursor: str | None = None) -> dict | None:
        """根据小智返回的 cursor 构造下一页响应。"""
        page_index = 0
        if cursor:
            if not cursor.startswith(LOCAL_TOOL_CURSOR_PREFIX):
                return None
            try:
                page_index = int(cursor[len(LOCAL_TOOL_CURSOR_PREFIX) :])
            except ValueError:
                return None

        if page_index < 0 or page_index >= len(self.pages):
            return None

        result: dict[str, object] = {"tools": self.pages[page_index]}
        if page_index + 1 < len(self.pages):
            result["nextCursor"] = f"{LOCAL_TOOL_CURSOR_PREFIX}{page_index + 1}"
        return {"jsonrpc": "2.0", "id": request_id, "result": result}

    def first_response(self, message: dict) -> dict:
        """把 mcp-proxy 的完整 tools/list 响应替换成第一页。"""
        result = message.get("result") or {}
        tools = result.get("tools") if isinstance(result, dict) else None
        if not isinstance(tools, list):
            return message

        self.cache_tools(tools)
        response = self.response_for(message.get("id"))
        return response or message


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
            tool_pager = ToolListPager()
            send_lock = asyncio.Lock()
            tasks = {
                asyncio.create_task(
                    pipe_websocket_to_process(websocket, process, tool_pager, send_lock)
                ),
                asyncio.create_task(
                    pipe_process_to_websocket(process, websocket, tool_pager, send_lock)
                ),
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

async def pipe_websocket_to_process(websocket, process, tool_pager, send_lock):
    """Read data from WebSocket and write to process stdin"""
    try:
        while True:
            # Read message from WebSocket
            message = await websocket.recv()
            logger.debug(f"<< {message[:120]}...")

            # Write to process stdin (in text mode)
            if isinstance(message, bytes):
                message = message.decode('utf-8')

            # 小智请求下一页时，直接从缓存响应，不再让 mcp-proxy 重复拉取整张工具表。
            try:
                request = json.loads(message)
            except (TypeError, json.JSONDecodeError):
                request = None

            if isinstance(request, dict) and request.get("method") == "tools/list":
                params = request.get("params") or {}
                cursor = params.get("cursor") if isinstance(params, dict) else None
                if cursor:
                    response = tool_pager.response_for(request.get("id"), cursor)
                    if response is not None:
                        page_number = int(cursor[len(LOCAL_TOOL_CURSOR_PREFIX) :]) + 1
                        logger.info(
                            "Serving cached tools/list page %d/%d to XiaoZhi",
                            page_number,
                            len(tool_pager.pages),
                        )
                        async with send_lock:
                            await websocket.send(
                                json.dumps(response, ensure_ascii=False, separators=(",", ":"))
                            )
                        continue
                else:
                    # 首次 tools/list 需要放行给 mcp-proxy，从 HA 获取工具目录。
                    tool_pager.reset()

            if isinstance(request, dict) and request.get("method") == "tools/call":
                params = request.get("params") or {}
                tool_name = params.get("name") if isinstance(params, dict) else None
                arguments = params.get("arguments") if isinstance(params, dict) else None

                if tool_name == ROUTER_SEARCH_TOOL:
                    response = tool_pager.search_response(request.get("id"), arguments)
                    async with send_lock:
                        await websocket.send(
                            json.dumps(response, ensure_ascii=False, separators=(",", ":"))
                        )
                    continue

                if tool_name == ROUTER_CALL_TOOL:
                    route_args = arguments if isinstance(arguments, dict) else {}
                    target_name = str(route_args.get("tool_name", "")).strip()
                    target_arguments = route_args.get("arguments")
                    if target_name not in tool_pager.tools_by_name:
                        response = tool_pager.call_result(
                            request.get("id"),
                            {
                                "error": "未知的 HA 工具名称，请先调用 ha_search_tools。",
                                "tool_name": target_name,
                            },
                            True,
                        )
                        async with send_lock:
                            await websocket.send(
                                json.dumps(response, ensure_ascii=False, separators=(",", ":"))
                            )
                        continue
                    if not isinstance(target_arguments, dict):
                        response = tool_pager.call_result(
                            request.get("id"),
                            "arguments 必须是 JSON 对象，请按照搜索结果的 inputSchema 传参。",
                            True,
                        )
                        async with send_lock:
                            await websocket.send(
                                json.dumps(response, ensure_ascii=False, separators=(",", ":"))
                            )
                        continue

                    request["params"] = {
                        "name": target_name,
                        "arguments": target_arguments,
                    }
                    message = json.dumps(
                        request, ensure_ascii=False, separators=(",", ":")
                    )
                    logger.info("Routing XiaoZhi call to HA tool: %s", target_name)

            process.stdin.write(message + '\n')
            process.stdin.flush()
    except Exception as e:
        logger.error(f"Error in WebSocket to process pipe: {e}")
        raise  # Re-throw exception to trigger reconnection
    finally:
        # Close process stdin
        if not process.stdin.closed:
            process.stdin.close()

async def pipe_process_to_websocket(process, websocket, tool_pager, send_lock):
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

            # tools/list 由桥接层分页后再发给小智，其他 JSON-RPC 消息原样转发。
            logger.debug(f">> {data[:120]}...")
            try:
                response = json.loads(data)
            except (TypeError, json.JSONDecodeError):
                response = None

            result = response.get("result") if isinstance(response, dict) else None
            if isinstance(result, dict) and isinstance(result.get("tools"), list):
                response = tool_pager.first_response(response)
                data = json.dumps(response, ensure_ascii=False, separators=(",", ":"))

            async with send_lock:
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
