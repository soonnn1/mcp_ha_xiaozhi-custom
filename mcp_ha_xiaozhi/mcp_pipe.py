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
ROUTER_DISCOVER_TOOL = "ha_discover"
ROUTER_HELP_TOOL = "ha_get_tool_help"
ROUTER_EXECUTE_TOOL = "ha_execute"

# 兼容已经缓存旧版工具列表的小智会话；旧名称不再暴露，但仍可继续调用。
LEGACY_ROUTER_SEARCH_TOOL = "ha_search_tools"
LEGACY_ROUTER_CALL_TOOL = "ha_call_tool"

ROUTER_TOOLS = [
    {
        "name": ROUTER_DISCOVER_TOOL,
        "description": (
            "Home Assistant 任务的第一步。根据用户目标发现相关 HA MCP 工具和推荐执行流程。"
            "每个新任务先调用一次；复杂任务和失败恢复时可以带着新线索重复调用。返回结果后，"
            "先用 ha_get_tool_help 查看准备调用工具的完整参数，再用 ha_execute 执行。"
            "对于音乐、媒体等集成服务，本工具会给出 list_services/get_integration/call_service 的组合步骤。"
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
                    "description": "返回候选数量，通常使用 5；复杂任务最多 8。",
                    "minimum": 1,
                    "maximum": 8,
                    "default": 5,
                },
                "context": {
                    "type": "string",
                    "description": "可选。上一步结果、错误信息或已经知道的实体/服务名称，用于继续规划。",
                },
            },
            "required": ["query"],
            "additionalProperties": False,
        },
    },
    {
        "name": ROUTER_HELP_TOOL,
        "description": (
            "读取一个真实 HA MCP 工具的完整用途、inputSchema、必填参数和关联提示。"
            "tool_name 必须来自 ha_discover。调用 ha_execute 前应先读取帮助；参数错误时再次读取。"
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "tool_name": {
                    "type": "string",
                    "description": "ha_discover 返回的真实 HA MCP 工具名称。",
                },
            },
            "required": ["tool_name"],
            "additionalProperties": False,
        },
    },
    {
        "name": ROUTER_EXECUTE_TOOL,
        "description": (
            "执行真实 HA MCP 工具。tool_name 必须来自 ha_discover，arguments 必须严格符合"
            "ha_get_tool_help 返回的 inputSchema。执行结果会回到当前对话；若任务尚未完成，"
            "继续调用 ha_discover、ha_get_tool_help 或 ha_execute，直到得到最终结果。"
            "不要猜参数；错误后根据错误内容修正，不要反复提交完全相同的调用。"
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "tool_name": {
                    "type": "string",
                    "description": "要执行的真实 HA MCP 工具名称。",
                },
                "arguments": {
                    "type": "object",
                    "description": "传给真实工具的参数对象。",
                    "additionalProperties": True,
                },
                "purpose": {
                    "type": "string",
                    "description": "可选。说明本次调用要获得什么结果，帮助保持多步任务方向。",
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
    "音乐": "music media player music_assistant service",
    "歌曲": "music song track media music_assistant search service",
    "歌手": "music artist track media music_assistant search service",
    "专辑": "music album media music_assistant search service",
    "歌单": "music playlist media music_assistant service",
    "播放": "play media player music_assistant call service",
    "暂停": "pause media player music_assistant call service",
    "下一首": "next track media player music_assistant call service",
    "广播": "radio media player music_assistant service",
    "服务": "service list call",
    "集成": "integration config entry",
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


# 不使用额外模型的确定性任务配方。DeepSeek 根据配方逐步调用真实 HA MCP 工具。
TASK_RECIPES = [
    {
        "name": "music_assistant_media",
        "triggers": (
            "音乐", "歌曲", "歌手", "专辑", "歌单", "播放", "暂停", "下一首",
            "music", "song", "track", "artist", "album", "playlist", "media",
        ),
        "tools": ("ha_list_services", "ha_get_integration", "ha_call_service", "ha_search"),
        "workflow": (
            "先用 ha_list_services 查询 music_assistant 域的服务；搜索歌曲不是 ha_search 的用途。",
            "如果服务要求 config_entry_id，用 ha_get_integration 查找 music_assistant 的配置条目。",
            "用 ha_call_service 调用 music_assistant.search；传入服务要求的 name、media_type 等数据并要求返回响应。",
            "需要播放时，再查 media_player 实体并调用 music_assistant.play_media 或对应播放服务。",
        ),
    },
    {
        "name": "entity_control",
        "triggers": (
            "打开", "关闭", "开关", "灯", "插座", "空调", "调温", "亮度",
            "turn on", "turn off", "light", "switch", "climate", "brightness",
        ),
        "tools": ("ha_search", "ha_get_state", "ha_call_service", "ha_bulk_control"),
        "workflow": (
            "用 ha_search 找到准确 entity_id。",
            "必要时用 ha_get_state 确认当前状态和可用属性。",
            "单个设备用 ha_call_service，多个设备用 ha_bulk_control。",
        ),
    },
    {
        "name": "entity_query",
        "triggers": (
            "状态", "温度", "湿度", "电量", "传感器", "历史", "查询实体",
            "state", "temperature", "humidity", "battery", "sensor", "history",
        ),
        "tools": ("ha_search", "ha_get_state", "ha_get_entity", "ha_get_history"),
        "workflow": (
            "先用 ha_search 定位实体。",
            "当前值用 ha_get_state，注册信息用 ha_get_entity，历史趋势用 ha_get_history。",
        ),
    },
    {
        "name": "automation_script_scene",
        "triggers": (
            "自动化", "脚本", "场景", "automation", "script", "scene",
        ),
        "tools": (
            "ha_config_get_automation", "ha_config_set_automation",
            "ha_config_get_script", "ha_config_set_script",
            "ha_config_get_scene", "ha_config_set_scene", "ha_call_service",
        ),
        "workflow": (
            "修改前先读取现有配置；创建或更新时使用对应 config_set 工具。",
            "仅运行已有自动化、脚本或场景时，用 ha_call_service 触发。",
        ),
    },
    {
        "name": "calendar_todo",
        "triggers": ("日历", "事件", "待办", "清单", "calendar", "event", "todo"),
        "tools": (
            "ha_config_get_calendar_events", "ha_config_set_calendar_event",
            "ha_get_todo", "ha_set_todo_item", "ha_remove_todo_item",
        ),
        "workflow": (
            "日历事件使用 calendar 配置工具；待办事项使用 todo 工具。",
            "更新或删除前先查询准确的日历实体或待办项目标识。",
        ),
    },
    {
        "name": "integration_service",
        "triggers": ("集成", "服务", "配置条目", "integration", "service", "config entry"),
        "tools": ("ha_get_integration", "ha_list_services", "ha_call_service", "ha_set_integration"),
        "workflow": (
            "用 ha_get_integration 获取配置条目及其 ID。",
            "用 ha_list_services 查看集成注册的服务和字段，再用 ha_call_service 调用。",
        ),
    },
    {
        "name": "addon_system",
        "triggers": ("插件", "应用", "备份", "更新", "日志", "重启", "addon", "backup", "update", "log", "restart"),
        "tools": (
            "ha_get_addon", "ha_manage_addon", "ha_manage_backup", "ha_manage_updates",
            "ha_get_logs", "ha_get_system_health", "ha_restart",
        ),
        "workflow": (
            "先查询目标和当前状态，再执行管理操作。",
            "重启、删除或恢复备份属于高影响操作，只有用户明确要求时执行。",
        ),
    },
    {
        "name": "dashboard_helper",
        "triggers": ("仪表盘", "面板", "助手", "标签", "分组", "dashboard", "helper", "label", "group"),
        "tools": (
            "ha_config_get_dashboard", "ha_config_set_dashboard", "ha_config_list_helpers",
            "ha_config_set_helper", "ha_config_get_label", "ha_config_set_label",
            "ha_config_list_groups", "ha_config_set_group",
        ),
        "workflow": ("先读取现有配置和准确 ID，再使用对应 set 工具创建或修改。",),
    },
    {
        "name": "camera_energy_area",
        "triggers": ("摄像头", "图片", "能源", "楼层", "区域", "camera", "image", "energy", "floor", "area"),
        "tools": (
            "ha_get_camera_image", "ha_manage_energy_prefs", "ha_list_floors_areas",
            "ha_set_area_or_floor",
        ),
        "workflow": ("先查询可用对象及 ID，再读取图片、能源设置或修改楼层区域。",),
    },
]


class ToolListPager:
    """缓存完整 HA 工具目录，并向小智提供轻量工具路由。"""

    def __init__(self) -> None:
        self.pages: list[list[dict]] = []
        self.tools: list[dict] = []
        self.tools_by_name: dict[str, dict] = {}
        self.pending_calls: dict[object, str] = {}
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
    def _clean_description(description: object, max_chars: int = 1200) -> str:
        """移除每个 HA 工具重复携带的技能前言和声明，只保留实际用途。"""
        if not isinstance(description, str):
            return ""
        cleaned = description.strip()
        cleaned = re.sub(
            r"^IMPORTANT:.*?SYMPTOMS:.*?\n\n",
            "",
            cleaned,
            flags=re.DOTALL,
        )
        cleaned = cleaned.split("\n\nexec tool declaration:", 1)[0].strip()
        cleaned = re.sub(r"\n{3,}", "\n\n", cleaned)
        return cleaned[:max_chars].rstrip()

    @staticmethod
    def _required_fields(tool: dict) -> list[str]:
        schema = tool.get("inputSchema")
        required = schema.get("required") if isinstance(schema, dict) else None
        return [str(item) for item in required] if isinstance(required, list) else []

    @staticmethod
    def _parameter_names(tool: dict) -> list[str]:
        schema = tool.get("inputSchema")
        properties = schema.get("properties") if isinstance(schema, dict) else None
        return [str(item) for item in properties] if isinstance(properties, dict) else []

    def tool_summary(self, tool: dict) -> dict:
        """生成用于发现阶段的短摘要，完整 schema 由帮助工具按需返回。"""
        return {
            "name": str(tool.get("name", "")),
            "summary": self._clean_description(tool.get("description"), 600),
            "required": self._required_fields(tool),
            "parameters": self._parameter_names(tool),
        }

    @staticmethod
    def matching_recipes(query: str, context: str = "") -> list[dict]:
        """按中英文意图词匹配无需模型参与的多步任务配方。"""
        text = f"{query} {context}".lower()
        ranked: list[tuple[int, str, dict]] = []
        for recipe in TASK_RECIPES:
            score = sum(1 for trigger in recipe["triggers"] if trigger.lower() in text)
            if score:
                ranked.append((score, str(recipe["name"]), recipe))
        ranked.sort(key=lambda item: (-item[0], item[1]))
        return [recipe for _, _, recipe in ranked[:3]]

    def discover_response(self, request_id: object, arguments: object) -> dict:
        """返回匹配的任务配方和工具摘要，引导 DeepSeek 继续循环调用。"""
        if not isinstance(arguments, dict) or not str(arguments.get("query", "")).strip():
            return self.call_result(
                request_id,
                {
                    "ok": False,
                    "error_code": "INVALID_QUERY",
                    "error": "query 不能为空，请描述用户要完成的动作和对象。",
                },
                True,
            )

        query = str(arguments["query"]).strip()
        context = str(arguments.get("context", "")).strip()
        try:
            limit = min(8, max(1, int(arguments.get("limit", 5))))
        except (TypeError, ValueError):
            limit = 5

        recipes = self.matching_recipes(query, context)
        selected_names: list[str] = []
        recipe_payload: list[dict] = []
        for recipe in recipes:
            available_tools = [
                name for name in recipe["tools"] if name in self.tools_by_name
            ]
            selected_names.extend(available_tools)
            recipe_payload.append(
                {
                    "name": recipe["name"],
                    "recommended_tools": available_tools,
                    "workflow": list(recipe["workflow"]),
                }
            )

        # 配方工具优先，随后补充目录语义检索结果。
        search_query = f"{query} {context}".strip()
        selected_names.extend(
            str(tool.get("name", ""))
            for tool in self.search_tools(search_query, max(limit * 2, 8))
        )
        unique_names: list[str] = []
        for name in selected_names:
            if name and name not in unique_names and name in self.tools_by_name:
                unique_names.append(name)
            if len(unique_names) >= limit:
                break

        candidates = [self.tool_summary(self.tools_by_name[name]) for name in unique_names]
        if not candidates:
            return self.call_result(
                request_id,
                {
                    "ok": True,
                    "query": query,
                    "recipes": recipe_payload,
                    "candidates": [],
                    "next_action": (
                        "换用动作、对象、集成域或错误中的英文名称再次调用 ha_discover；"
                        "不要直接猜工具名。"
                    ),
                },
            )

        first_name = candidates[0]["name"]
        logger.info(
            "Capability discovery %r returned %d recipe(s) and %d tool(s): %s",
            query,
            len(recipe_payload),
            len(candidates),
            ", ".join(str(item["name"]) for item in candidates),
        )
        return self.call_result(
            request_id,
            {
                "ok": True,
                "query": query,
                "recipes": recipe_payload,
                "candidates": candidates,
                "next_action": {
                    "tool": ROUTER_HELP_TOOL,
                    "arguments": {"tool_name": first_name},
                    "instruction": (
                        "查看准备执行工具的完整 schema。多步配方应按 workflow 顺序执行；"
                        "每次拿到结果后判断任务是否完成，未完成就继续下一步。"
                    ),
                },
            },
        )

    def help_response(self, request_id: object, arguments: object) -> dict:
        """按名称返回单个真实工具的完整调用帮助。"""
        route_args = arguments if isinstance(arguments, dict) else {}
        tool_name = str(route_args.get("tool_name", "")).strip()
        tool = self.tools_by_name.get(tool_name)
        if tool is None:
            return self.call_result(
                request_id,
                {
                    "ok": False,
                    "error_code": "UNKNOWN_TOOL",
                    "error": "未知的 HA MCP 工具名称。",
                    "tool_name": tool_name,
                    "recovery": "调用 ha_discover 重新查找，不要猜测名称。",
                },
                True,
            )

        prepared = self.prepare_tool(tool)
        prepared["description"] = self._clean_description(tool.get("description"), 3000)
        logger.info("Serving full help for HA tool: %s", tool_name)
        return self.call_result(
            request_id,
            {
                "ok": True,
                "tool": prepared,
                "required": self._required_fields(tool),
                "instruction": (
                    "严格按 inputSchema 组织 arguments，然后调用 ha_execute。"
                    "如果该工具返回列表或配置 ID，把结果用于配方的下一步，不要提前结束任务。"
                ),
            },
        )

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
        """兼容 1.0.9 已缓存会话的旧搜索协议。"""
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
        """保存 HA 完整目录；小智只需发现三个代理路由工具。"""
        self.tools = [tool for tool in tools if isinstance(tool, dict)]
        self.tools_by_name = {
            str(tool.get("name")): tool
            for tool in self.tools
            if tool.get("name")
        }
        self.pages = [[dict(tool) for tool in ROUTER_TOOLS]]
        logger.info(
            "Agentic tool router ready: %d HA tools cached, exposing %d loop tools to XiaoZhi",
            len(self.tools),
            len(ROUTER_TOOLS),
        )

    def enrich_routed_response(self, response: object) -> object:
        """给真实工具错误追加恢复指引，帮助 DeepSeek 自主进入下一轮。"""
        if not isinstance(response, dict):
            return response
        request_id = response.get("id")
        target_name = self.pending_calls.pop(request_id, None)
        if not target_name:
            return response

        if isinstance(response.get("error"), dict):
            error = dict(response["error"])
            data = error.get("data")
            error["data"] = {
                "original": data,
                "executed_tool": target_name,
                "recovery": (
                    "读取错误中的字段要求，必要时调用 ha_get_tool_help；"
                    "若选错工具则带着此错误重新调用 ha_discover。"
                ),
            }
            response["error"] = error
        else:
            result = response.get("result")
            if isinstance(result, dict) and result.get("isError"):
                content = result.get("content")
                if not isinstance(content, list):
                    content = []
                    result["content"] = content
                content.append(
                    {
                        "type": "text",
                        "text": json.dumps(
                            {
                                "executed_tool": target_name,
                                "recovery": (
                                    "不要原样重复失败调用。先根据错误修正参数；不确定参数时调用 "
                                    "ha_get_tool_help，选错工具时调用 ha_discover 并把错误放入 context。"
                                ),
                            },
                            ensure_ascii=False,
                            separators=(",", ":"),
                        ),
                    }
                )

        encoded = json.dumps(
            response, ensure_ascii=False, separators=(",", ":")
        ).encode("utf-8")
        if len(encoded) <= self.max_bytes:
            return response

        # 只在单次执行结果超过小智 WebSocket 上限时裁剪，避免再次触发 1009。
        original_result = response.get("result", response.get("error", {}))
        original_text = json.dumps(
            original_result, ensure_ascii=False, separators=(",", ":")
        )
        preview_chars = min(len(original_text), self.max_bytes // 2)
        while preview_chars > 256:
            bounded = self.call_result(
                request_id,
                {
                    "ok": True,
                    "executed_tool": target_name,
                    "truncated": True,
                    "preview": original_text[:preview_chars],
                    "recovery": (
                        "结果超过传输上限。继续任务时请使用更具体的查询、过滤器、字段投影或分页参数；"
                        "不要把同一个宽泛调用原样重试。"
                    ),
                },
            )
            if len(json.dumps(bounded, ensure_ascii=False).encode("utf-8")) <= self.max_bytes:
                logger.warning(
                    "HA tool result from %s exceeded %d bytes; sending a bounded preview",
                    target_name,
                    self.max_bytes,
                )
                return bounded
            preview_chars = int(preview_chars * 0.75)

        return self.call_result(
            request_id,
            {
                "ok": False,
                "executed_tool": target_name,
                "error_code": "RESULT_TOO_LARGE",
                "error": "工具结果超过小智传输上限。",
                "recovery": "改用更具体的查询、过滤器、字段投影或分页参数后继续。",
            },
            True,
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

                if tool_name == ROUTER_DISCOVER_TOOL:
                    response = tool_pager.discover_response(request.get("id"), arguments)
                    async with send_lock:
                        await websocket.send(
                            json.dumps(response, ensure_ascii=False, separators=(",", ":"))
                        )
                    continue

                if tool_name == ROUTER_HELP_TOOL:
                    response = tool_pager.help_response(request.get("id"), arguments)
                    async with send_lock:
                        await websocket.send(
                            json.dumps(response, ensure_ascii=False, separators=(",", ":"))
                        )
                    continue

                if tool_name == LEGACY_ROUTER_SEARCH_TOOL:
                    response = tool_pager.search_response(request.get("id"), arguments)
                    async with send_lock:
                        await websocket.send(
                            json.dumps(response, ensure_ascii=False, separators=(",", ":"))
                        )
                    continue

                if tool_name in {ROUTER_EXECUTE_TOOL, LEGACY_ROUTER_CALL_TOOL}:
                    route_args = arguments if isinstance(arguments, dict) else {}
                    target_name = str(route_args.get("tool_name", "")).strip()
                    target_arguments = route_args.get("arguments")
                    if target_name not in tool_pager.tools_by_name:
                        response = tool_pager.call_result(
                            request.get("id"),
                            {
                                "ok": False,
                                "error_code": "UNKNOWN_TOOL",
                                "error": "未知的 HA MCP 工具名称。",
                                "tool_name": target_name,
                                "recovery": "调用 ha_discover 重新查找，不要猜测工具名。",
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
                            {
                                "ok": False,
                                "error_code": "INVALID_ARGUMENTS",
                                "error": "arguments 必须是 JSON 对象。",
                                "recovery": (
                                    "调用 ha_get_tool_help 查看该工具 inputSchema 后重新组织参数。"
                                ),
                            },
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
                    tool_pager.pending_calls[request.get("id")] = target_name
                    logger.info(
                        "Agent loop executing HA tool: %s%s",
                        target_name,
                        f" ({route_args.get('purpose')})" if route_args.get("purpose") else "",
                    )

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
            else:
                response = tool_pager.enrich_routed_response(response)

            if isinstance(response, dict):
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
