import json
import sys
import types
import unittest

# 路由测试不建立网络连接，本机未安装运行时依赖时使用空模块隔离导入。
sys.modules.setdefault("websockets", types.ModuleType("websockets"))

from mcp_pipe import ROUTER_TOOLS, ToolListPager


def make_tool(name, description, properties=None, required=None):
    return {
        "name": name,
        "description": description,
        "inputSchema": {
            "type": "object",
            "properties": properties or {},
            "required": required or [],
            "additionalProperties": False,
        },
    }


def result_payload(response):
    text = response["result"]["content"][0]["text"]
    return json.loads(text)


class ToolRouterTests(unittest.TestCase):
    def setUp(self):
        self.router = ToolListPager()
        self.router.cache_tools(
            [
                make_tool(
                    "ha_list_services",
                    "List Home Assistant services and their field schemas.",
                    {"domain": {"type": "string"}},
                ),
                make_tool(
                    "ha_get_integration",
                    "Get integration config entries.",
                    {"domain": {"type": "string"}},
                ),
                make_tool(
                    "ha_call_service",
                    "Call a Home Assistant service.",
                    {
                        "domain": {"type": "string"},
                        "service": {"type": "string"},
                        "data": {"type": "object"},
                    },
                    ["domain", "service"],
                ),
                make_tool(
                    "ha_search",
                    "Search Home Assistant entities.",
                    {"query": {"type": "string"}},
                    ["query"],
                ),
                make_tool(
                    "ha_get_state",
                    "Get entity state.",
                    {"entity_id": {"type": "string"}},
                    ["entity_id"],
                ),
            ]
        )

    def test_exposes_three_loop_tools(self):
        self.assertEqual(
            [tool["name"] for tool in ROUTER_TOOLS],
            ["ha_discover", "ha_get_tool_help", "ha_execute"],
        )
        self.assertEqual(len(self.router.pages[0]), 3)

    def test_music_discovery_returns_service_workflow(self):
        payload = result_payload(
            self.router.discover_response(
                1,
                {"query": "搜索并播放周杰伦的歌曲", "limit": 5},
            )
        )
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["recipes"][0]["name"], "music_assistant_media")
        self.assertEqual(payload["candidates"][0]["name"], "ha_list_services")
        self.assertIn("ha_call_service", payload["recipes"][0]["recommended_tools"])
        self.assertNotEqual(payload["candidates"][0]["name"], "ha_search")

    def test_help_returns_full_input_schema(self):
        payload = result_payload(
            self.router.help_response(2, {"tool_name": "ha_call_service"})
        )
        self.assertEqual(payload["tool"]["name"], "ha_call_service")
        self.assertIn("data", payload["tool"]["inputSchema"]["properties"])
        self.assertEqual(payload["required"], ["domain", "service"])

    def test_failed_real_call_gets_recovery_instruction(self):
        self.router.pending_calls[7] = "ha_call_service"
        response = {
            "jsonrpc": "2.0",
            "id": 7,
            "result": {
                "content": [{"type": "text", "text": "missing domain"}],
                "isError": True,
            },
        }
        enriched = self.router.enrich_routed_response(response)
        self.assertEqual(len(enriched["result"]["content"]), 2)
        self.assertIn("ha_get_tool_help", enriched["result"]["content"][1]["text"])

    def test_large_real_call_result_is_bounded(self):
        self.router.pending_calls[8] = "ha_get_overview"
        response = {
            "jsonrpc": "2.0",
            "id": 8,
            "result": {
                "content": [{"type": "text", "text": "数据" * 30000}],
                "isError": False,
            },
        }
        enriched = self.router.enrich_routed_response(response)
        encoded = json.dumps(enriched, ensure_ascii=False).encode("utf-8")
        self.assertLessEqual(len(encoded), self.router.max_bytes)
        payload = result_payload(enriched)
        self.assertTrue(payload["truncated"])
        self.assertIn("字段投影", payload["recovery"])


if __name__ == "__main__":
    unittest.main()
