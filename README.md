# mcp_ha_xiaozhi
小智官方服务器(虾哥)对接home assistant的mcp server
### 原理
使用小智官方给的示例代码，结合mcp_proxy,实现小智官方服务器和home assistant的mcp server打通


### 参数
XIAOZHI_MCP_ENDPOINT：你的小智 MCP 接入点
HA_MCP_ENDPOINT：你的 HA MCP SERVER 地址
API_ACCESS_TOKEN：你的长效 API 令牌
MCP_TRANSPORT：`auto`、`sse` 或 `streamablehttp`，默认 `auto`

1.  **小智 MCP 接入点：** 登录小智官方服务器即可获取。
2.  **HA MCP SERVER 地址：** 可以使用旧版官方 SSE 地址，也可以使用新版 HA-MCP 的 Streamable HTTP 地址。
    * 点击此链接：[Home Assistant MCP Server 集成](https://my.home-assistant.io/redirect/config_flow_start?domain=mcp_server)直达安装
    * 或 在 Home Assistant 中，前往 **设置 > 设备和服务 > 添加集成**。
    * 从列表中选择“**模型上下文协议服务器**”，并按照屏幕上的说明完成设置。
    * 使用新版 HA-MCP 时，填写设置页面生成的 `/private_xxx` 连接地址，并确保地址末尾没有空格。
3.  **长效 API 令牌：** 用于授权访问你的 Home Assistant 实例。
    * 访问你的 [Home Assistant 账户配置文件设置](https://my.home-assistant.io/redirect/profile)，进入“**安全**”选项卡。
    * 创建**长期访问令牌**。

### 1.1.0 代理式工具路由

本版本不再把全部 HA 工具压缩后直接发布给小智，也不需要额外的大模型。
桥接器缓存 HA-MCP 的完整工具目录，只向小智后台的模型提供三个循环工具：

- `ha_discover`：根据用户目标发现相关工具和推荐执行流程。
- `ha_get_tool_help`：按需读取一个真实 HA 工具的完整参数结构。
- `ha_execute`：执行真实 HA 工具，并把结果交回小智模型继续判断。

复杂任务采用“发现 → 查看参数 → 执行 → 根据结果继续”的循环。例如搜索歌曲时，
桥接器会明确引导模型依次查询 `music_assistant` 服务、获取集成配置条目，最后调用
`music_assistant.search`，避免把歌曲搜索误判为普通 HA 实体搜索。

启动成功后，日志应出现：

```text
Agentic tool router ready: 78 HA tools cached, exposing 3 loop tools to XiaoZhi
```

小智执行任务时，还会依次出现 `Capability discovery`、`Serving full help` 和
`Agent loop executing HA tool`。这表示后台模型正在持续使用同一组工具结果完成多步任务。


### docker运行
```bash
docker run -d --name mcp_ha_xiaozhi \
-e XIAOZHI_MCP_ENDPOINT="你的小智MCP接入点" \
-e HA_MCP_ENDPOINT="你的HA MCP SERVER地址" \
-e API_ACCESS_TOKEN="你的长时效API令牌" \
-e MCP_TRANSPORT=auto \
shawn68/mcp_ha_xiaozhi
```

https://hub.docker.com/r/shawn68/mcp_ha_xiaozhi/

### 新版 HA-MCP 配置示例

新版 HA-MCP 的连接地址通常是设置页面生成的私密地址，使用 Streamable HTTP：

```bash
docker run -d --name mcp_ha_xiaozhi \
  -e XIAOZHI_MCP_ENDPOINT="你的小智 MCP 接入点" \
  -e HA_MCP_ENDPOINT="http://192.168.1.8:9584/private_xxx" \
  -e MCP_TRANSPORT=streamablehttp \
  -e API_ACCESS_TOKEN="你的长期访问令牌" \
  mcp_ha_xiaozhi:local
```

`HA_MCP_ENDPOINT` 末尾不要有空格。使用 `/private_xxx` 私密地址时，完整地址本身就是凭据，请不要公开。

### 作为 Home Assistant 自定义应用安装

1. 在 Home Assistant 的应用商店中添加本仓库地址：
   `https://github.com/soonnn1/mcp_ha_xiaozhi-custom`
2. 找到“小智 MCP Server（新版）”并安装。
3. 在配置中填写小智 MCP 地址和 HA-MCP 私密地址。
4. `MCP_TRANSPORT` 选择 `streamablehttp`，然后启动应用。
