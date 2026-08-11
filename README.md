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
