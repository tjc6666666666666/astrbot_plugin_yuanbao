# 腾讯元宝 (Yuanbao) 适配器

AstrBot 平台适配器，基于 WebSocket 协议接入腾讯元宝 IM 机器人平台。

支持收发文本、图片、文件、语音、视频等消息，以及群聊 @提及 和 回复/引用消息。

## 安装

### 方式一：AstrBot 插件市场

1. 打开 AstrBot WebUI → **插件市场**
2. 搜索 `yuanbao` 或 `腾讯元宝`
3. 点击 **安装**

### 方式二：手动安装

```bash
# 进入 AstrBot 的 plugins 目录
cd data/plugins

# 克隆仓库
git clone https://github.com/tjc6666666666666/astrbot_plugin_yuanbao.git yuanbao

# 安装依赖
pip install -r yuanbao/requirements.txt

# 重启 AstrBot
```

## 配置

### 获取凭证

1. 登录 [腾讯元宝开放平台](https://bot.yuanbao.tencent.com)
2. 创建机器人应用，获得 **AppKey** 和 **AppSecret**

### 配置项

在 AstrBot WebUI → **配置** → **平台适配器** 中添加或编辑 `yuanbao` 适配器：

| 配置项 | 必填 | 默认值 | 说明 |
|--------|------|--------|------|
| `token` | 推荐 | `""` | `appKey:appSecret` 冒号分隔格式 |
| `app_key` | 备选 | `""` | 单独填写 app_key |
| `app_secret` | 备选 | `""` | 单独填写 app_secret |
| `ws_url` | 否 | `wss://bot-wss.yuanbao.tencent.com/wss/connection` | WebSocket 网关地址 |
| `api_domain` | 否 | `bot.yuanbao.tencent.com` | API 域名 |
| `require_mention` | 否 | `true` | 群聊是否必须 @机器人 才响应 |
| `enable` | 是 | `false` | **必须设为 `true`** 才能启用 |

**token 格式示例：**

```yaml
token: "aBcDeFgHiJkLmNoPqRsTuV:Z1y2X3w4V5u6T7s8R9q0"
```

### 网络访问说明

适配器需要访问以下地址：

| 地址 | 用途 |
|------|------|
| `wss://bot-wss.yuanbao.tencent.com` | WebSocket 长连接 |
| `https://bot.yuanbao.tencent.com` | REST API（签名、上传、下载） |
| `*.cos.*.myqcloud.com` | 腾讯云 COS 上传（媒体文件） |

请确保运行环境（服务器/容器/本地）可以访问以上域名。

## 使用

### 消息类型支持

| 类型 | 收发 | 说明 |
|------|------|------|
| 文本 | ✅ 收/发 | 纯文本消息 |
| 图片 | ✅ 收/发 | 自动下载到本地缓存后传给 AI |
| 文件 | ✅ 收/发 | 自动下载到本地缓存后传给 AI |
| 语音 | ✅ 收/发 | 自动下载 |
| 视频 | ✅ 收/发 | 自动下载 |
| @提及 | ✅ 收 | 群聊 @机器人 检测 |
| 回复/引用 | ✅ 收 | 解析引用消息，附带上下文给 LLM |

### 命令

适配器自身无命令，所有消息处理由 AstrBot 的 LLM 或其他插件完成。

## 常见错误排查

### 1. WebSocket 连接失败

**现象：** 日志中反复出现 `WebSocket 错误` 或 `连接超时`

**检查：**
- 网络是否能访问 `wss://bot-wss.yuanbao.tencent.com`（部分服务器需配置防火墙/代理）
- `app_key` / `app_secret` 是否正确
- 机器人是否在元宝开放平台已上线

### 2. 令牌签名失败

**现象：** 日志 `令牌签名失败`

**原因：** AppKey/AppSecret 错误或配置格式不对

**解决：**
- 确认 token 格式为 `appKey:appSecret`（冒号分隔，中间无空格）
- 或分别填写 `app_key` 和 `app_secret`
- 检查 API 域名是否配置正确

### 3. 认证失败 (code=41103/41104/41108)

**现象：** WebSocket 连接后立即断开，日志包含 `认证失败`

**原因：** Token 过期或被服务端拒绝

**解决：** 适配器会自动调用 `on_auth_failed` 回调刷新 token。如果持续失败，检查 AppKey/AppSecret 是否有效。

### 4. 媒体下载失败

**现象：** 群聊消息能收到文本，但图片/文件显示"无法获取"

**检查：**
- `token` 是否有效（媒体下载依赖签名 token）
- 服务器是否能访问元宝 API 域名
- 查看 AstrBot 日志中的详细错误信息

### 5. 消息发送失败

**现象：** 机器人回复后用户看不见消息

**检查：**
- WebSocket 连接状态是否为 `CONNECTED`
- 检查日志中 `send_raw` 是否返回 OK
- 确认机器人有向该用户/群发消息的权限

## 依赖

- Python >= 3.10
- [aiohttp](https://pypi.org/project/aiohttp/) — 异步 HTTP 请求
- [websockets](https://pypi.org/project/websockets/) — WebSocket 客户端
- [astrbot](https://github.com/Soulter/AstrBot) — AstrBot 框架（运行环境自带）

## 协议

本项目基于 MIT 协议开源。
