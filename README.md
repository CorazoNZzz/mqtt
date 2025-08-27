# MQTT消息转发器

这是一个Python程序，用于监听指定MQTT broker上的设备消息，并将消息重新格式化后转发到指定topic。

## 功能特性

- 监听多个AMT设备的状态消息（topic格式：`status/AMTxxxxxxxxxxxx`）
- 将收到的消息重新格式化并转发
- 支持JSON和普通文本payload
- 自动将JSON键值对转换为name-value格式
- 自动过滤空消息和空JSON对象
- 添加固定Type字段和动态时间戳
- 完整的日志记录功能
- 优雅的启动和停止

## 安装依赖

```bash
pip install -r requirements.txt
```

## 配置文件

编辑 `config.json` 文件来配置您的MQTT设置：

```json
{
  "mqtt": {
    "broker": "mqtt.azuratech.com",    // 监听的MQTT broker地址
    "port": 1883,                      // MQTT broker端口
    "username": "admin",               // 用户名
    "password": "test",                // 密码
    "keepalive": 60                    // 保活时间（秒）
  },
  "devices": [                         // 要监听的设备ID列表（14位数字）
    "12345678901234",
    "56789012345678",
    "98765432109876"
  ],
  "forward": {                         // 转发目标设置
    "broker": "mqtt.azuratech.com",    // 转发到的broker地址
    "port": 1883,                      // 转发broker端口
    "topic": "YCWLWZGW2024111801"      // 转发的目标topic
  }
}
```

### 重要配置说明：

1. **devices数组**：包含要监听的AMT设备的14位数字编码
2. **监听topic格式**：`status/AMT{14位数字编码}`
3. **转发消息格式**：
   ```json
   {
     "data": [原始payload],           // 原始消息内容（数组格式）
     "SN": "AMTxxxxxxxxxxxx",         // 设备序列号
     "Type": "park",                  // 固定类型标识
     "flexem_timestamp": 1753101971000 // 当前时间戳（毫秒）
   }
   ```

### 消息处理规则：

- **空消息过滤**：空字符串或空JSON对象不会被转发
- **JSON格式转换**：将 `"AI1": 0.07997` 转换为 `{"name":"AI1","value":0.07997}`
- **时间戳生成**：每次转发时自动生成当前毫秒级时间戳
- **类型标识**：所有转发消息的Type字段固定为"park"

## 运行程序

```bash
python mqtt_forwarder.py
```

程序启动后会：
1. 连接到监听broker（mqtt.azuratech.com）
2. 连接到转发broker（同一个broker）
3. 订阅所有配置的设备topic
4. 开始监听和转发消息

## 停止程序

按 `Ctrl+C` 优雅停止程序。

## 日志

程序会将日志同时输出到：
- 控制台
- 文件：`mqtt_forwarder.log`

## 示例

如果配置了设备ID `12345678901234`，程序会：

1. **监听topic**：`status/AMT12345678901234`
2. **收到消息**：`{"AI1": 0.07997, "AI2": 0.08123}`
3. **转发格式**：
   ```json
   {
     "data": [
       {"name": "AI1", "value": 0.07997},
       {"name": "AI2", "value": 0.08123}
     ],
     "SN": "AMT12345678901234",
     "Type": "park",
     "flexem_timestamp": 1753101971000
   }
   ```
4. **发送到**：`YCWLWZGW2024111801` topic

### JSON转换示例：

**原始消息**：
```json
{"AI1": 0.07997, "AI2": 0.08123, "DI1": 1}
```

**转换后**：
```json
[
  {"name": "AI1", "value": 0.07997},
  {"name": "AI2", "value": 0.08123},
  {"name": "DI1", "value": 1}
]
```

**非JSON消息**（如纯文本）会保持原样不转换。

## 故障排除

1. **连接失败**：检查broker地址、端口、用户名密码
2. **没有收到消息**：确认设备ID正确，topic存在
3. **转发失败**：检查broker连接状态和目标topic权限

## 注意事项

- 设备ID必须是14位数字
- 确保broker可以正常连接
- 确保有目标topic的发布权限
- 程序会自动处理连接断开和重连 