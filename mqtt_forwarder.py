#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MQTT消息监听和转发程序
监听指定的AMT设备topic，将收到的消息转发到目标broker
"""

import json
import logging
import time
import sys
from typing import List, Dict, Any
import paho.mqtt.client as mqtt

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('mqtt_forwarder.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


class MQTTForwarder:
    def __init__(self, config_file: str = "config.json"):
        """初始化MQTT转发器"""
        self.config = self.load_config(config_file)
        self.client = None
        self.is_running = False
        self.is_connected = False
        
    def load_config(self, config_file: str) -> Dict[str, Any]:
        """加载配置文件"""
        try:
            with open(config_file, 'r', encoding='utf-8') as f:
                config = json.load(f)
            logger.info(f"配置文件 {config_file} 加载成功")
            return config
        except FileNotFoundError:
            logger.error(f"配置文件 {config_file} 不存在")
            sys.exit(1)
        except json.JSONDecodeError as e:
            logger.error(f"配置文件格式错误: {e}")
            sys.exit(1)
            
    def on_connect(self, client, userdata, flags, rc):
        """MQTT客户端连接回调"""
        if rc == 0:
            logger.info("成功连接到MQTT broker")
            self.is_connected = True
            # 订阅所有配置的设备topic
            for device_id in self.config['devices']:
                topic = f"status/AMT{device_id}"
                client.subscribe(topic)
                logger.info(f"订阅topic: {topic}")
        else:
            logger.error(f"连接MQTT broker失败，错误码: {rc}")
            self.is_connected = False
            
    def on_disconnect(self, client, userdata, rc):
        """MQTT客户端断开连接回调"""
        logger.warning(f"与MQTT broker断开连接，错误码: {rc}")
        self.is_connected = False
        
    def on_message(self, client, userdata, msg):
        """消息接收回调"""
        try:
            topic = msg.topic
            payload = msg.payload.decode('utf-8').strip()
            
            # 检查payload是否为空
            if not payload:
                logger.info(f"收到空消息，跳过转发")
                return
                
            # 解析topic获取设备ID
            if topic.startswith("status/AMT"):
                device_id = topic.replace("status/AMT", "")
                logger.info(f"收到设备 {device_id} 的消息: {payload}")
                
                # 检查如果是JSON且为空对象，也跳过转发
                if self.is_json(payload):
                    parsed_data = json.loads(payload)
                    if not parsed_data or (isinstance(parsed_data, dict) and len(parsed_data) == 0):
                        logger.info(f"收到空JSON消息，跳过转发")
                        return
                    # 转换JSON格式：将 "AI1": 0.07997 转换为 {"name":"AI1","value":0.07997}
                    data_content = self.convert_json_format(parsed_data)
                else:
                    data_content = payload
                
                # 构造转发消息格式
                # 如果data_content已经是列表（转换后的JSON），直接使用；否则包装成数组
                data_array = data_content if isinstance(data_content, list) else [data_content]
                
                forward_data = {
                    "data": data_array,
                    "SN": f"AMT{device_id}",
                    "Type": "park",
                    "flexem_timestamp": int(time.time() * 1000)
                }
                
                # 转发消息
                self.forward_message(forward_data)
                
        except Exception as e:
            logger.error(f"处理消息时出错: {e}")
            
    def is_json(self, text: str) -> bool:
        """检查字符串是否为有效的JSON"""
        try:
            json.loads(text)
            return True
        except (json.JSONDecodeError, TypeError):
            return False
            
    def convert_json_format(self, data):
        """转换JSON格式：将 "AI1": 0.07997 转换为 {"name":"AI1","value":0.07997}"""
        if isinstance(data, dict):
            # 将字典的每个键值对转换为包含name和value的对象
            converted_list = []
            for key, value in data.items():
                converted_list.append({
                    "name": key,
                    "value": value
                })
            return converted_list
        else:
            # 如果不是字典，直接返回原数据
            return data
            
    def forward_message(self, data: Dict[str, Any]):
        """转发消息到目标topic"""
        try:
            if self.client and self.is_connected:
                message_json = json.dumps(data, ensure_ascii=False)
                topic = self.config['forward']['topic']
                
                result = self.client.publish(topic, message_json)
                if result.rc == mqtt.MQTT_ERR_SUCCESS:
                    logger.info(f"消息转发成功: {data['SN']} -> {topic}")
                else:
                    logger.error(f"消息转发失败: {result.rc}")
            else:
                logger.error("MQTT客户端未连接")
        except Exception as e:
            logger.error(f"转发消息时出错: {e}")
            
    def setup_client(self):
        """设置MQTT客户端"""
        # 检查监听和转发是否使用同一个broker
        listen_broker = f"{self.config['mqtt']['broker']}:{self.config['mqtt']['port']}"
        forward_broker = f"{self.config['forward']['broker']}:{self.config['forward']['port']}"
        
        if listen_broker == forward_broker:
            logger.info("监听和转发使用同一个broker，使用单一客户端连接")
        else:
            logger.info("监听和转发使用不同broker，使用单一客户端连接到监听broker")
            
        # 设置单一MQTT客户端
        self.client = mqtt.Client(client_id="mqtt_forwarder")
        self.client.username_pw_set(
            self.config['mqtt']['username'],
            self.config['mqtt']['password']
        )
        self.client.on_connect = self.on_connect
        self.client.on_disconnect = self.on_disconnect
        self.client.on_message = self.on_message
        
    def start(self):
        """启动转发器"""
        try:
            logger.info("启动MQTT转发器...")
            self.setup_client()
            
            # 连接MQTT broker
            broker_host = self.config['mqtt']['broker']
            broker_port = self.config['mqtt']['port']
            logger.info(f"连接到MQTT broker: {broker_host}:{broker_port}")
            
            self.client.connect(
                broker_host,
                broker_port,
                self.config['mqtt']['keepalive']
            )
            
            # 启动客户端循环
            self.client.loop_start()
            
            self.is_running = True
            logger.info("MQTT转发器启动完成")
            
            # 保持程序运行
            try:
                while self.is_running:
                    time.sleep(1)
            except KeyboardInterrupt:
                logger.info("收到停止信号，正在关闭...")
                self.stop()
                
        except Exception as e:
            logger.error(f"启动转发器时出错: {e}")
            self.stop()
            
    def stop(self):
        """停止转发器"""
        logger.info("正在停止MQTT转发器...")
        self.is_running = False
        
        if self.client:
            self.client.loop_stop()
            self.client.disconnect()
            
        logger.info("MQTT转发器已停止")


def main():
    """主函数"""
    logger.info("启动MQTT消息转发程序")
    
    # 检查配置文件中的设备ID格式
    try:
        with open("config.json", 'r', encoding='utf-8') as f:
            config = json.load(f)
            
        for device_id in config['devices']:
            if len(device_id) != 14 or not device_id.isdigit():
                logger.warning(f"设备ID {device_id} 不是14位数字编码")
                
    except Exception as e:
        logger.error(f"验证配置时出错: {e}")
        return
    
    # 启动转发器
    forwarder = MQTTForwarder()
    forwarder.start()


if __name__ == "__main__":
    main() 