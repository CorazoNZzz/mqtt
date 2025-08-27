@echo off
echo 启动MQTT消息转发器...
echo.

REM 检查Python是否安装
python --version >nul 2>&1
if errorlevel 1 (
    echo 错误: 未找到Python，请先安装Python
    pause
    exit /b 1
)

REM 检查配置文件是否存在
if not exist config.json (
    echo 错误: 配置文件 config.json 不存在
    pause
    exit /b 1
)

REM 检查依赖是否安装
echo 检查依赖包...
pip show paho-mqtt >nul 2>&1
if errorlevel 1 (
    echo 安装依赖包...
    pip install -r requirements.txt
    if errorlevel 1 (
        echo 错误: 依赖安装失败
        pause
        exit /b 1
    )
)

echo 依赖检查完成，启动程序...
echo.
echo 按 Ctrl+C 停止程序
echo.

REM 启动程序
python mqtt_forwarder.py

echo.
echo 程序已停止
pause 