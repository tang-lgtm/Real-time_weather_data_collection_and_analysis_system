# GitHub 项目文档：天气数据采集与分析系统

## 📌 项目概述
这是一个完整的天气数据采集与分析系统，包含两个核心组件：
1. **数据采集端** (`tianqi.py`)：从中国天气网抓取多个城市的实时天气数据
2. **数据分析端** (`xiaofei.py`)：通过 Kafka 消费数据并生成可视化分析图表

系统采用生产者-消费者模式，通过 Kafka 消息队列实现数据的高效传输和处理。

## 🛠️ 技术栈
- **数据采集**: Requests + BeautifulSoup
- **消息队列**: Apache Kafka
- **数据处理**: Pandas
- **数据可视化**: Matplotlib
- **日志管理**: logging

## 📂 文件结构
```
/weather-system
│── producer/
│   ├── tianqi.py          # 天气数据采集程序
│── consumer/
│   ├── xiaofei.py         # 天气数据分析程序
│── docs/
│   ├── architecture.md    # 系统架构说明
│── README.md              # 项目说明文档

```

## 🔧 安装与运行

### 1. 前置要求
- 安装并运行 Kafka 服务 (localhost:9092)
- 安装并配置 Flume (如需)

### 2. 环境配置
pychram

### 3. 运行系统
```bash
# 启动生产者 (数据采集)
python producer/tianqi.py

# 启动消费者 (数据分析)
python consumer/xiaofei.py
```

## 🧠 系统架构

### 数据流图
```mermaid
graph LR
    A[中国天气网] -->|爬取| B(tianqi.py)
    B -->|写入| C[Kafka]
    C -->|消费| D(xiaofei.py)
    D --> E[生成可视化图表]
```

### 核心功能
1. **数据采集端**:
   - 定时抓取北京、上海等5个城市的天气数据
   - 数据包含城市、日期、天气状况、温度等信息
   - 支持异常处理和日志记录

2. **数据分析端**:
   - 实时消费 Kafka 中的天气数据
   - 动态生成温度变化趋势图
   - 支持数据质量检查和清洗

## ⚙️ 配置说明

### 1. Kafka 配置
在 `xiaofei.py` 中修改以下参数：
```python
bootstrap_servers=['localhost:9092']  # Kafka 服务器地址
topic = 'weather_topic'              # Kafka 主题
```

### 2. 采集城市配置
在 `tianqi.py` 中修改 `city_codes` 字典增减城市：
```python
city_codes = {
    "北京": "101010100",
    "上海": "101020100",
    # 可添加更多城市...
}
```

### 3. 采集频率设置
```python
total_scrapes = 21      # 总采集次数
interval_hours = 1      # 采集间隔(小时)
```

## 📊 数据格式

### 采集数据示例
```json
{
  "city": "北京",
  "date": "2023-07-20",
  "weather": "晴",
  "temperature": "32/26",
  "timestamp": "2023-07-20T14:30:45.123456"
}
```

### 数据分析输出
- 生成 `temperature_changes_*.png` 温度变化趋势图
- 图表包含所有城市温度变化曲线和数据点标注

## 🚀 扩展建议
1. 添加更多数据源和城市
2. 实现异常天气预警功能
3. 集成到数据看板
4. 添加数据库存储
5. 实现分布式采集

## 📜 开源协议
Apache License 2.0

---

> **提示**：运行前请确保 Kafka 服务已启动。系统会自动创建所需的数据目录和图表文件。
