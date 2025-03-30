import json
import logging
from kafka import KafkaConsumer
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# 创建 Kafka 消费者
consumer = KafkaConsumer(
    'weather_topic',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='latest',
    enable_auto_commit=True,
    group_id='weather_analysis_group',
    value_deserializer=lambda x: x
)


def generate_chart(df, chart_number):
    plt.style.use('ggplot')
    plt.rcParams['font.sans-serif'] = ['SimHei']  # 设置中文字体
    plt.rcParams['axes.unicode_minus'] = False  # 正确显示负号
    plt.figure(figsize=(14, 8))
    for city in df['city'].unique():
        city_data = df[df['city'] == city]
        plt.plot(city_data['timestamp'], city_data['temperature'], label=city, marker='o', linewidth=2, markersize=6)

    plt.title(f'各城市温度变化 (图表 {chart_number})', fontsize=20, fontweight='bold')
    plt.xlabel('时间', fontsize=14)
    plt.ylabel('温度 (°C)', fontsize=14)
    plt.legend(fontsize=12, loc='best')
    plt.xticks(rotation=45, ha='right', fontsize=10)
    plt.yticks(fontsize=10)
    plt.grid(True, linestyle='--', alpha=0.7)

    for city in df['city'].unique():
        city_data = df[df['city'] == city]
        for i, txt in enumerate(city_data['temperature']):
            plt.annotate(f"{txt:.1f}", (city_data['timestamp'].iloc[i], txt),
                         xytext=(0, 5), textcoords='offset points', ha='center', va='bottom',
                         fontsize=8, alpha=0.7)

    plt.tight_layout()
    plt.savefig(f'temperature_changes_{chart_number}.png', dpi=300, bbox_inches='tight')
    plt.close()
    logger.info(f"图表 {chart_number} 已生成并保存。")


weather_data = []
chart_count = 0
max_messages = 60

try:
    while len(weather_data) < max_messages:
        message = next(consumer, None)
        if message is not None:
            try:
                msg_str = message.value.decode('utf-8')
                data = json.loads(msg_str)
                weather_data.append(data)

                if len(weather_data) % 5 == 0:
                    logger.info(f"已收集 {len(weather_data)} 条数据")

                    # 创建 DataFrame 并处理所有累积的数据
                    df = pd.DataFrame(weather_data)
                    df['timestamp'] = pd.to_datetime(df['timestamp'])
                    df['temperature'] = df['temperature'].apply(
                        lambda x: float(x.split('/')[0]) if x and x.split('/')[0] else None)
                    df = df.dropna(subset=['temperature'])

                    if not df.empty:
                        chart_count += 1
                        generate_chart(df, chart_count)
                    else:
                        logger.warning("没有有效的数据可以生成图表")
            except Exception as e:
                logger.error(f"处理消息时出错: {str(e)}")
                continue
        else:
            logger.warning("没有收到新消息，等待中...")
            continue

    logger.info(f"成功收集到 {len(weather_data)} 条数据，共生成了 {chart_count} 张图表")

except Exception as e:
    logger.error(f"处理过程中发生错误: {str(e)}", exc_info=True)

finally:
    consumer.close()
    logger.info("Kafka消费者已关闭")
'''import json
import logging
from kafka import KafkaConsumer
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# 创建 Kafka 消费者
consumer = KafkaConsumer(
    'weather_topic',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='latest',
    enable_auto_commit=True,
    group_id='weather_analysis_group',
    value_deserializer=lambda x: x  # 不在这里解析，留到后面处理
)

# 收集数据
weather_data = []
max_messages = 120  # 最大消息数

try:
    # 逐条处理消息
    while len(weather_data) < max_messages:
        message = next(consumer, None)
        if message is not None:
            try:
                msg_str = message.value.decode('utf-8')
                data = json.loads(msg_str)
                weather_data.append(data)
                if len(weather_data) % 7 == 0:  # 每收集7条数据记录一次日志
                    logger.info(f"已收集 {len(weather_data)} 条数据")
            except Exception as e:
                logger.error(f"处理消息时出错: {str(e)}")
                continue
        else:
            logger.warning("没有收到新消息，等待中...")
            continue  # 继续等待新消息

    logger.info(f"成功收集到 {len(weather_data)} 条数据")

    # 将数据转换为 DataFrame
    df = pd.DataFrame(weather_data)

    # 处理时间戳
    df['timestamp'] = pd.to_datetime(df['timestamp'])

    # 处理温度数据
    df['temperature'] = df['temperature'].apply(lambda x: float(x.split('/')[0]) if x and x.split('/')[0] else None)
    df = df.dropna(subset=['temperature'])  # 删除无效温度数据

    if df.empty:
        raise ValueError("没有有效的数据可以分析")

    # 设置matplotlib样式
    plt.style.use('ggplot')
    plt.rcParams['font.sans-serif'] = ['SimHei']  # 设置中文字体
    plt.rcParams['axes.unicode_minus'] = False  # 正确显示负号

    # 生成图表
    plt.figure(figsize=(14, 8))
    for city in df['city'].unique():
        city_data = df[df['city'] == city]
        plt.plot(city_data['timestamp'], city_data['temperature'], label=city, marker='o', linewidth=2, markersize=6)

    plt.title('各城市温度变化', fontsize=20, fontweight='bold')
    plt.xlabel('时间', fontsize=14)
    plt.ylabel('温度 (°C)', fontsize=14)
    plt.legend(fontsize=12, loc='best')
    plt.xticks(rotation=45, ha='right', fontsize=10)
    plt.yticks(fontsize=10)
    plt.grid(True, linestyle='--', alpha=0.7)

    # 添加数据点标签
    for city in df['city'].unique():
        city_data = df[df['city'] == city]
        for i, txt in enumerate(city_data['temperature']):
            plt.annotate(f"{txt:.1f}", (city_data['timestamp'].iloc[i], txt),
                         xytext=(0, 5), textcoords='offset points', ha='center', va='bottom',
                         fontsize=8, alpha=0.7)

    plt.tight_layout()
    plt.savefig('temperature_changes.png', dpi=300, bbox_inches='tight')
    plt.close()

    logger.info("分析完成，图表已保存。")

except Exception as e:
    logger.error(f"处理过程中发生错误: {str(e)}", exc_info=True)

finally:
    consumer.close()
    logger.info("Kafka消费者已关闭")'''
'''import json
import logging
from kafka import KafkaConsumer
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# 创建 Kafka 消费者
consumer = KafkaConsumer(
    'weather_topic',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='latest',
    enable_auto_commit=True,
    group_id='weather_analysis_group',
    value_deserializer=lambda x: x  # 不在这里解析，留到后面处理
)

# 收集数据
weather_data = []
max_messages = 70  # 最大消息数

try:
    # 逐条处理消息
    while len(weather_data) < max_messages:
        message = next(consumer, None)
        if message is not None:
            try:
                msg_str = message.value.decode('utf-8')
                data = json.loads(msg_str)
                weather_data.append(data)
                if len(weather_data) % 7 == 0:  # 每收集10条数据记录一次日志
                    logger.info(f"已收集 {len(weather_data)} 条数据")
            except Exception as e:
                logger.error(f"处理消息时出错: {str(e)}")
                continue
        else:
            logger.warning("没有收到新消息，等待中...")
            continue  # 继续等待新消息

    logger.info(f"成功收集到 {len(weather_data)} 条数据")

    # 将数据转换为 DataFrame
    df = pd.DataFrame(weather_data)
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df['temperature'] = df['temperature'].apply(lambda x: float(x.split('/')[0]) if x and x.split('/')[0] else None)
    df = df.dropna(subset=['temperature'])  # 删除无效温度数据

    if df.empty:
        raise ValueError("没有有效的数据可以分析")

    # 设置matplotlib样式
    plt.style.use('seaborn')
    plt.rcParams['font.sans-serif'] = ['SimHei']  # 设置中文字体
    plt.rcParams['axes.unicode_minus'] = False  # 正确显示负号

    # 生成图表
    plt.figure(figsize=(12, 6))
    for city in df['city'].unique():
        city_data = df[df['city'] == city]
        plt.plot(city_data['timestamp'], city_data['temperature'], label=city, marker='o')

    plt.title('各城市温度变化')
    plt.xlabel('时间')
    plt.ylabel('温度 (°C)')
    plt.legend()
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig('temperature_changes.png')
    plt.close()

    logger.info("分析完成，图表已保存。")

except Exception as e:
    logger.error(f"处理过程中发生错误: {str(e)}", exc_info=True)

finally:
    consumer.close()
    logger.info("Kafka消费者已关闭")'''



'''import json
import time
import logging
from kafka import KafkaConsumer
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# 安全的JSON解析函数
def safe_json_loads(data):
    try:
        return json.loads(data.decode('utf-8'))
    except json.JSONDecodeError as e:
        logger.error(f"JSON解析错误: {e}")
        logger.error(f"问题数据: {data.decode('utf-8', errors='ignore')}")
        return None

# 安全的温度转换函数
def safe_temp_convert(temp_str):
    try:
        return float(temp_str.split('/')[0])
    except (ValueError, AttributeError, IndexError):
        logger.warning(f"无法转换温度值: {temp_str}")
        return None

# 创建 Kafka 消费者
consumer = KafkaConsumer(
    'weather_topic',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='weather_analysis_group',
    value_deserializer=safe_json_loads
)

# 收集数据
weather_data = []
start_time = time.time()
timeout = 300  # 5分钟超时

try:
    while len(weather_data) < 7 * 3 and time.time() - start_time < timeout:
        message = next(consumer, None)
        if message and message.value:
            weather_data.extend(message.value if isinstance(message.value, list) else [message.value])
        elif message is None:
            logger.warning("没有收到新消息，可能已经到达主题末尾")
            break

    if len(weather_data) < 7 * 3:
        logger.warning(f"在规定时间内未收集到足够的数据。当前数据量: {len(weather_data)}")

    # 将数据转换为 DataFrame
    df = pd.DataFrame(weather_data)
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df['temperature'] = df['temperature'].apply(safe_temp_convert)
    df = df.dropna(subset=['temperature'])  # 删除无效温度数据

    if df.empty:
        raise ValueError("没有有效的数据可以分析")

    # 设置matplotlib样式
    plt.style.use('seaborn')
    plt.rcParams['font.sans-serif'] = ['SimHei']  # 设置中文字体
    plt.rcParams['axes.unicode_minus'] = False  # 正确显示负号

    # 1. 每个城市之间温度随时间变化趋势的对比
    plt.figure(figsize=(15, 8))
    for city in df['city'].unique():
        city_data = df[df['city'] == city].sort_values('timestamp')
        plt.plot(city_data['timestamp'], city_data['temperature'], marker='o', label=city)

    plt.title('城市间温度随时间变化趋势对比')
    plt.xlabel('时间')
    plt.ylabel('温度 (°C)')
    plt.legend(title='城市', bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.xticks(rotation=45)
    plt.grid(True, linestyle='--', alpha=0.7)
    plt.tight_layout()
    plt.savefig('city_temperature_trend_comparison.png')
    plt.close()

    # 2. 每个城市的天气状况随时间变化的对比
    cities = df['city'].unique()
    n_cities = len(cities)
    fig, axes = plt.subplots(n_cities, 1, figsize=(15, 5*n_cities), sharex=True)
    fig.suptitle('城市天气状况随时间变化对比', fontsize=16)

    for i, city in enumerate(cities):
        city_data = df[df['city'] == city].sort_values('timestamp')
        weather_counts = pd.crosstab(city_data['timestamp'], city_data['weather'])
        weather_counts.plot(kind='bar', stacked=True, ax=axes[i] if n_cities > 1 else axes)
        (axes[i] if n_cities > 1 else axes).set_title(f'{city}天气状况变化')
        (axes[i] if n_cities > 1 else axes).set_ylabel('次数')
        (axes[i] if n_cities > 1 else axes).legend(title='天气状况', bbox_to_anchor=(1.05, 1), loc='upper left')

    plt.xlabel('时间')
    plt.tight_layout()
    plt.savefig('city_weather_condition_trend_comparison.png')
    plt.close()

    # 3. 温度变化热力图
    pivot_temp = df.pivot(index='timestamp', columns='city', values='temperature')
    plt.figure(figsize=(12, 8))
    sns.heatmap(pivot_temp, annot=True, fmt='.1f', cmap='YlOrRd')
    plt.title('城市温度变化热力图')
    plt.ylabel('时间')
    plt.xlabel('城市')
    plt.tight_layout()
    plt.savefig('city_temperature_heatmap.png')
    plt.close()

    # 4. 添加统计分析
    city_stats = df.groupby('city')['temperature'].agg(['mean', 'min', 'max'])
    logger.info("城市温度统计:\n%s", city_stats)

    logger.info("分析完成，图表已保存。")

except Exception as e:
    logger.error(f"处理过程中发生错误: {str(e)}", exc_info=True)

finally:
    consumer.close()
    logger.info("Kafka消费者已关闭")'''