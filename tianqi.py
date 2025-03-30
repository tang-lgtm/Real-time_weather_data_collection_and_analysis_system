import requests
from bs4 import BeautifulSoup
from datetime import datetime
import json
import time
import os

def scrape_weather(city_code):
    url = f"http://www.weather.com.cn/weather1d/{city_code}.shtml"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        response.encoding = 'utf-8'

        soup = BeautifulSoup(response.text, 'html.parser')

        city = soup.select('div.crumbs.fl a')[-1].text if soup.select('div.crumbs.fl a') else "未知城市"
        date = soup.select('input#hidden_title')[0]['value'].split(' ')[0] if soup.select(
            'input#hidden_title') else "未知日期"
        weather = soup.select('p.wea')[0].text if soup.select('p.wea') else "未知天气"

        temp_high = soup.select('p.tem span')
        temp_low = soup.select('p.tem i')
        temperature = f"{temp_high[0].text if temp_high else ''}/{temp_low[0].text if temp_low else ''}"


        weather_info = {
            "city": city,
            "date": date,
            "weather": weather,
            "temperature": temperature,
            "timestamp": datetime.now().isoformat()
        }

        return weather_info

    except Exception as e:
        print(f"Error scraping weather data for {city_code}: {e}")
        return None
def write_jsonl_file(data, directory):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f'F:/data_scrap/examination/weather_data/weather_data_{timestamp}.json'
    with open(filename, 'w', encoding='utf-8') as f:
        for item in data:
            json.dump(item, f, ensure_ascii=False)
            f.write('\n')
    print(f"Data written to {filename}")
    return filename

city_codes = {
    "北京": "101010100",
    "上海": "101020100",
    "广州": "101280101",
    "成都": "101270101",
    "海口": "101310101"
}

# 设置爬取次数和间隔
total_scrapes = 21
interval_hours = 1

# 创建一个文件夹来存储数据
data_dir = 'F:/data_scrap/examination/weather_data'
if not os.path.exists(data_dir):
    os.makedirs(data_dir)

for scrape_count in range(total_scrapes):
    all_weather_data = []
    for city, code in city_codes.items():
        print(f"Scraping weather data for {city}...")
        weather_data = scrape_weather(code)
        if weather_data:
            all_weather_data.append(weather_data)

    # 写入新的JSONL文件
    filename = write_jsonl_file(all_weather_data, data_dir)

    if scrape_count < total_scrapes - 1:
        print(f"Waiting for {interval_hours} hours before next scrape...")
        time.sleep(interval_hours * 3600)  # 转换小时为秒

print("Data collection completed.")


'''def save_to_file(data, filename):
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


city_codes = {
    "北京": "101010100",
    "上海": "101020100",
    "广州": "101280101",
    "深圳": "101280601",
    "杭州": "101210101",
    "成都": "101270101",
    "海口": "101310101"
}

# 设置爬取次数和间隔
total_scrapes = 6
interval_hours = 1

# 创建一个文件夹来存储数据
if not os.path.exists('F:/data_scrap/examination/weather_data'):
    os.makedirs('F:/data_scrap/examination/weather_data')

for scrape_count in range(total_scrapes):
    all_weather_data = []
    for city, code in city_codes.items():
        print(f"Scraping weather data for {city}...")
        weather_data = scrape_weather(code)
        if weather_data:
            all_weather_data.append(weather_data)

    # 保存数据到文件
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f'F:/data_scrap/examination/weather_data/weather_data_{timestamp}.json'
    save_to_file(all_weather_data, filename)

    print(f"Data saved to {filename}")

    if scrape_count < total_scrapes - 1:
        print(f"Waiting for {interval_hours} hours before next scrape...")
        time.sleep(interval_hours * 3600)  # 转换小时为秒

print("Data collection completed.")'''