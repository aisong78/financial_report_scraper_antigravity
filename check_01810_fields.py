import sqlite3
import pandas as pd
import json
from pathlib import Path

DB_PATH = Path(__file__).parent / "finance.db"

def check_fields():
    conn = sqlite3.connect(DB_PATH)
    try:
        # 获取最新的 raw_data
        df = pd.read_sql("SELECT raw_data FROM financial_reports_raw WHERE stock_code='01810' ORDER BY report_period DESC LIMIT 1", conn)
        if not df.empty and df.iloc[0]['raw_data']:
            data = json.loads(df.iloc[0]['raw_data'])
            print("🔍 小米 (01810) 字段列表:")
            
            # 打印所有字段
            for k in sorted(data.keys()):
                print(f"  - {k}")
        else:
            print("❌ 没有找到数据")
    finally:
        conn.close()

if __name__ == "__main__":
    check_fields()
