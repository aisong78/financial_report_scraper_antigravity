import sqlite3
import pandas as pd
import json
from pathlib import Path

DB_PATH = Path(__file__).parent / "finance.db"

def check_raw_data():
    print("🔍 检查 raw_data 字段...")
    conn = sqlite3.connect(DB_PATH)
    
    try:
        # 读取 raw_data
        df = pd.read_sql("SELECT stock_code, report_period, raw_data FROM financial_reports_raw WHERE stock_code='01810' ORDER BY report_period DESC LIMIT 1", conn)
        
        if df.empty:
            print("❌ 没有找到 01810 的数据。请先在 UI 上点击更新。")
        else:
            raw_json = df.iloc[0]['raw_data']
            if raw_json:
                data = json.loads(raw_json)
                print(f"✅ 成功读取 raw_data！")
                print(f"📊 包含字段数: {len(data)}")
                print(f"👀 字段预览 (前10个): {list(data.keys())[:10]}")
                
                # 检查一些不在核心表里的冷门字段
                rare_fields = ['递延税项资产', '汇兑收益', '其他非流动负债']
                print("\n🔍 冷门字段检查:")
                for f in rare_fields:
                    val = data.get(f)
                    print(f"   - {f}: {val}")
            else:
                print("❌ raw_data 为空。可能 Fetcher 还没更新或没运行。")
            
    except Exception as e:
        print(f"❌ 查询失败: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    check_raw_data()
