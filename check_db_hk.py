import sqlite3
import pandas as pd
from pathlib import Path

DB_PATH = Path(__file__).parent / "finance.db"

def check_hk_data():
    print("🔍 检查数据库中的港股数据...")
    conn = sqlite3.connect(DB_PATH)
    
    # 1. 检查是否有 01810 的数据
    try:
        df = pd.read_sql("SELECT * FROM financial_reports_raw WHERE stock_code='01810'", conn)
        if df.empty:
            print("❌ 数据库中没有 01810 的数据。")
        else:
            print(f"✅ 找到 {len(df)} 条 01810 的记录！")
            print("数据预览 (前2条):")
            # 只显示非空列
            df_preview = df.dropna(axis=1, how='all')
            print(df_preview.head(2).T)
            
            # 检查关键字段
            print("\n关键字段检查:")
            cols = ['revenue', 'net_income_parent', 'eps_basic', 'market', 'currency']
            print(df[cols].head())
            
    except Exception as e:
        print(f"❌ 查询失败: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    check_hk_data()
