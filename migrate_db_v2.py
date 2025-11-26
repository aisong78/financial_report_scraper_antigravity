import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).parent / "finance.db"

def migrate():
    print(f"🚀 开始数据库迁移 (v2.0)...")
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # 1. 检查并添加 currency 字段
    try:
        cursor.execute("ALTER TABLE financial_reports_raw ADD COLUMN currency TEXT DEFAULT 'CNY'")
        print("  ✅ 添加 currency 字段成功")
    except sqlite3.OperationalError:
        print("  ⚠️ currency 字段已存在")
        
    # 2. 检查并添加 market 字段
    try:
        cursor.execute("ALTER TABLE financial_reports_raw ADD COLUMN market TEXT DEFAULT 'CN'")
        print("  ✅ 添加 market 字段成功")
    except sqlite3.OperationalError:
        print("  ⚠️ market 字段已存在")

    # 3. 检查并添加 eps_basic, bps, debt_to_asset 等港股常用字段
    new_fields = ['eps_basic', 'bps', 'debt_to_asset']
    for field in new_fields:
        try:
            cursor.execute(f"ALTER TABLE financial_reports_raw ADD COLUMN {field} REAL")
            print(f"  ✅ 添加 {field} 字段成功")
        except sqlite3.OperationalError:
            print(f"  ⚠️ {field} 字段已存在")

    conn.commit()
    conn.close()
    print("✅ 迁移完成！")

if __name__ == "__main__":
    migrate()
