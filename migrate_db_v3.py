import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).parent / "finance.db"

def migrate_v3():
    print("🚀 开始数据库迁移 (v3.0 - 全量数据支持)...")
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # 1. 检查并添加 raw_data 字段
    # 这个字段将存储 JSON 格式的完整 API 返回数据
    try:
        cursor.execute("ALTER TABLE financial_reports_raw ADD COLUMN raw_data TEXT")
        print("  ✅ 添加 raw_data 字段成功")
    except sqlite3.OperationalError:
        print("  ⚠️ raw_data 字段已存在")

    conn.commit()
    conn.close()
    print("✅ 迁移完成！现在数据库可以存储全量报表数据了。")

if __name__ == "__main__":
    migrate_v3()
