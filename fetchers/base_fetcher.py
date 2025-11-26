import sqlite3
import pandas as pd
from pathlib import Path
from abc import ABC, abstractmethod

class BaseFetcher(ABC):
    def __init__(self, db_path=None):
        if db_path:
            self.db_path = db_path
        else:
            # 默认数据库路径
            self.db_path = Path(__file__).parent.parent / "finance.db"

    @abstractmethod
    def fetch_financial_data(self, stock_code: str):
        """
        抓取财务数据的抽象方法，子类必须实现。
        """
        pass

    def save_to_db(self, stock_code: str, report_period: str, report_type: str, data: dict, market: str = 'CN', currency: str = 'CNY', raw_data: str = None):
        """
        通用的数据保存方法。
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        # 1. 检查是否被锁定
        cursor.execute(
            "SELECT is_locked FROM financial_reports_raw WHERE stock_code=? AND report_period=?",
            (stock_code, report_period)
        )
        row = cursor.fetchone()
        if row and row[0] == 1:
            print(f"  🔒 {report_period} 数据已锁定，跳过更新")
            conn.close()
            return

        # 2. 准备数据
        # 确保 data 里的 None 值被正确处理
        for k, v in data.items():
            if pd.isna(v):
                data[k] = None
                
        # 准备插入字段
        fields = ['stock_code', 'report_period', 'report_type', 'market', 'currency'] + list(data.keys())
        values = [stock_code, report_period, report_type, market, currency] + list(data.values())
        
        if raw_data:
            fields.append('raw_data')
            values.append(raw_data)
            
        placeholders = ', '.join(['?'] * len(fields))
        columns = ', '.join(fields)
        
        sql = f"INSERT OR REPLACE INTO financial_reports_raw ({columns}) VALUES ({placeholders})"
        
        try:
            cursor.execute(sql, values)
            conn.commit()
            print(f"  ✅ 保存成功 {report_period}")
        except Exception as e:
            print(f"  ❌ 保存失败 {report_period}: {e}")
        finally:
            conn.close()
