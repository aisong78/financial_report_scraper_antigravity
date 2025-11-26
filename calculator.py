import pandas as pd
import sqlite3
from pathlib import Path

# 数据库路径
DB_PATH = Path(__file__).parent / "finance.db"

class FinancialCalculator:
    def __init__(self):
        self.db_path = DB_PATH
        
    def calculate_indicators(self, stock_code):
        """
        计算指定股票的衍生指标
        """
        print(f"🧮 开始计算 {stock_code} 的衍生指标...")
        
        conn = sqlite3.connect(self.db_path)
        
        # 1. 读取原始数据 (按时间正序排列)
        df = pd.read_sql(f"SELECT * FROM financial_reports_raw WHERE stock_code='{stock_code}' ORDER BY report_period ASC", conn)
        
        if df.empty:
            print("⚠️ 没有找到原始数据，无法计算。")
            conn.close()
            return False
            
        # 设置 report_period 为索引，方便 shift 操作
        df['report_period'] = pd.to_datetime(df['report_period'])
        df.set_index('report_period', inplace=True)
        
        # 2. 计算指标
        indicators = pd.DataFrame(index=df.index)
        indicators['stock_code'] = stock_code
        indicators['report_period'] = df.index.strftime('%Y-%m-%d') # 转回字符串存库
        
        # --- A. 盈利能力 ---
        # 辅助函数：安全除法
        def safe_div(a, b):
            if pd.isna(a) or pd.isna(b) or b == 0:
                return None
            return a / b

        # 2. 计算衍生指标
        # 盈利能力
        indicators['gross_margin'] = df.apply(lambda x: safe_div(x['gross_profit'], x['revenue']) * 100 if safe_div(x['gross_profit'], x['revenue']) is not None else None, axis=1)
        indicators['net_margin'] = df.apply(lambda x: safe_div(x['net_income'], x['revenue']) * 100 if safe_div(x['net_income'], x['revenue']) is not None else None, axis=1)
        indicators['roe'] = df.apply(lambda x: safe_div(x['net_income_parent'], x['total_equity']) * 100 if safe_div(x['net_income_parent'], x['total_equity']) is not None else None, axis=1)
        indicators['roa'] = df.apply(lambda x: safe_div(x['net_income'], x['total_assets']) * 100 if safe_div(x['net_income'], x['total_assets']) is not None else None, axis=1)
        
        # 成长能力 (YoY)
        # 成长能力 (YoY)
        # 数据是正序排列的 (2022, 2023, ...)，所以比较上一行 (去年)
        # 假设主要是年度数据，所以 periods=1
        indicators['revenue_yoy'] = df['revenue'].pct_change(periods=1) * 100
        indicators['net_profit_yoy'] = df['net_income_parent'].pct_change(periods=1) * 100
        
        # 偿债能力
        indicators['debt_to_asset'] = df.apply(lambda x: safe_div(x['total_liabilities'], x['total_assets']) * 100 if safe_div(x['total_liabilities'], x['total_assets']) is not None else None, axis=1)
        # 流动比率 = 流动资产 / 流动负债
        indicators['current_ratio'] = df.apply(lambda x: safe_div(x['current_assets'], x['current_liabilities']) if safe_div(x['current_assets'], x['current_liabilities']) is not None else None, axis=1)
        
        # 运营能力
        # 存货周转天数 = 365 * 存货 / 营业成本
        indicators['inventory_turnover_days'] = df.apply(lambda x: safe_div(365 * x['inventory'], x['cost_of_revenue']) if pd.notna(x['inventory']) else None, axis=1)
        # 应收账款周转天数 = 365 * 应收账款 / 营业收入
        indicators['receivables_turnover_days'] = df.apply(lambda x: safe_div(365 * x['accounts_receivable'], x['revenue']) if pd.notna(x['accounts_receivable']) else None, axis=1)
        
        # 现金流
        # 自由现金流 FCF = 经营现金流净额 - 资本开支
        # 注意：如果 capex 是 None，结果也是 None，这是 pandas 的特性，不会报错
        indicators['fcf'] = df['cfo_net'] - df['capex']
        
        # 净现比 = 经营现金流净额 / 净利润
        indicators['cfo_to_net_income'] = df.apply(lambda x: safe_div(x['cfo_net'], x['net_income']) if safe_div(x['cfo_net'], x['net_income']) is not None else None, axis=1)
        
        # --- D. 成长能力 (YoY) ---
        # 已在上方通过 pct_change 计算，此处移除重复且易报错的 merge 逻辑

        
        # --- E. TTM 数据 (滚动12个月) ---
        # 仅针对季报/半年报计算。年报 TTM = 年报本身。
        # TTM = 本期累计 + (上年年报 - 上年同期累计)
        # 这是一个比较复杂的逻辑，为了 MVP 快速上线，我们暂时先用“年报数据”作为 TTM 的近似值（如果是非年报，则不计算或沿用上年数据）。
        # 后续我们会完善这个 TTM 算法。
        indicators['net_profit_ttm'] = df['net_income_parent'] # 临时占位
        
        # 3. 存入数据库
        cursor = conn.cursor()
        
        # 逐行插入
        for idx, row in indicators.iterrows():
            # 处理 NaN 为 None
            row = row.where(pd.notnull(row), None)
            
            data = {
                'stock_code': stock_code,
                'report_period': row['report_period'],
                'gross_margin': row['gross_margin'],
                'net_margin': row['net_margin'],
                'roe': row['roe'],
                'roa': row['roa'],
                'revenue_yoy': row['revenue_yoy'],
                'net_profit_yoy': row['net_profit_yoy'],
                'debt_to_asset': row['debt_to_asset'],
                'inventory_turnover_days': row['inventory_turnover_days'],
                'fcf': row['fcf'],
                'cfo_to_net_income': row['cfo_to_net_income']
            }
            
            columns = ', '.join(data.keys())
            placeholders = ', '.join(['?'] * len(data))
            
            sql = f'''
            INSERT OR REPLACE INTO financial_indicators_derived ({columns})
            VALUES ({placeholders})
            '''
            cursor.execute(sql, list(data.values()))
            
        conn.commit()
        conn.close()
        print(f"✅ {stock_code} 指标计算完成！")

if __name__ == "__main__":
    calc = FinancialCalculator()
    calc.calculate_indicators("688005")
