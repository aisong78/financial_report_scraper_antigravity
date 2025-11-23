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
        # 毛利率 = 毛利 / 营业收入 * 100
        indicators['gross_margin'] = df.apply(lambda x: (x['gross_profit'] / x['revenue'] * 100) if x['revenue'] else None, axis=1)
        
        # 净利率 = 净利润 / 营业收入 * 100
        indicators['net_margin'] = df.apply(lambda x: (x['net_income'] / x['revenue'] * 100) if x['revenue'] else None, axis=1)
        
        # ROE = 净利润 / 归母股东权益 * 100 (简化版：使用期末权益，严格版应用平均权益)
        indicators['roe'] = df.apply(lambda x: (x['net_income_parent'] / x['total_equity'] * 100) if x['total_equity'] else None, axis=1)
        
        # ROA = 净利润 / 总资产 * 100
        indicators['roa'] = df.apply(lambda x: (x['net_income'] / x['total_assets'] * 100) if x['total_assets'] else None, axis=1)
        
        # --- B. 偿债与运营 ---
        # 资产负债率 = 总负债 / 总资产 * 100
        indicators['debt_to_asset'] = df.apply(lambda x: (x['total_liabilities'] / x['total_assets'] * 100) if x['total_assets'] else None, axis=1)
        
        # 流动比率 = 流动资产 / 流动负债 (注意：数据库中需要确保有这两个字段，如果没有则为 None)
        # 我们的 raw 表里暂时没抓流动资产/负债合计，这里先留空或用近似值
        indicators['current_ratio'] = None 
        
        # 存货周转天数 = 365 / (营业成本 / 平均存货)
        # 简化版：365 * 存货 / 营业成本
        indicators['inventory_turnover_days'] = df.apply(lambda x: (365 * x['inventory'] / x['cost_of_revenue']) if x['cost_of_revenue'] else None, axis=1)
        
        # --- C. 现金流 ---
        # 自由现金流 FCF = 经营现金流净额 - 资本开支
        # 注意：资本开支通常是负数（流出），如果数据库存的是正数代表流出，则用减法；如果是负数则用加法。
        # AkShare 返回的 '购建固定资产...' 通常是正数。
        indicators['fcf'] = df['cfo_net'] - df['capex']
        
        # 净现比 = 经营现金流净额 / 净利润
        indicators['cfo_to_net_income'] = df.apply(lambda x: (x['cfo_net'] / x['net_income']) if x['net_income'] else None, axis=1)
        
        # --- D. 成长能力 (YoY) ---
        # 需要找到去年同期的数据。
        # 简单做法：shift(4) 假设每年4个季度。但如果数据缺失就不准。
        # 精确做法：用 resample 或 merge。这里用 merge self。
        
        df_last_year = df.copy()
        df_last_year.index = df_last_year.index + pd.DateOffset(years=1) # 把去年的时间推到今年，方便对齐
        
        # 合并
        merged = pd.merge(df, df_last_year, left_index=True, right_index=True, suffixes=('', '_last'), how='left')
        
        # 营收增长率
        indicators['revenue_yoy'] = (merged['revenue'] - merged['revenue_last']) / merged['revenue_last'].abs() * 100
        
        # 净利增长率
        indicators['net_profit_yoy'] = (merged['net_income_parent'] - merged['net_income_parent_last']) / merged['net_income_parent_last'].abs() * 100
        
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
