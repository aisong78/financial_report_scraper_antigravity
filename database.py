import sqlite3
from pathlib import Path
import pandas as pd
from datetime import datetime

# 数据库文件路径
DB_PATH = Path(__file__).parent / "finance.db"

def init_db():
    """初始化数据库：创建表结构"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # --- 1. 原始财务数据表 (financial_reports_raw) ---
    # 包含 A/港/美 三地市场的核心字段
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS financial_reports_raw (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        stock_code TEXT NOT NULL,       -- 股票代码 (如 600519)
        report_period TEXT NOT NULL,    -- 报告期 (如 2023-12-31)
        report_type TEXT NOT NULL,      -- 报告类型 (Q1/S1/Q3/A)
        publish_date TEXT,              -- 财报发布日期
        currency TEXT,                  -- 货币单位 (CNY/USD/HKD)
        data_quality TEXT DEFAULT 'UNVERIFIED',  -- 数据质量标记 (VERIFIED/UNVERIFIED/CONFLICT)
        
        -- A. 利润表
        revenue REAL,                   -- 营业收入
        cost_of_revenue REAL,           -- 营业成本
        gross_profit REAL,              -- 毛利
        selling_expenses REAL,          -- 销售费用
        admin_expenses REAL,            -- 管理费用
        rd_expenses REAL,               -- 研发费用
        financial_expenses REAL,        -- 财务费用
        investment_income REAL,         -- 投资收益
        operating_income REAL,          -- 营业利润
        total_profit REAL,              -- 利润总额
        net_income REAL,                -- 净利润
        net_income_parent REAL,         -- 归母净利润
        net_income_deducted REAL,       -- 扣非净利润 (A股)
        
        -- B. 资产负债表
        total_assets REAL,              -- 总资产
        total_liabilities REAL,         -- 总负债
        total_equity REAL,              -- 股东权益
        cash_equivalents REAL,          -- 货币资金
        accounts_receivable REAL,       -- 应收账款
        inventory REAL,                 -- 存货
        fixed_assets REAL,              -- 固定资产
        goodwill REAL,                  -- 商誉
        short_term_debt REAL,           -- 短期借款
        long_term_debt REAL,            -- 长期借款
        accounts_payable REAL,          -- 应付账款
        contract_liabilities REAL,      -- 合同负债
        
        -- C. 现金流量表
        cfo_net REAL,                   -- 经营活动现金流净额
        cfi_net REAL,                   -- 投资活动现金流净额
        cff_net REAL,                   -- 筹资活动现金流净额
        capex REAL,                     -- 资本开支
        cash_paid_for_dividends REAL,   -- 分红支付的现金
        
        -- 唯一索引：同一只股票同一个报告期只能有一条记录
        UNIQUE(stock_code, report_period)
    )
    ''')
    
    # --- 2. 衍生财务指标表 (financial_indicators_derived) ---
    # 基于原始数据计算出的比率和增长率
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS financial_indicators_derived (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        stock_code TEXT NOT NULL,
        report_period TEXT NOT NULL,
        
        -- 盈利能力
        gross_margin REAL,              -- 毛利率 (%)
        net_margin REAL,                -- 净利率 (%)
        roe REAL,                       -- ROE (%)
        roa REAL,                       -- ROA (%)
        
        -- 成长能力 (同比)
        revenue_yoy REAL,               -- 营收增长率 (%)
        net_profit_yoy REAL,            -- 净利增长率 (%)
        
        -- 偿债与运营
        debt_to_asset REAL,             -- 资产负债率 (%)
        current_ratio REAL,             -- 流动比率
        inventory_turnover_days REAL,   -- 存货周转天数
        receivables_turnover_days REAL, -- 应收周转天数
        
        -- 现金流与分红
        fcf REAL,                       -- 自由现金流
        cfo_to_net_income REAL,         -- 净现比
        dividend_payout_ratio REAL,     -- 分红率 (%)
        dividend_per_share REAL,        -- 每股分红
        
        -- 每股数据 (用于估值)
        eps_basic REAL,                 -- 基本EPS
        eps_ttm REAL,                   -- 滚动EPS (TTM)
        bps REAL,                       -- 每股净资产
        
        UNIQUE(stock_code, report_period)
    )
    ''')
    
    # --- 4. 财报文件记录表 (financial_reports_files) ---
    # 记录所有下载的 PDF/HTML 原始文件
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS financial_reports_files (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        stock_code TEXT NOT NULL,
        report_period TEXT NOT NULL,
        report_type TEXT NOT NULL,
        file_type TEXT,                 -- 'PDF' 或 'HTML'
        file_path TEXT,                 -- 相对路径，如 'downloads/688005/2023年年报.pdf'
        txt_path TEXT,                  -- 解析后的 TXT 路径
        download_date TEXT,             -- 下载时间
        file_size INTEGER,              -- 文件大小（字节）
        parse_status TEXT DEFAULT 'PENDING',  -- 解析状态 (PENDING/SUCCESS/FAILED)
        
        UNIQUE(stock_code, report_period, report_type)
    )
    ''')
    
    # --- 5. 指标字典表 (metric_definitions) ---
    # 存储指标的中文解释
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS metric_definitions (
        metric_code TEXT PRIMARY KEY,
        metric_name TEXT NOT NULL,
        description TEXT,
        good_standard TEXT
    )
    ''')
    
    # 预填充一些字典数据
    definitions = [
        ('roe', '净资产收益率', '衡量股东资金的使用效率，巴菲特最看重的指标。', '一般 >15% 为优秀，<10% 为一般'),
        ('gross_margin', '毛利率', '衡量产品的直接获利能力和护城河。', '茅台 >90%，一般制造业 20-30%'),
        ('debt_to_asset', '资产负债率', '衡量公司的杠杆风险。', '一般 <60% 比较安全，金融业除外'),
        ('cfo_net', '经营现金流净额', '公司通过卖货真正收回来的现金。', '长期应大于净利润')
    ]
    cursor.executemany('INSERT OR IGNORE INTO metric_definitions VALUES (?,?,?,?)', definitions)
    
    conn.commit()
    conn.close()
    print(f"数据库已初始化: {DB_PATH}")

if __name__ == "__main__":
    init_db()
