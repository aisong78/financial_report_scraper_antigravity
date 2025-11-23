import streamlit as st
import pandas as pd
import akshare as ak
import plotly.graph_objects as go
import sqlite3
from datetime import datetime
from pathlib import Path
from data_fetcher import DataFetcher
from calculator import FinancialCalculator

# 数据库路径
DB_PATH = Path(__file__).parent / "finance.db"

# 初始化工具
fetcher = DataFetcher()
calculator = FinancialCalculator()

# 设置页面配置
st.set_page_config(
    page_title="Antigravity 智能财报分析",
    page_icon="🚀",
    layout="wide"
)

def get_stock_data(stock_code):
    """
    获取股票数据：先查库，没有则抓取
    """
    conn = sqlite3.connect(DB_PATH)
    
    # 1. 检查数据库是否有数据
    df = pd.read_sql(f"SELECT * FROM financial_indicators_derived WHERE stock_code='{stock_code}' ORDER BY report_period DESC LIMIT 1", conn)
    
    if df.empty:
        st.info(f"本地无 {stock_code} 数据，正在从云端抓取 (2010-2024)...")
        success = fetcher.fetch_a_stock_financials(stock_code)
        if success:
            calculator.calculate_indicators(stock_code)
            # 重新读取
            df = pd.read_sql(f"SELECT * FROM financial_indicators_derived WHERE stock_code='{stock_code}' ORDER BY report_period DESC LIMIT 1", conn)
        else:
            st.error("抓取失败，请检查股票代码是否正确。")
            return None, None
            
    conn.close()
    
    # 获取实时行情（用于展示市值等）
    try:
        stock_info = ak.stock_individual_info_em(symbol=stock_code)
        info_dict = dict(zip(stock_info['item'], stock_info['value']))
    except:
        info_dict = {}
        
    return info_dict, df.iloc[0] if not df.empty else None

def analyze_gap(metrics, framework_type="value"):
    """
    差距分析引擎
    """
    results = []
    score = 0
    total_score = 0
    
    def safe_get(val, default=0):
        return val if val is not None else default
        
    def format_val(val, is_percent=True):
        if val is None: return "-"
        return f"{val:.2f}%" if is_percent else f"{val/1e8:.2f} 亿"

    if framework_type == "价值投资 (巴菲特)":
        # 1. ROE
        target = 15
        actual = metrics['roe']
        if actual is not None:
            gap = actual - target
            status = "✅ 达标" if gap >= 0 else "❌ 未达标"
            score += 100 if gap >= 0 else (50 if gap > -5 else 0)
        else:
            gap = 0
            status = "⚠️ 数据缺失"
            
        results.append({
            "指标": "ROE (净资产收益率)",
            "标准": f"> {target}%",
            "实际": format_val(actual),
            "差距": f"{gap:+.2f}%" if actual is not None else "-",
            "状态": status,
            "解读": "盈利能力强劲" if actual is not None and gap >= 0 else "盈利能力较弱或数据缺失"
        })
        
        # 2. 毛利率
        target = 40
        actual = metrics['gross_margin']
        if actual is not None:
            gap = actual - target
            status = "✅ 达标" if gap >= 0 else "⚠️ 偏低"
        else:
            gap = 0
            status = "⚠️ 数据缺失"
            
        results.append({
            "指标": "毛利率",
            "标准": f"> {target}%",
            "实际": format_val(actual),
            "差距": f"{gap:+.2f}%" if actual is not None else "-",
            "状态": status,
            "解读": "产品具备定价权" if actual is not None and gap >= 0 else "竞争激烈或数据缺失"
        })
        
        # 3. 负债率
        target = 60
        actual = metrics['debt_to_asset']
        if actual is not None:
            gap = target - actual # 越低越好
            status = "✅ 达标" if gap >= 0 else "❌ 风险"
        else:
            gap = 0
            status = "⚠️ 数据缺失"
            
        results.append({
            "指标": "资产负债率",
            "标准": f"< {target}%",
            "实际": format_val(actual),
            "差距": f"{gap:+.2f}% (安全空间)" if actual is not None else "-",
            "状态": status,
            "解读": "财务结构健康" if actual is not None and gap >= 0 else "杠杆过高或数据缺失"
        })
        
        # 4. 自由现金流
        actual = metrics['fcf']
        if actual is not None:
            status = "✅ 正向" if actual > 0 else "❌ 负向"
        else:
            status = "⚠️ 数据缺失"
            
        results.append({
            "指标": "自由现金流",
            "标准": "> 0",
            "实际": format_val(actual, is_percent=False),
            "差距": "-",
            "状态": status,
            "解读": "具备造血能力" if actual is not None and actual > 0 else "持续烧钱或数据缺失"
        })

    return results

# --- 界面逻辑 ---

# 侧边栏
with st.sidebar:
    st.title("🚀 控制台")
    if 'watchlist' not in st.session_state:
        st.session_state.watchlist = ['600519', '688005', '000858']
    
    new_stock = st.text_input("添加股票代码", placeholder="如 00700")
    if st.button("添加"):
        if new_stock and new_stock not in st.session_state.watchlist:
            st.session_state.watchlist.append(new_stock)
            
    selected_stock = st.radio("选择股票", st.session_state.watchlist)
    
    st.markdown("---")
    st.subheader("数据筛选")
    report_type = st.selectbox("报告类型", ["全部", "年报 (A)", "三季报 (Q3)", "半年报 (S1)", "一季报 (Q1)"], index=0)
    
    if st.button("强制更新数据"):
        fetcher.fetch_a_stock_financials(selected_stock)
        calculator.calculate_indicators(selected_stock)
        st.success("已更新！")

# 主界面
st.title(f"📊 {selected_stock} 财务数据全景")

# 获取数据函数
def get_all_history(stock_code):
    conn = sqlite3.connect(DB_PATH)
    df_raw = pd.read_sql(f"SELECT * FROM financial_reports_raw WHERE stock_code='{stock_code}' ORDER BY report_period DESC", conn)
    df_derived = pd.read_sql(f"SELECT * FROM financial_indicators_derived WHERE stock_code='{stock_code}' ORDER BY report_period DESC", conn)
    conn.close()
    return df_raw, df_derived

# 初始化 session_state
if 'df_raw' not in st.session_state:
    st.session_state.df_raw = pd.DataFrame()
if 'df_derived' not in st.session_state:
    st.session_state.df_derived = pd.DataFrame()

# 加载按钮逻辑
if st.button("加载/刷新数据", type="primary"):
    with st.spinner("正在提取历史数据..."):
        # 检查是否需要抓取
        conn = sqlite3.connect(DB_PATH)
        check_df = pd.read_sql(f"SELECT id FROM financial_reports_raw WHERE stock_code='{selected_stock}' LIMIT 1", conn)
        conn.close()
        
        if check_df.empty:
            st.info("本地无数据，正在云端抓取...")
            fetcher.fetch_a_stock_financials(selected_stock)
            calculator.calculate_indicators(selected_stock)
            
        raw, derived = get_all_history(selected_stock)
        st.session_state.df_raw = raw
        st.session_state.df_derived = derived
        st.success("数据已加载！")

# 数据展示逻辑 (只要 session_state 有数据就显示)
if not st.session_state.df_raw.empty:
    df_raw = st.session_state.df_raw
    df_derived = st.session_state.df_derived

    # --- 1. 筛选器 (移至主界面) ---
    st.markdown("### 🛠️ 数据筛选")
    col_filter, col_empty = st.columns([1, 3])
    with col_filter:
        report_type = st.selectbox("只看哪种报表？", ["全部", "年报 (A)", "三季报 (Q3)", "半年报 (S1)", "一季报 (Q1)"], index=0)

    # --- 2. 数据预处理：生成 '2023A' 格式的列名 ---
    # 统一转换为 datetime 对象，处理可能的字符串或 Timestamp
    df_raw['report_period_dt'] = pd.to_datetime(df_raw['report_period'])
    # --- 2. 数据预处理：生成 '2023A' 格式的列名 ---
    # 统一转换为 datetime 对象
    df_raw['report_period_dt'] = pd.to_datetime(df_raw['report_period'])
    df_derived['report_period_dt'] = pd.to_datetime(df_derived['report_period'])
    
    # 关键修复：df_derived 表里没有 report_type 字段，需要从 df_raw 合并过来
    # 或者简单处理：直接根据 report_period 匹配
    # 这里我们假设两者行数一致且顺序一致（因为都是 ORDER BY report_period DESC）
    # 更稳妥的做法是 merge
    if 'report_type' not in df_derived.columns:
        temp_map = df_raw[['report_period', 'report_type']].drop_duplicates()
        df_derived = pd.merge(df_derived, temp_map, on='report_period', how='left')

    def generate_name(row):
        try:
            year = row['report_period_dt'].year
            rtype = row['report_type']
            return f"{year}{rtype}"
        except:
            return str(row['report_period'])

    df_raw['report_name'] = df_raw.apply(generate_name, axis=1)
    df_derived['report_name'] = df_derived.apply(generate_name, axis=1)
        
    # 调试：在侧边栏显示数据状态
    with st.sidebar:
        st.markdown("---")
        st.caption("🔧 调试信息")
        st.write(f"原始行数: {len(df_raw)}")
        st.write(f"包含类型: {df_raw['report_type'].unique()}")

    # --- 3. 执行筛选 ---
    type_map = {"年报 (A)": "A", "三季报 (Q3)": "Q3", "半年报 (S1)": "S1", "一季报 (Q1)": "Q1"}
    
    if report_type != "全部":
        target_type = type_map[report_type]
        # 严格筛选
        df_raw = df_raw[df_raw['report_type'] == target_type]
        df_derived = df_derived[df_derived['report_type'] == target_type]
        
        if df_raw.empty:
            st.error(f"筛选 '{target_type}' 后数据为空！请检查数据源。")
    
    # --- 3.5 数据质量统计 ---
    if 'data_quality' in df_raw.columns:
        st.markdown("### 📊 数据质量概览")
        col1, col2, col3 = st.columns(3)
        
        verified_count = len(df_raw[df_raw['data_quality'] == 'VERIFIED'])
        unverified_count = len(df_raw[df_raw['data_quality'] == 'UNVERIFIED'])
        conflict_count = len(df_raw[df_raw['data_quality'] == 'CONFLICT'])
        
        with col1:
            st.metric("✅ 已验证", verified_count)
        with col2:
            st.metric("⚠️ 未验证", unverified_count)
        with col3:
            st.metric("❌ 数据冲突", conflict_count)
        
        st.markdown("---")
    
    # --- 4. 辅助函数：转置表格 ---
    def transpose_df(df, index_col='report_name', exclude_cols=['id', 'stock_code', 'currency', 'publish_date', 'report_period', 'report_type', 'report_period_dt', 'data_quality']):
        if df.empty: return pd.DataFrame()
        # 确保索引唯一
        df = df.drop_duplicates(subset=[index_col])
        # 设置索引
        df = df.set_index(index_col)
        # 剔除无关列
        cols = [c for c in df.columns if c not in exclude_cols]
        df = df[cols]
        # 转置
        return df.T

    # --- 5. 字段映射字典 (补全) ---
    field_map = {
        # 衍生指标
        'gross_margin': '毛利率 (Gross Margin) [%]',
        'net_margin': '净利率 (Net Margin) [%]',
        'roe': '净资产收益率 (ROE) [%]',
        'roa': '总资产收益率 (ROA) [%]',
        'revenue_yoy': '营收增长率 (YoY) [%]',
        'net_profit_yoy': '净利增长率 (YoY) [%]',
        'debt_to_asset': '资产负债率 [%]',
        'current_ratio': '流动比率',
        'inventory_turnover_days': '存货周转天数 [天]',
        'receivables_turnover_days': '应收账款周转天数 [天]', # 补全
        'fcf': '自由现金流 (FCF) [亿]',
        'cfo_to_net_income': '净现比',
        'dividend_payout_ratio': '分红率 [%]',
        'dividend_per_share': '每股分红 [元]',
        'dividend_total': '分红总额 [亿]', # 补全
        'eps_basic': '基本每股收益 (EPS) [元]',
        'eps_ttm': '滚动每股收益 (EPS-TTM) [元]',
        'bps': '每股净资产 (BPS) [元]',
        
        # 利润表
        'revenue': '营业收入 [亿]',
        'cost_of_revenue': '营业成本 [亿]',
        'gross_profit': '毛利 [亿]',
        'selling_expenses': '销售费用 [亿]',
        'admin_expenses': '管理费用 [亿]',
        'rd_expenses': '研发费用 [亿]',
        'financial_expenses': '财务费用 [亿]',
        'investment_income': '投资收益 [亿]',
        'operating_income': '营业利润 [亿]',
        'total_profit': '利润总额 [亿]',
        'net_income': '净利润 [亿]',
        'net_income_parent': '归母净利润 [亿]',
        'net_income_deducted': '扣非净利润 [亿]',
        
        # 资产负债表
        'total_assets': '总资产 [亿]',
        'total_liabilities': '总负债 [亿]',
        'total_equity': '股东权益 [亿]',
        'cash_equivalents': '货币资金 [亿]',
        'accounts_receivable': '应收账款 [亿]',
        'inventory': '存货 [亿]',
        'fixed_assets': '固定资产 [亿]',
        'goodwill': '商誉 [亿]',
        'short_term_debt': '短期借款 [亿]',
        'long_term_debt': '长期借款 [亿]',
        'accounts_payable': '应付账款 [亿]',
        'contract_liabilities': '合同负债 [亿]',
        
        # 现金流量表
        'cfo_net': '经营现金流净额 [亿]',
        'cfi_net': '投资现金流净额 [亿]',
        'cff_net': '筹资现金流净额 [亿]',
        'capex': '资本开支 [亿]',
        'cash_paid_for_dividends': '分红支付现金 [亿]'
    }

    # --- 数值格式化函数 ---
    def format_dataframe(df_transposed):
        # 这里的 df_transposed 行索引是字段名 (如 'revenue')
        
        def fmt(val, field_name):
            if val is None: return "-"
            try:
                val = float(val)
            except:
                return val
                
            # 百分比类
            if any(x in field_name for x in ['margin', 'roe', 'roa', 'yoy', 'ratio', 'percent', 'rate']):
                if 'current_ratio' in field_name or 'cfo_to_net' in field_name: # 比率不带%
                    return f"{val:.2f}"
                return f"{val:.2f}%"
            
            # 金额类 (带 [亿] 的)
            if '[亿]' in field_map.get(field_name, ''):
                return f"{val/1e8:.2f}"
            
            # 天数/每股
            return f"{val:.2f}"

        # 应用格式化
        # 创建一个新的 DataFrame 用于显示
        df_display = df_transposed.copy()
        
        # 重命名索引 (英文 -> 中文)
        new_index = [field_map.get(idx, idx) for idx in df_display.index]
        df_display.index = new_index
        
        # 逐个单元格格式化 (效率较低但逻辑简单)
        # 更好的做法是 applymap，但需要知道原始字段名。
        # 这里我们在重命名前处理数据
        
        for col in df_transposed.columns:
            for idx in df_transposed.index:
                raw_val = df_transposed.loc[idx, col]
                display_val = fmt(raw_val, idx)
                # 填入新表 (注意新表索引已经变了，所以要用位置或映射)
                display_idx = field_map.get(idx, idx)
                df_display.loc[display_idx, col] = display_val
                
        return df_display

    # 1. 核心衍生指标 (表二)
    st.subheader("📈 核心财务指标 (Derived Metrics)")
    st.caption("基于原始数据计算得出的关键比率和增长率")
    
    if not df_derived.empty:
        # 明确指定 index_col='report_name'
        df_t = transpose_df(df_derived, index_col='report_name')
        st.dataframe(format_dataframe(df_t), use_container_width=True, height=400)
    else:
        st.warning("暂无衍生指标数据")

    # 2. 原始财务报表 (表一)
    st.markdown("---")
    st.subheader("📑 原始财务报表 (Financial Statements)")
    st.caption("从财报中直接提取的原始数据")
    
    if not df_raw.empty:
        # 分类展示，避免表格太长
        tab1, tab2, tab3 = st.tabs(["利润表", "资产负债表", "现金流量表"])
        
        # 明确指定 index_col='report_name'
        df_t = transpose_df(df_raw, index_col='report_name')
        
        # 定义各表包含的字段 (根据 database.py 的定义)
        income_cols = ['revenue', 'cost_of_revenue', 'gross_profit', 'selling_expenses', 'admin_expenses', 'rd_expenses', 'financial_expenses', 'investment_income', 'operating_income', 'total_profit', 'net_income', 'net_income_parent', 'net_income_deducted']
        balance_cols = ['total_assets', 'total_liabilities', 'total_equity', 'cash_equivalents', 'accounts_receivable', 'inventory', 'fixed_assets', 'goodwill', 'short_term_debt', 'long_term_debt', 'accounts_payable', 'contract_liabilities']
        cash_cols = ['cfo_net', 'cfi_net', 'cff_net', 'capex', 'cash_paid_for_dividends']
        
        with tab1:
            valid_cols = [c for c in income_cols if c in df_t.index]
            st.dataframe(format_dataframe(df_t.loc[valid_cols]), use_container_width=True)
            
        with tab2:
            valid_cols = [c for c in balance_cols if c in df_t.index]
            st.dataframe(format_dataframe(df_t.loc[valid_cols]), use_container_width=True)
            
        with tab3:
            valid_cols = [c for c in cash_cols if c in df_t.index]
            st.dataframe(format_dataframe(df_t.loc[valid_cols]), use_container_width=True)
            
    else:
        st.warning("未找到数据。")

