import streamlit as st
import pandas as pd
import akshare as ak
import plotly.graph_objects as go
import sqlite3
import json
from datetime import datetime
from pathlib import Path
from fetchers.a_share import AShareFetcher
from fetchers.hk_share import HKShareFetcher
from calculator import FinancialCalculator

# 数据库路径
DB_PATH = Path(__file__).parent / "finance.db"

# 初始化工具
# 初始化工具
# fetcher = AShareFetcher() (已移除全局实例)
calculator = FinancialCalculator()

def get_fetcher(stock_code):
    if len(stock_code) == 5 and stock_code.isdigit():
        return HKShareFetcher()
    return AShareFetcher()

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
        fetcher = get_fetcher(stock_code)
        success = fetcher.fetch_financial_data(stock_code)
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
    
    # 简单直接的股票代码输入
    if 'stock_code' not in st.session_state:
        st.session_state['stock_code'] = '01810' # 默认小米

    def update_code():
        st.session_state['stock_code'] = st.session_state.code_input

    selected_stock = st.text_input(
        "输入股票代码", 
        value=st.session_state['stock_code'],
        key='code_input',
        on_change=update_code,
        help="输入代码后回车，如 01810, 600519"
    )
    
    # 确保同步
    st.session_state['stock_code'] = selected_stock
    
    st.markdown("---")
    st.subheader("数据筛选")
    report_type = st.selectbox("报告类型", ["全部", "年报 (A)", "三季报 (Q3)", "半年报 (S1)", "一季报 (Q1)"], index=0)
    
    if st.button("强制更新数据"):
        fetcher = get_fetcher(selected_stock)
        fetcher.fetch_financial_data(selected_stock)
        calculator.calculate_indicators(selected_stock)
        # 清除缓存以重新加载数据
        if 'df_raw' in st.session_state:
            del st.session_state.df_raw
        st.success("已更新！请刷新页面。")
        st.rerun()

    st.markdown("---")
    with st.expander("✏️ 修正数据 (Manual Override)"):
        st.caption("手动修改数据将锁定该记录，防止被自动覆盖。")
        
        # 获取当前股票的所有报告期
        conn = sqlite3.connect(DB_PATH)
        periods = pd.read_sql(f"SELECT report_period FROM financial_reports_raw WHERE stock_code='{selected_stock}' ORDER BY report_period DESC", conn)['report_period'].tolist()
        conn.close()
        
        edit_period = st.selectbox("选择报告期", periods)
        
        # 字段列表
        edit_fields = {
            'revenue': '营业收入',
            'net_income_parent': '归母净利润',
            'total_assets': '总资产',
            'total_equity': '股东权益',
            'gross_profit': '毛利',
            'net_income': '净利润',
            'cfo_net': '经营现金流净额',
            'income_tax_expenses': '所得税费用',
            'current_assets': '流动资产',
            'non_current_assets': '非流动资产',
            'intangible_assets': '无形资产',
            'current_liabilities': '流动负债',
            'non_current_liabilities': '非流动负债',
            'share_capital': '股本',
            'retained_earnings': '未分配利润',
            'net_cash_flow': '现金净增加额'
        }
        edit_field_key = st.selectbox("选择字段", list(edit_fields.keys()), format_func=lambda x: f"{edit_fields[x]} ({x})")
        
        # 获取当前值
        current_val = 0.0
        if edit_period:
            conn = sqlite3.connect(DB_PATH)
            cursor = conn.cursor()
            cursor.execute(f"SELECT {edit_field_key} FROM financial_reports_raw WHERE stock_code=? AND report_period=?", (selected_stock, edit_period))
            row = cursor.fetchone()
            if row and row[0] is not None:
                current_val = float(row[0])
            conn.close()
            
        new_val = st.number_input("新值 (单位: 元)", value=current_val, format="%.2f")
        st.caption(f"当前值: {current_val/1e8:.2f} 亿")
        
        if st.button("保存并锁定"):
            try:
                conn = sqlite3.connect(DB_PATH)
                cursor = conn.cursor()
                # 更新数据并锁定
                cursor.execute(f'''
                    UPDATE financial_reports_raw 
                    SET {edit_field_key} = ?, is_locked = 1, data_quality = 'MANUAL'
                    WHERE stock_code = ? AND report_period = ?
                ''', (new_val, selected_stock, edit_period))
                conn.commit()
                conn.close()
                
                # 重新计算指标
                calculator.calculate_indicators(selected_stock)
                
                # 清除缓存
                if 'df_raw' in st.session_state:
                    del st.session_state.df_raw
                
                st.success(f"已更新 {edit_period} 的 {edit_fields[edit_field_key]}！")
                st.rerun()
            except Exception as e:
                st.error(f"更新失败: {e}")

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
            fetcher = get_fetcher(selected_stock)
            fetcher.fetch_financial_data(selected_stock)
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
        
        # 详细验证状态（可展开）
        with st.expander("📋 查看详细验证状态"):
            # 按报告期分组显示
            # 确保 validation_details 存在
            cols_to_use = ['report_period', 'report_type', 'data_quality']
            if 'validation_details' in df_raw.columns:
                cols_to_use.append('validation_details')
                
            quality_details = df_raw[cols_to_use].copy()
            quality_details['report_name'] = quality_details.apply(
                lambda row: f"{row['report_period'][:4]}{row['report_type']}", axis=1
            )
            
            # 显示已验证的
            if verified_count > 0:
                st.markdown("**✅ 已验证的报告：**")
                verified = quality_details[quality_details['data_quality'] == 'VERIFIED']
                st.write(", ".join(verified['report_name'].tolist()))
            
            # 显示有冲突的
            if conflict_count > 0:
                st.markdown("**❌ 数据冲突的报告：**")
                conflicts = quality_details[quality_details['data_quality'] == 'CONFLICT']
                
                for _, row in conflicts.iterrows():
                    st.markdown(f"**{row['report_name']}**")
                    
                    # 解析 validation_details JSON
                    if 'validation_details' in row and row['validation_details']:
                        try:
                            details = json.loads(row['validation_details'])
                            # 只显示冲突的字段
                            conflict_fields = {k: v for k, v in details.items() if v.get('status') == 'CONFLICT'}
                            
                            if conflict_fields:
                                for field, info in conflict_fields.items():
                                    field_name_map = {
                                        'revenue': ('营业收入', '利润表'),
                                        'net_income_parent': ('归母净利润', '利润表'),
                                        'total_assets': ('总资产', '资产负债表'),
                                        'total_equity': ('股东权益', '资产负债表')
                                    }
                                    field_info = field_name_map.get(field, (field, '未知表'))
                                    field_cn = field_info[0]
                                    table_name = field_info[1]
                                    
                                    st.warning(
                                        f"⚠️ **{field_cn}** ({table_name}): "
                                        f"AkShare={info['akshare']}亿, "
                                        f"PDF={info['pdf']}亿, "
                                        f"差异={info['diff_pct']}%"
                                    )
                            else:
                                st.caption("（详情缺失，请重新验证）")
                        except Exception as e:
                            st.caption(f"（解析详情失败: {e}）")
                    else:
                        st.caption("（无详细信息）")
            
            # 显示未验证的（只显示前10个，避免太长）
            if unverified_count > 0:
                st.markdown(f"**⚠️ 未验证的报告（共{unverified_count}个）：**")
                unverified = quality_details[quality_details['data_quality'] == 'UNVERIFIED']
                unverified_list = unverified['report_name'].tolist()
                if len(unverified_list) > 10:
                    st.write(", ".join(unverified_list[:10]) + f" ...等{len(unverified_list)}个")
                else:
                    st.write(", ".join(unverified_list))
                
                st.caption("💡 提示：未验证的数据缺少对应的PDF文件，需要先下载财报原文才能验证。")
        
        st.markdown("---")
    
    # --- 4. 辅助函数：转置表格 ---
    def transpose_df(df, index_col='report_name', exclude_cols=['id', 'stock_code', 'currency', 'publish_date', 'report_period', 'report_type', 'report_period_dt', 'data_quality', 'validation_details', 'is_locked']):
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
        'receivables_turnover_days': '应收账款周转天数 [天]',
        'fcf': '自由现金流 (FCF) [亿]',
        'cfo_to_net_income': '净现比 (CFO/NetIncome)',
        'dividend_payout_ratio': '分红率 (Payout Ratio) [%]',
        'dividend_per_share': '每股分红 (DPS) [元]',
        'dividend_total': '分红总额 [亿]',
        'eps_basic': '基本每股收益 (EPS) [元]',
        'eps_ttm': '滚动每股收益 (EPS-TTM) [元]',
        'bps': '每股净资产 (BPS) [元]',
        
        # 原始报表 - 利润表
        'revenue': '营业收入 [亿]',
        'cost_of_revenue': '营业成本 [亿]',
        'gross_profit': '毛利 [亿]',
        'selling_expenses': '销售费用 [亿]',
        'admin_expenses': '管理费用 [亿]',
        'rd_expenses': '研发费用 [亿]',
        'financial_expenses': '财务费用 [亿]',
        'income_tax_expenses': '所得税费用 [亿]', # 新增
        'investment_income': '投资收益 [亿]',
        'operating_income': '营业利润 [亿]',
        'total_profit': '利润总额 [亿]',
        'net_income': '净利润 [亿]',
        'net_income_parent': '归母净利润 [亿]',
        'net_income_deducted': '扣非净利润 [亿]',
        
        # 资产负债表
        'total_assets': '总资产 [亿]',
        'current_assets': '流动资产 [亿]',      # 新增
        'non_current_assets': '非流动资产 [亿]',  # 新增
        'total_liabilities': '总负债 [亿]',
        'current_liabilities': '流动负债 [亿]',   # 新增
        'non_current_liabilities': '非流动负债 [亿]', # 新增
        'total_equity': '股东权益 [亿]',
        'share_capital': '股本 [亿]',          # 新增
        'retained_earnings': '未分配利润 [亿]',   # 新增
        'cash_equivalents': '货币资金 [亿]',
        'accounts_receivable': '应收账款 [亿]',
        'inventory': '存货 [亿]',
        'fixed_assets': '固定资产 [亿]',
        'intangible_assets': '无形资产 [亿]',     # 新增
        'goodwill': '商誉 [亿]',
        'short_term_debt': '短期借款 [亿]',
        'long_term_debt': '长期借款 [亿]',
        'accounts_payable': '应付账款 [亿]',
        'contract_liabilities': '合同负债 [亿]',
        
        # 现金流量表
        'cfo_net': '经营现金流净额 [亿]',
        'cfi_net': '投资现金流净额 [亿]',
        'cff_net': '筹资现金流净额 [亿]',
        'net_cash_flow': '现金净增加额 [亿]',    # 新增
        'capex': '资本开支 [亿]',
        'cash_paid_for_dividends': '分红支付现金 [亿]'
    }

    # --- 6. 高亮样式函数 ---
    def highlight_conflicts(df_display, df_source):
        """
        df_display: 转置后的用于显示的 DataFrame (行是字段，列是报告期)
        df_source: 原始的 DataFrame (行是报告期，包含 validation_details)
        """
        # 创建一个空的样式 DataFrame，默认无样式
        df_style = pd.DataFrame('', index=df_display.index, columns=df_display.columns)
        
        # 遍历每一列（即每一个报告期）
        for col_name in df_display.columns:
            # 找到对应的源数据行
            # col_name 可能是 "2023A" 或 "2023A ❌"
            # 我们需要通过 report_name 找到对应的行
            source_row = df_source[df_source['report_name'] == col_name]
            
            if not source_row.empty:
                details_json = source_row.iloc[0].get('validation_details')
                if details_json:
                    try:
                        details = json.loads(details_json)
                        # 找出有冲突的字段
                        conflict_fields = [k for k, v in details.items() if v.get('status') == 'CONFLICT']
                        
                        # 遍历显示表格的每一行（即每一个字段）
                        for idx in df_display.index:
                            # idx 是中文显示名，如 "营业收入 [亿]"
                            # 我们需要反向映射回英文字段名，或者在 field_map 里找
                            # 简单起见，我们检查 field_map 的 value 是否包含 idx
                            
                            original_field = None
                            for k, v in field_map.items():
                                if v == idx:
                                    original_field = k
                                    break
                            
                            if original_field and original_field in conflict_fields:
                                # 标记冲突：背景淡红，文字红色加粗
                                df_style.loc[idx, col_name] = 'background-color: #ffe6e6; color: #d9534f; font-weight: bold;'
                                
                    except:
                        pass
        return df_style

    # --- 7. 数据展示 ---
    
    # 7.1 核心指标
    st.subheader("📈 核心财务指标")
    df_metrics = transpose_df(df_derived)
    # 映射行名
    df_metrics.index = df_metrics.index.map(lambda x: field_map.get(x, x))
    # 格式化 (处理空值)
    st.dataframe(df_metrics.style.format("{:.2f}", na_rep="-"), height=400)

    # 7.2 原始财务报表 (全量数据)
    st.subheader("📄 原始财务报表 (Raw Data)")
    
    # 定义映射字典 (用于在 UI 上标注核心变量)
    # 注意：这里只是为了显示，实际逻辑在 Fetcher 里
    # 我们做一个简单的反向查找： 原始中文 -> 内部英文
    hk_mapping_display = {}
    # 搬运自 hk_share.py 的映射逻辑
    raw_map = {
        'revenue': ['营业额', '营业收入', '营业总收入', '收入'],
        'gross_profit': ['毛利'],
        'net_income_parent': ['本公司拥有人应占溢利', '归属于母公司股东的净利润', '归母净利润'],
        'net_income': ['年度溢利', '净利润'],
        'eps_basic': ['基本每股盈利', '基本每股收益'],
        'rd_expenses': ['研究及开发成本', '研发费用'],
        'total_assets': ['资产总值', '资产合计', '总资产'],
        'total_liabilities': ['负债总额', '负债合计', '总负债'],
        'total_equity': ['本公司拥有人应占权益', '权益合计', '股东权益合计'],
        'cash_equivalents': ['现金及现金等价物', '货币资金'],
        'cfo_net': ['经营业务现金净额', '经营活动产生的现金流量净额'],
        'capex': ['购建固定资产', '购买物业、厂房及设备']
    }
    for internal_key, raw_list in raw_map.items():
        for raw_name in raw_list:
            hk_mapping_display[raw_name] = internal_key

    if 'raw_data' in df_raw.columns and not df_raw['raw_data'].isna().all():
        # 解析所有行的 JSON
        all_rows = []
        for idx, row in df_raw.iterrows():
            if row['raw_data']:
                try:
                    row_dict = json.loads(row['raw_data'])
                    # 加上报告期作为第一列
                    row_dict['report_period'] = row['report_period']
                    all_rows.append(row_dict)
                except:
                    pass
        
        if all_rows:
            df_full = pd.DataFrame(all_rows)
            # 把 report_period 设为索引
            if 'report_period' in df_full.columns:
                df_full.set_index('report_period', inplace=True)
            
            # --- 控制选项 ---
            col1, col2 = st.columns(2)
            with col1:
                unit_opt = st.radio("单位", ["原始值 (元)", "亿"], horizontal=True, key="full_data_unit")
            with col2:
                transpose_opt = st.checkbox("转置表格 (时间横轴)", value=True, key="full_data_transpose")
            
            # --- 数据处理 ---
            # 1. 单位转换
            if unit_opt == "亿":
                for col in df_full.columns:
                    df_full[col] = pd.to_numeric(df_full[col], errors='ignore')
                    if pd.api.types.is_numeric_dtype(df_full[col]):
                        if df_full[col].abs().median() > 10000:
                            df_full[col] = df_full[col] / 1e8
            
            # 2. 转置
            if transpose_opt:
                df_display = df_full.T
                # 在转置后的索引(字段名)上添加标注
                new_index = []
                for idx in df_display.index:
                    internal_name = hk_mapping_display.get(idx)
                    if internal_name:
                        new_index.append(f"{idx} ({internal_name})")
                    else:
                        new_index.append(idx)
                df_display.index = new_index
            else:
                df_display = df_full
                # 如果不转置，列名添加标注
                new_cols = []
                for col in df_display.columns:
                    internal_name = hk_mapping_display.get(col)
                    if internal_name:
                        new_cols.append(f"{col} ({internal_name})")
                    else:
                        new_cols.append(col)
                df_display.columns = new_cols

            # 展示
            st.dataframe(df_display, height=600)
            st.caption(f"共包含 {len(df_full.columns)} 个原始字段。括号内为系统识别的核心变量名。")
    else:
        st.info("暂无原始数据，请点击侧边栏'强制更新数据'。")

else:
    st.warning("未找到数据。")
