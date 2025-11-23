import re
import sqlite3
import json
from pathlib import Path
from datetime import datetime

DB_PATH = Path(__file__).parent / "finance.db"

# 尝试导入 Gemini
try:
    import google.generativeai as genai
    HAS_GEMINI = True
except ImportError:
    HAS_GEMINI = False
    print("⚠️ 未安装 google-generativeai，请运行: pip install google-generativeai")

class FinancialDataValidator:
    """财务数据交叉验证器 (LLM 增强版)"""
    
    TOLERANCE = 0.02  # 允许 2% 的误差
    
    # 关键字段映射
    CRITICAL_FIELDS = {
        'revenue': (['营业收入', '营业总收入', '一、营业总收入'], 1e8),
        'net_income_parent': (['归属于母公司.*净利润', '归母净利润', '归属于上市公司股东的净利润'], 1e8),
        'total_assets': (['资产总计', '总资产', '资产合计'], 1e8),
        'total_equity': (['股东权益合计', '所有者权益合计', '归属于母公司股东权益合计'], 1e8),
    }
    
    def __init__(self, use_llm=True, gemini_api_key=None):
        self.conn = sqlite3.connect(DB_PATH)
        self.use_llm = use_llm and HAS_GEMINI
        
        if self.use_llm:
            # 配置 Gemini
            if gemini_api_key:
                genai.configure(api_key=gemini_api_key)
            # 使用 Flash 模型（便宜快速）
            self.model = genai.GenerativeModel('gemini-2.5-flash')
    
    def validate_report(self, stock_code, report_period):
        """
        验证单个财报的数据质量
        返回: {'status': 'VERIFIED'/'CONFLICT', 'details': {...}}
        """
        # 1. 获取 AkShare 数据
        akshare_data = self._get_akshare_data(stock_code, report_period)
        if not akshare_data:
            return {'status': 'NO_DATA', 'message': 'AkShare 数据不存在'}
        
        # 2. 获取 TXT 文件路径
        txt_path = self._get_txt_path(stock_code, report_period)
        if not txt_path or not Path(txt_path).exists():
            return {'status': 'NO_FILE', 'message': 'PDF/TXT 文件不存在'}
        
        # 3. 从 TXT 提取数据（优先使用 LLM）
        if self.use_llm:
            print("  🤖 使用 Gemini 提取财务数据...")
            pdf_data = self._extract_with_llm(txt_path, akshare_data)
        else:
            print("  📝 使用正则表达式提取财务数据...")
            pdf_data = self._extract_with_regex(txt_path)
        
        # 4. 逐字段验证
        results = {}
        has_conflict = False
        
        for field in ['revenue', 'net_income_parent', 'total_assets', 'total_equity']:
            ak_value = akshare_data.get(field)
            pdf_value = pdf_data.get(field)
            
            if ak_value is None:
                results[field] = {'status': 'MISSING_AKSHARE'}
                continue
            
            if pdf_value is None:
                results[field] = {'status': 'MISSING_PDF'}
                continue
            
            # 计算差异
            diff_ratio = abs(ak_value - pdf_value) / max(abs(ak_value), abs(pdf_value))
            
            if diff_ratio < self.TOLERANCE:
                results[field] = {
                    'status': 'PASS',
                    'akshare': round(ak_value / 1e8, 2),
                    'pdf': round(pdf_value / 1e8, 2),
                    'diff_pct': round(diff_ratio * 100, 2)
                }
            else:
                has_conflict = True
                results[field] = {
                    'status': 'CONFLICT',
                    'akshare': round(ak_value / 1e8, 2),
                    'pdf': round(pdf_value / 1e8, 2),
                    'diff_pct': round(diff_ratio * 100, 2)
                }
        
        # 5. 更新数据库质量标记和详情
        quality_status = 'CONFLICT' if has_conflict else 'VERIFIED'
        self._update_quality_flag(stock_code, report_period, quality_status, results)
        
        return {
            'status': quality_status,
            'details': results,
            'timestamp': datetime.now().isoformat()
        }
    
    def _extract_with_llm(self, txt_path, akshare_data):
        """使用 Gemini LLM 提取财务数据"""
        try:
            # 读取文件（只取前 100k 字符，避免超出 token 限制）
            with open(txt_path, 'r', encoding='utf-8') as f:
                text = f.read()[:100000]
            
            # 构造 Prompt
            prompt = f"""
你是一个专业的财务分析师。请从以下财务报告中提取关键数字。

参考值（来自 AkShare，用于对比）：
- 营业收入: {akshare_data.get('revenue', 0) / 1e8:.2f} 亿元
- 归母净利润: {akshare_data.get('net_income_parent', 0) / 1e8:.2f} 亿元
- 总资产: {akshare_data.get('total_assets', 0) / 1e8:.2f} 亿元
- 股东权益: {akshare_data.get('total_equity', 0) / 1e8:.2f} 亿元
- 所得税费用: {akshare_data.get('income_tax_expenses', 0) / 1e8:.2f} 亿元
- 流动资产: {akshare_data.get('current_assets', 0) / 1e8:.2f} 亿元
- 非流动资产: {akshare_data.get('non_current_assets', 0) / 1e8:.2f} 亿元
- 无形资产: {akshare_data.get('intangible_assets', 0) / 1e8:.2f} 亿元
- 流动负债: {akshare_data.get('current_liabilities', 0) / 1e8:.2f} 亿元
- 非流动负债: {akshare_data.get('non_current_liabilities', 0) / 1e8:.2f} 亿元
- 股本: {akshare_data.get('share_capital', 0) / 1e8:.2f} 亿元
- 未分配利润: {akshare_data.get('retained_earnings', 0) / 1e8:.2f} 亿元
- 现金流量净额: {akshare_data.get('net_cash_flow', 0) / 1e8:.2f} 亿元

请从财报原文中提取这些数字（合并报表），返回 JSON 格式：
{{
    "revenue": <营业收入，单位：元>,
    "net_income_parent": <归母净利润，单位：元>,
    "total_assets": <总资产，单位：元>,
    "total_equity": <股东权益合计，单位：元>,
    "income_tax_expenses": <所得税费用，单位：元>,
    "current_assets": <流动资产合计，单位：元>,
    "non_current_assets": <非流动资产合计，单位：元>,
    "intangible_assets": <无形资产，单位：元>,
    "current_liabilities": <流动负债合计，单位：元>,
    "non_current_liabilities": <非流动负债合计，单位：元>,
    "share_capital": <实收资本(或股本)，单位：元>,
    "retained_earnings": <未分配利润，单位：元>,
    "net_cash_flow": <现金及现金等价物净增加额，单位：元>
}}

财报原文（节选）：
{text}

只返回 JSON，不要其他解释。如果某个字段找不到，返回 null。
"""
            
            response = self.model.generate_content(prompt)
            result_text = response.text.strip()
            
            # 提取 JSON（去掉可能的 markdown 标记）
            if '```json' in result_text:
                result_text = result_text.split('```json')[1].split('```')[0]
            elif '```' in result_text:
                result_text = result_text.split('```')[1].split('```')[0]
            
            extracted = json.loads(result_text)
            
            # 转换 None 为实际的 None
            return {k: (v if v is not None else None) for k, v in extracted.items()}
            
        except Exception as e:
            print(f"  ⚠️ LLM 提取失败: {e}")
            # 降级到正则表达式
            return self._extract_with_regex(txt_path)
    
    def _extract_with_regex(self, txt_path):
        """使用正则表达式提取财务数据（备用方案）"""
        try:
            with open(txt_path, 'r', encoding='utf-8') as f:
                text = f.read()
        except Exception as e:
            print(f"读取文件失败: {e}")
            return {}
        
        extracted = {}
        
        for field, (keywords, unit) in self.CRITICAL_FIELDS.items():
            for keyword in keywords:
                pattern = rf'{keyword}\s*\n?\s*([\d,]+\.?\d*)'
                matches = re.findall(pattern, text)
                
                if matches:
                    value_str = matches[0].replace(',', '')
                    try:
                        value = float(value_str)
                        
                        if value > 1e9:
                            extracted[field] = value
                        elif value > 1e5:
                            extracted[field] = value * 1e4
                        else:
                            extracted[field] = value * 1e8
                        
                        break
                    except ValueError:
                        continue
        
        return extracted
    
    def _get_akshare_data(self, stock_code, report_period):
        """从数据库读取 AkShare 数据"""
        cursor = self.conn.cursor()
        cursor.execute('''
            SELECT revenue, net_income_parent, total_assets, total_equity,
                   income_tax_expenses, current_assets, non_current_assets, intangible_assets,
                   current_liabilities, non_current_liabilities, share_capital, retained_earnings, net_cash_flow
            FROM financial_reports_raw
            WHERE stock_code = ? AND report_period = ?
        ''', (stock_code, report_period))
        
        row = cursor.fetchone()
        if not row:
            return None
        
        return {
            'revenue': row[0],
            'net_income_parent': row[1],
            'total_assets': row[2],
            'total_equity': row[3],
            'income_tax_expenses': row[4],
            'current_assets': row[5],
            'non_current_assets': row[6],
            'intangible_assets': row[7],
            'current_liabilities': row[8],
            'non_current_liabilities': row[9],
            'share_capital': row[10],
            'retained_earnings': row[11],
            'net_cash_flow': row[12]
        }
    
    def _get_txt_path(self, stock_code, report_period):
        """从数据库获取 TXT 文件路径"""
        cursor = self.conn.cursor()
        cursor.execute('''
            SELECT txt_path FROM financial_reports_files
            WHERE stock_code = ? AND report_period = ?
        ''', (stock_code, report_period))
        
        row = cursor.fetchone()
        if row:
            txt_path = Path(__file__).parent / row[0]
            return str(txt_path)
        return None
    
    def _autofill_data(self, stock_code, report_period, data_dict):
        """回填缺失数据到数据库"""
        if not data_dict:
            return
            
        cursor = self.conn.cursor()
        
        # 构建 UPDATE 语句
        set_clauses = [f"{k} = ?" for k in data_dict.keys()]
        values = list(data_dict.values())
        values.extend([stock_code, report_period])
        
        sql = f'''
            UPDATE financial_reports_raw
            SET {', '.join(set_clauses)}
            WHERE stock_code = ? AND report_period = ?
        '''
        
        try:
            cursor.execute(sql, values)
            self.conn.commit()
            print(f"  ✅ 已自动回填 {len(data_dict)} 个字段")
        except Exception as e:
            print(f"  ⚠️ 回填失败: {e}")

    def _update_quality_flag(self, stock_code, report_period, status, details=None):
        """更新数据库中的质量标记和详情"""
        cursor = self.conn.cursor()
        
        # 将详情转换为 JSON 字符串
        details_json = json.dumps(details, ensure_ascii=False) if details else None
        
        cursor.execute('''
            UPDATE financial_reports_raw
            SET data_quality = ?, validation_details = ?
            WHERE stock_code = ? AND report_period = ?
        ''', (status, details_json, stock_code, report_period))
        self.conn.commit()
    
    def close(self):
        self.conn.close()

if __name__ == "__main__":
    # 测试验证器
    import os
    
    # 从环境变量读取 API Key
    api_key = os.getenv('GEMINI_API_KEY')
    
    if not api_key:
        print("⚠️ 请设置环境变量 GEMINI_API_KEY")
        print("export GEMINI_API_KEY='your_api_key'")
    
    validator = FinancialDataValidator(use_llm=True, gemini_api_key=api_key)
    result = validator.validate_report("600519", "2024-12-31")
    print("\n验证结果:")
    print(f"状态: {result['status']}")
    if 'details' in result:
        for field, detail in result['details'].items():
            if detail.get('status') == 'PASS':
                print(f"✅ {field}: AkShare={detail['akshare']}亿, PDF={detail['pdf']}亿")
            elif detail.get('status') == 'CONFLICT':
                print(f"❌ {field}: 差异={detail['diff_pct']}%")
            else:
                print(f"⚠️ {field}: {detail['status']}")
    validator.close()
