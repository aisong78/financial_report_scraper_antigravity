import re
import sqlite3
from pathlib import Path
from datetime import datetime

DB_PATH = Path(__file__).parent / "finance.db"

class FinancialDataValidator:
    """财务数据交叉验证器"""
    
    TOLERANCE = 0.02  # 允许 2% 的误差
    
    # 关键字段映射：AkShare字段名 -> (PDF关键词列表, 单位转换系数)
    CRITICAL_FIELDS = {
        'revenue': (['营业收入', '营业总收入', '一、营业总收入'], 1e8),
        'net_income_parent': (['归属于母公司.*净利润', '归母净利润', '归属于上市公司股东的净利润'], 1e8),
        'total_assets': (['资产总计', '总资产', '资产合计'], 1e8),
        'total_equity': (['股东权益合计', '所有者权益合计', '归属于母公司股东权益合计'], 1e8),
    }
    
    def __init__(self):
        self.conn = sqlite3.connect(DB_PATH)
    
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
        
        # 3. 从 TXT 提取数据
        pdf_data = self._extract_from_txt(txt_path)
        
        # 4. 逐字段验证
        results = {}
        has_conflict = False
        
        for field, (keywords, unit) in self.CRITICAL_FIELDS.items():
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
                    'akshare': round(ak_value / unit, 2),
                    'pdf': round(pdf_value / unit, 2),
                    'diff_pct': round(diff_ratio * 100, 2)
                }
            else:
                has_conflict = True
                results[field] = {
                    'status': 'CONFLICT',
                    'akshare': round(ak_value / unit, 2),
                    'pdf': round(pdf_value / unit, 2),
                    'diff_pct': round(diff_ratio * 100, 2)
                }
        
        # 5. 更新数据库质量标记
        quality_status = 'CONFLICT' if has_conflict else 'VERIFIED'
        self._update_quality_flag(stock_code, report_period, quality_status)
        
        return {
            'status': quality_status,
            'details': results,
            'timestamp': datetime.now().isoformat()
        }
    
    def _get_akshare_data(self, stock_code, report_period):
        """从数据库读取 AkShare 数据"""
        cursor = self.conn.cursor()
        cursor.execute('''
            SELECT revenue, net_income_parent, total_assets, total_equity
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
            'total_equity': row[3]
        }
    
    def _get_txt_path(self, stock_code, report_period):
        """从数据库获取 TXT 文件路径"""
        cursor = self.conn.cursor()
        cursor.execute('''
            SELECT txt_path FROM financial_reports_files
            WHERE stock_code = ? AND report_period = ?
        ''', (stock_code, report_period))
        
        row = cursor.fetchone()
        return row[0] if row else None
    
    def _extract_from_txt(self, txt_path):
        """从 TXT 文件中提取关键财务数字"""
        try:
            with open(txt_path, 'r', encoding='utf-8') as f:
                text = f.read()
        except Exception as e:
            print(f"读取文件失败: {e}")
            return {}
        
        extracted = {}
        
        for field, (keywords, unit) in self.CRITICAL_FIELDS.items():
            for keyword in keywords:
                # 正则模式：关键词后面跟着数字（可能有逗号、小数点）
                # 示例：营业收入 15,088,123,456.78
                pattern = rf'{keyword}[^\d]{{0,20}}?([\d,]+\.?\d*)'
                matches = re.findall(pattern, text)
                
                if matches:
                    # 取第一个匹配（通常是主表数据）
                    value_str = matches[0].replace(',', '')
                    try:
                        value = float(value_str)
                        # 判断单位：如果数字很小（<1000），可能已经是亿为单位
                        # 如果很大（>1000000），可能是元为单位
                        if value > 1000000:
                            value = value  # 元为单位，不转换
                        elif value < 10000:
                            value = value * 1e8  # 亿为单位，转换为元
                        
                        extracted[field] = value
                        break  # 找到就跳出
                    except ValueError:
                        continue
        
        return extracted
    
    def _update_quality_flag(self, stock_code, report_period, status):
        """更新数据库中的质量标记"""
        cursor = self.conn.cursor()
        cursor.execute('''
            UPDATE financial_reports_raw
            SET data_quality = ?
            WHERE stock_code = ? AND report_period = ?
        ''', (status, stock_code, report_period))
        self.conn.commit()
    
    def close(self):
        self.conn.close()

if __name__ == "__main__":
    # 测试验证器
    validator = FinancialDataValidator()
    result = validator.validate_report("688005", "2023-12-31")
    print("验证结果:")
    print(f"状态: {result['status']}")
    print(f"详情: {result['details']}")
    validator.close()
