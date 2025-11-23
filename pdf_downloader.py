import requests
import os
import time
import random
import sqlite3
from pathlib import Path
from datetime import datetime, timedelta

from pdf_parser import PDFParser

# 尝试导入美股下载库 (如果没安装则跳过)
try:
    from sec_edgar_downloader import Downloader as SecDownloader
    HAS_SEC = True
except ImportError:
    HAS_SEC = False

DB_PATH = Path(__file__).parent / "finance.db"

class PDFDownloader:
    def __init__(self, download_dir="downloads"):
        self.base_dir = Path(__file__).parent / download_dir
        self.base_dir.mkdir(exist_ok=True)
        self.parser = PDFParser()
        self.conn = sqlite3.connect(DB_PATH)  # 数据库连接
        
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            "Origin": "http://www.cninfo.com.cn",
            "Referer": "http://www.cninfo.com.cn/new/commonUrl/pageOfSearch?url=disclosure/list/search&lastPage=index"
        }
    
    def _record_file(self, stock_code, report_period, report_type, file_path, txt_path):
        """将文件信息记录到数据库"""
        try:
            file_size = file_path.stat().st_size if file_path.exists() else 0
            relative_path = str(file_path.relative_to(Path(__file__).parent))
            relative_txt = str(txt_path.relative_to(Path(__file__).parent)) if txt_path else None
            
            cursor = self.conn.cursor()
            cursor.execute('''
                INSERT OR REPLACE INTO financial_reports_files
                (stock_code, report_period, report_type, file_type, file_path, txt_path, download_date, file_size, parse_status)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                stock_code,
                report_period,
                report_type,
                'PDF',
                relative_path,
                relative_txt,
                datetime.now().isoformat(),
                file_size,
                'SUCCESS' if txt_path and txt_path.exists() else 'PENDING'
            ))
            self.conn.commit()
        except Exception as e:
            print(f"  ⚠️ 记录文件信息失败: {e}")
    
    def __del__(self):
        """析构时关闭数据库连接"""
        if hasattr(self, 'conn'):
            self.conn.close()


    def _download_cninfo(self, stock_code, stock_type, save_dir, lookback_days):
        # ... (前面的代码不变) ...
        
        # 3. 分页下载
        while True:
            try:
                # ... (请求代码不变) ...
                
                for ann in announcements:
                    # ... (文件名处理不变) ...
                    
                    # 下载
                    pdf_url = "http://static.cninfo.com.cn/" + ann['adjunctUrl']
                    
                    # 如果文件不存在才下载
                    if not file_path.exists():
                        print(f"  ⬇️ 下载: {title}")
                        r = requests.get(pdf_url, stream=True)
                        with open(file_path, 'wb') as f:
                            for chunk in r.iter_content(8192):
                                f.write(chunk)
                        time.sleep(0.5)
                    else:
                        print(f"  跳过: {title}")

                    # --- 集成解析逻辑 ---
                    # 无论是否新下载，都检查一下有没有 TXT，没有就解析
                    self.parser.parse_pdf(file_path)
                
                if not data.get('hasMore'):
                    break
                params['pageNum'] += 1
                
            except Exception as e:
                print(f"下载出错: {e}")
                break
        
        print(f"✅ {stock_code} 下载与解析完成！")

    def _download_sec(self, ticker, save_dir, lookback_days):
        # ... (美股下载逻辑不变，美股本身就是 HTML，暂时不需要 PDF 解析) ...
        # 但如果未来需要把 HTML 转 TXT，也可以在这里加逻辑
        pass


    def _get_stock_type(self, code):
        if code.isdigit():
            if len(code) == 6: return 'A'
            elif len(code) == 5: return 'HK'
        elif code.isalpha():
            return 'US'
        return 'UNKNOWN'

    def _get_cninfo_org_id(self, stock_code):
        """获取巨潮资讯 orgId"""
        url = "http://www.cninfo.com.cn/new/information/topSearch/query"
        try:
            res = requests.post(url, data={"keyWord": stock_code}, headers=self.headers)
            if res.status_code == 200:
                data = res.json()
                for item in data:
                    if item['code'] == stock_code:
                        return item['orgId']
        except Exception as e:
            print(f"获取 orgId 失败: {e}")
        return None

    def download(self, stock_code, lookback_days=365*3):
        """
        通用下载入口
        """
        stock_type = self._get_stock_type(stock_code)
        save_dir = self.base_dir / stock_code
        save_dir.mkdir(exist_ok=True)
        
        print(f"📥 开始下载 {stock_code} ({stock_type}) 的财报...")
        
        if stock_type in ['A', 'HK']:
            self._download_cninfo(stock_code, stock_type, save_dir, lookback_days)
        elif stock_type == 'US':
            if HAS_SEC:
                self._download_sec(stock_code, save_dir, lookback_days)
            else:
                print("❌ 未安装 sec-edgar-downloader，无法下载美股财报。请运行: pip install sec-edgar-downloader")
        else:
            print(f"❌ 未知股票类型: {stock_code}")

    def _download_cninfo(self, stock_code, stock_type, save_dir, lookback_days):
        url = "http://www.cninfo.com.cn/new/hisAnnouncement/query"
        
        # 1. 获取 orgId
        org_id = self._get_cninfo_org_id(stock_code)
        stock_param = f"{stock_code},{org_id}" if org_id else stock_code
        
        # 2. 构造参数
        end_date = datetime.now()
        start_date = end_date - timedelta(days=lookback_days)
        
        params = {
            "pageNum": 1,
            "pageSize": 30,
            "tabName": "fulltext",
            "stock": stock_param,
            "seDate": f"{start_date.strftime('%Y-%m-%d')}~{end_date.strftime('%Y-%m-%d')}",
            "isHLtitle": "true"
        }
        
        if stock_type == 'A':
            params['column'] = 'sse' if stock_code.startswith('6') else 'szse'
            params['category'] = "category_ndbg_szsh;category_bndbg_szsh;category_yjdbg_szsh;category_sjdbg_szsh"
        else:
            params['column'] = 'hke'
            params['category'] = "category_ndbg_hkhk;category_bndbg_hkhk"

        # 3. 分页下载
        while True:
            try:
                res = requests.post(url, data=params, headers=self.headers)
                data = res.json()
                announcements = data.get('announcements')
                
                if not announcements:
                    break
                    
                for ann in announcements:
                    title = ann['announcementTitle'].replace("<em>", "").replace("</em>", "")
                    
                    # 过滤摘要
                    if "摘要" in title or "取消" in title: 
                        continue
                    
                    # 构造文件名
                    file_name = f"{title}.pdf"
                    file_path = save_dir / file_name.replace("/", "_")
                    
                    # 下载 PDF
                    if not file_path.exists():
                        pdf_url = "http://static.cninfo.com.cn/" + ann['adjunctUrl']
                        print(f"  ⬇️ 下载: {title}")
                        
                        r = requests.get(pdf_url, stream=True)
                        with open(file_path, 'wb') as f:
                            for chunk in r.iter_content(8192):
                                f.write(chunk)
                        time.sleep(0.5)
                    else:
                        print(f"  跳过: {title}")
                    
                    # 解析 PDF 为 TXT
                    txt_path = self.parser.parse_pdf(file_path)
                    
                    # 从标题中提取 report_period (简单处理)
                    # 标题格式: "2023年年度报告" -> report_period: "2023-12-31"
                    report_period = self._extract_period_from_title(title)
                    report_type = self._extract_type_from_title(title)
                    
                    # 记录文件信息到数据库
                    if report_period and report_type:
                        self._record_file(stock_code, report_period, report_type, file_path, txt_path)
                
                if not data.get('hasMore'):
                    break
                params['pageNum'] += 1
                
            except Exception as e:
                print(f"下载出错: {e}")
                break
        
        print(f"✅ {stock_code} 下载与解析完成！")
    
    def _extract_period_from_title(self, title):
        """从标题提取报告期"""
        import re
        # 匹配年份
        year_match = re.search(r'(\d{4})年', title)
        if not year_match:
            return None
        year = year_match.group(1)
        
        # 判断报告类型（优先判断季度/半年，避免被"年度"误判）
        if '第三季度' in title or '三季报' in title:
            return f"{year}-09-30"
        elif '第一季度' in title or '一季报' in title:
            return f"{year}-03-31"
        elif '半年' in title or '中报' in title:
            return f"{year}-06-30"
        elif '年度报告' in title or '年报' in title:
            return f"{year}-12-31"
        return None
    
    def _extract_type_from_title(self, title):
        """从标题提取报告类型"""
        if '第三季度' in title or '三季报' in title:
            return 'Q3'
        elif '第一季度' in title or '一季报' in title:
            return 'Q1'
        elif '半年' in title or '中报' in title:
            return 'S1'
        elif '年度报告' in title or '年报' in title:
            return 'A'
        return 'A'  # 默认


    def _download_sec(self, ticker, save_dir, lookback_days):
        # 需要配置 email
        email = "your_email@example.com" 
        dl = SecDownloader("Antigravity", email, str(save_dir))
        
        after_date = (datetime.now() - timedelta(days=lookback_days)).strftime('%Y-%m-%d')
        
        print(f"  正在从 SEC 下载 10-K/10-Q (after {after_date})...")
        try:
            dl.get("10-K", ticker, after=after_date)
            dl.get("10-Q", ticker, after=after_date)
            print(f"✅ {ticker} 美股财报下载完成 (HTML格式)")
        except Exception as e:
            print(f"SEC 下载失败: {e}")

if __name__ == "__main__":
    d = PDFDownloader()
    # 测试 A股 (测试 1 年数据，验证数据库记录功能)
    d.download("688005", lookback_days=365)
    # 测试 港股 (腾讯)
    # d.download("00700", lookback_days=365)
