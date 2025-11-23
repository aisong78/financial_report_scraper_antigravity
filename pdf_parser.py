import fitz  # PyMuPDF
import os
from pathlib import Path

class PDFParser:
    def __init__(self):
        pass

    def parse_pdf(self, pdf_path):
        """
        将 PDF 转换为 TXT
        """
        pdf_path = Path(pdf_path)
        if not pdf_path.exists():
            print(f"❌ 文件不存在: {pdf_path}")
            return None
            
        txt_path = pdf_path.with_suffix('.txt')
        
        # 如果 TXT 已经存在且比 PDF 新，跳过
        if txt_path.exists() and txt_path.stat().st_mtime > pdf_path.stat().st_mtime:
            print(f"  跳过已解析: {txt_path.name}")
            return txt_path

        print(f"📄 正在解析: {pdf_path.name} ...")
        
        try:
            text_content = []
            with fitz.open(pdf_path) as doc:
                for page in doc:
                    text_content.append(page.get_text())
            
            full_text = "\n".join(text_content)
            
            with open(txt_path, 'w', encoding='utf-8') as f:
                f.write(full_text)
                
            print(f"✅ 解析完成，已保存为 TXT")
            return txt_path
            
        except Exception as e:
            print(f"❌ 解析失败: {e}")
            return None

    def parse_directory(self, dir_path):
        """
        批量解析目录下的所有 PDF
        """
        dir_path = Path(dir_path)
        pdfs = list(dir_path.glob("*.pdf"))
        print(f"📂 在 {dir_path} 发现 {len(pdfs)} 个 PDF 文件")
        
        for pdf in pdfs:
            self.parse_pdf(pdf)

if __name__ == "__main__":
    parser = PDFParser()
    # 测试解析容百科技的下载目录
    target_dir = Path(__file__).parent / "downloads" / "688005"
    if target_dir.exists():
        parser.parse_directory(target_dir)
    else:
        print("请先运行 pdf_downloader.py 下载文件")
