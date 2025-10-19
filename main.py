from fastapi import FastAPI, File, UploadFile, Form
from fastapi.responses import JSONResponse
import pdfplumber
import tempfile
import camelot
import PyPDF2
import shutil
import os
import base64

def parse_pages_param(pages_str, total_pages):
    if pages_str:
        pages_clean = pages_str.strip().lower()
        if pages_clean == "all":
            return list(range(1, total_pages + 1))
        elif "-" in pages_clean:
            start, end = map(int, pages_clean.split("-"))
            return list(range(start, min(end, total_pages) + 1))
        elif pages_clean.isdigit():
            end = int(pages_clean)
            return list(range(1, min(end, total_pages) + 1))
        else:
            return [
                int(p.strip())
                for p in pages_clean.split(",")
                if p.strip().isdigit() and 1 <= int(p.strip()) <= total_pages
            ]
    else:
        return list(range(1, min(6, total_pages) + 1))

app = FastAPI(title="PDF Extractor API", version="1.2")

@app.post("/extract")
async def extract_text(
    file: UploadFile = File(...),
    pages: str = Form(None)  # pages можно не передавать
):
    """
    Извлекает текст из загруженного PDF-файла с помощью pdfplumber.

    Параметры:
    - file: PDF-файл для обработки.
    - pages (необязательно): страницы для обработки.
      Можно указать диапазон ("1-5"), список ("1,3,7") или "all".
      Если не указан, обрабатываются первые 6 страниц.

    Возвращает JSON с извлечённым текстом и информацией о файле.
    """
    try:
        # Временное сохранение файла
        with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp:
            tmp.write(await file.read())
            tmp_path = tmp.name

        result_text = []
        total_pages = 0

        with pdfplumber.open(tmp_path) as pdf:
            total_pages = len(pdf.pages)
            page_numbers = parse_pages_param(pages, total_pages)
            for i, page in enumerate(pdf.pages, start=1):
                if i in page_numbers:
                    text = page.extract_text() or ""
                    result_text.append(f"\n=== Страница {i} ===\n{text}")

        os.unlink(tmp_path)

        return JSONResponse({
            "status": "ok",
            "file_name": file.filename,
            "total_pages": total_pages,
            "pages_processed": len(result_text),
            "text_length": sum(len(t) for t in result_text),
            "text": "\n".join(result_text),
            "engine_used": "pdfplumber"
        })

    except Exception as e:
        return JSONResponse(
            {"status": "error", "message": str(e)},
            status_code=500
        )

@app.post("/convert-to-excel")
async def convert_to_excel(file: UploadFile = File(...)):
    """
    Конвертирует PDF в Excel (.xlsx) с помощью Camelot.
    Возвращает JSON с Excel в base64 (удобно для n8n).
    """
    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp_pdf:
            tmp_pdf.write(await file.read())
            pdf_path = tmp_pdf.name

        # === Извлекаем метаданные (до первой строки с заголовками) ===
        metadata_lines = []
        header_keywords = ["КНП", "Дебет", "Кредит", "Назначение", "БИК", "Номер документа"]
        with pdfplumber.open(pdf_path) as pdf:
            if len(pdf.pages) > 0:
                lines = (pdf.pages[0].extract_text() or "").splitlines()
                for line in lines:
                    if any(key in line for key in header_keywords):
                        break
                    metadata_lines.append(line)

        # === Camelot extraction ===
        tables = camelot.read_pdf(pdf_path, flavor="lattice", pages="all")

        if not tables or len(tables) == 0:
            os.unlink(pdf_path)
            return JSONResponse({"status": "error", "message": "No tables found."}, status_code=400)

        import pandas as pd
        all_dfs = []
        for idx, table in enumerate(tables):
            df = table.df
            if idx == 0:
                all_dfs.append(df)
            else:
                # Пропускаем строки заголовков для последующих таблиц
                all_dfs.append(df.iloc[1:].reset_index(drop=True))
        combined_df = pd.concat(all_dfs, ignore_index=True)

        with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as tmp_xlsx:
            excel_path = tmp_xlsx.name

        with pd.ExcelWriter(excel_path, engine="openpyxl") as writer:
            if metadata_lines:
                meta_df = pd.DataFrame({0: metadata_lines})
                meta_df.to_excel(writer, sheet_name="Extracted", index=False, header=False)
                start_row = len(metadata_lines) + 1
            else:
                start_row = 0
            combined_df.to_excel(writer, sheet_name="Extracted", index=False, startrow=start_row)

        # === Возвращаем Base64 Excel ===
        with open(excel_path, "rb") as f:
            excel_bytes = f.read()
        excel_b64 = base64.b64encode(excel_bytes).decode("utf-8")

        # Удаляем временные файлы
        os.unlink(pdf_path)
        os.unlink(excel_path)

        return JSONResponse({
            "status": "ok",
            "file_name": file.filename.replace(".pdf", ".xlsx"),
            "tables_extracted": len(tables),
            "excel_base64": excel_b64
        })

    except Exception as e:
        return JSONResponse({"status": "error", "message": str(e)}, status_code=500)