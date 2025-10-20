from fastapi import FastAPI, File, UploadFile, Form
from fastapi.responses import JSONResponse
import pdfplumber
import tempfile
import camelot
import shutil
import os
import base64
import gc

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
    Возвращает JSON с путем к Excel и количеством извлечённых таблиц.
    Обрабатывает PDF постранично, объединяя таблицы, чтобы оптимизировать память.
    """
    import pandas as pd
    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp_pdf:
            tmp_pdf.write(await file.read())
            pdf_path = tmp_pdf.name

        # Получаем общее количество страниц
        with pdfplumber.open(pdf_path) as pdf:
            total_pages = len(pdf.pages)

        all_dfs = []
        total_tables = 0

        # Обрабатываем по одной странице за раз
        for page_num in range(1, total_pages + 1):
            # Извлекаем таблицы с текущей страницы
            tables = camelot.read_pdf(pdf_path, flavor="lattice", pages=str(page_num))
            if tables:
                total_tables += len(tables)
                for idx, table in enumerate(tables):
                    df = table.df
                    if len(all_dfs) == 0:
                        all_dfs.append(df)
                    else:
                        # Проверяем, является ли первая строка заголовком, если да, пропускаем её
                        first_row = df.iloc[0].tolist()
                        header_keywords = ["КНП", "Дебет", "Кредит", "Назначение", "БИК", "Номер документа"]
                        if any(any(key in str(cell) for key in header_keywords) for cell in first_row):
                            all_dfs.append(df.iloc[1:].reset_index(drop=True))
                        else:
                            all_dfs.append(df.reset_index(drop=True))
            # Очищаем память после обработки страницы
            gc.collect()

        if not all_dfs:
            os.unlink(pdf_path)
            return JSONResponse({"status": "error", "message": "No tables found."}, status_code=400)

        combined_df = pd.concat(all_dfs, ignore_index=True)

        # === Извлекаем метаданные (до первой строки с заголовками) ===
        metadata_lines = []
        with pdfplumber.open(pdf_path) as pdf:
            if total_pages > 0:
                lines = (pdf.pages[0].extract_text() or "").splitlines()
                header_keywords = ["КНП", "Дебет", "Кредит", "Назначение", "БИК", "Номер документа"]
                for line in lines:
                    if any(key in line for key in header_keywords):
                        break
                    metadata_lines.append(line)

        with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as tmp_xlsx:
            excel_path = tmp_xlsx.name

        with pd.ExcelWriter(excel_path, engine="openpyxl") as writer:
            # Добавляем одну пустую строку перед метаданными
            empty_meta = pd.DataFrame([[""]])
            empty_meta.to_excel(writer, sheet_name="Extracted", index=False, header=False, startrow=0)

            if metadata_lines:
                meta_df = pd.DataFrame({0: metadata_lines})
                # метаданные начинаются со второй строки (после пустой)
                meta_df.to_excel(writer, sheet_name="Extracted", index=False, header=False, startrow=1)
                start_row = len(metadata_lines) + 2  # метаданные + пустая строка
            else:
                start_row = 2  # если нет метаданных, таблица начнётся со 2 строки

            # Добавляем пустую строку перед таблицей с 0 в первой ячейке
            empty_row = pd.DataFrame([[""] * combined_df.shape[1]], columns=combined_df.columns)
            combined_df_with_empty = pd.concat([empty_row, combined_df], ignore_index=True)

            combined_df_with_empty.to_excel(
                writer,
                sheet_name="Extracted",
                index=False,
                header=False,
                startrow=start_row
            )

        # Кодируем Excel файл в base64
        with open(excel_path, "rb") as f_excel:
            excel_bytes = f_excel.read()
            excel_base64 = base64.b64encode(excel_bytes).decode("utf-8")

        # Удаляем временные файлы
        os.unlink(pdf_path)
        os.unlink(excel_path)

        return JSONResponse({
            "status": "ok",
            "file_name": file.filename.replace(".pdf", ".xlsx"),
            "tables_extracted": total_tables,
            "excel_base64": excel_base64
        })

    except Exception as e:
        return JSONResponse({"status": "error", "message": str(e)}, status_code=500)