from fastapi import FastAPI, File, UploadFile, Form
from fastapi.responses import JSONResponse
import pdfplumber
import tempfile
import camelot
import PyPDF2

def clean_camelot_csv(raw_csv: str):
    import csv, io
    reader = csv.reader(io.StringIO(raw_csv))
    rows = [r for r in reader if any(cell.strip() for cell in r)]
    if rows and all(c.isdigit() or c == '' for c in rows[0]):
        rows = rows[1:]
    cleaned = []
    buffer = []
    for row in rows:
        if row[0].strip():
            if buffer:
                cleaned.append(buffer)
                buffer = []
            buffer = row
        else:
            buffer = [a + " " + b if b else a for a, b in zip(buffer, row)]
    if buffer:
        cleaned.append(buffer)
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerows(cleaned)
    return output.getvalue()

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
    pages: str = Form(None),  # pages можно не передавать
    options: str = Form("plumber")
):
    """
    Извлекает текст из загруженного PDF-файла.

    Параметры:
    - file: PDF-файл, из которого нужно получить текст.
    - pages (необязательно): укажите, какие страницы нужно обработать.
        • Можно указать диапазон — например, "1-5"
        • Можно список — например, "1,3,7"
        • Можно "all" — чтобы извлечь текст со всех страниц.
        • Если параметр не указан, обрабатываются первые 6 страниц.
      Параметр pages работает одинаково для обоих движков.
    - options: движок для извлечения текста ("plumber", "camelot").

    Возвращает JSON с полным текстом, количеством страниц, используемым движком и другой информацией.
    """
    try:
        # Временное сохранение файла
        with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp:
            tmp.write(await file.read())
            tmp_path = tmp.name

        engine_used = None
        result_text = []
        total_pages = 0
        pages_processed = 0

        if options == "camelot":
            # Определяем количество страниц PDF
            with open(tmp_path, "rb") as f:
                reader = PyPDF2.PdfReader(f)
                total_pages = len(reader.pages)

            # Преобразуем список страниц в строку для Camelot
            page_numbers = parse_pages_param(pages, total_pages)
            pages_str = ",".join(map(str, page_numbers))

            # Используем camelot для извлечения таблиц
            tables = camelot.read_pdf(tmp_path, flavor="stream", pages=pages_str)
            if tables:
                import pandas as pd
                header_row = None  # Для хранения заголовков таблицы
                for idx, table in enumerate(tables):
                    df = table.df.copy()
                    flat_text = " ".join(df.astype(str).values.flatten()).lower()

                    # 📄 Если первая таблица содержит служебную шапку — запоминаем её, но не добавляем в результат
                    if idx == 0 and any(k in flat_text for k in ["выписка", "остаток", "владелец", "счет №"]):
                        header_row = df.iloc[-1].tolist() if len(df) > 1 else df.iloc[0].tolist()
                        continue

                    # 🧩 Добавляем заголовок, если он отсутствует
                    if header_row is not None and list(df.iloc[0]) != header_row:
                        n_cols = df.shape[1]
                        hdr = header_row[:n_cols] if len(header_row) > n_cols else header_row + [''] * (n_cols - len(header_row))
                        import pandas as pd
                        header_df = pd.DataFrame([hdr], columns=df.columns)
                        df = pd.concat([header_df, df], ignore_index=True)

                    csv_text = clean_camelot_csv(df.to_csv(index=False))
                    result_text.append(f"=== Страница {table.page} ===\n{csv_text}")
                unique_pages = {t.page for t in tables}
                pages_processed = len(unique_pages)
            else:
                pages_processed = 0
            engine_used = "camelot"

        elif options == "plumber":
            with pdfplumber.open(tmp_path) as pdf:
                total_pages = len(pdf.pages)
                page_numbers = parse_pages_param(pages, total_pages)
                for i, page in enumerate(pdf.pages, start=1):
                    if i in page_numbers:
                        text = page.extract_text() or ""
                        result_text.append(f"\n=== Страница {i} ===\n{text}")
                pages_processed = len(result_text)
            engine_used = "pdfplumber"

        else:
            return JSONResponse(
                {"status": "error", "message": f"Unsupported option '{options}'. Use 'plumber' or 'camelot'."},
                status_code=400
            )

        return JSONResponse({
            "status": "ok",
            "file_name": file.filename,
            "total_pages": total_pages,
            "pages_processed": pages_processed,
            "text_length": sum(len(t) for t in result_text),
            "text": "\n".join(result_text),
            "engine_used": engine_used
        })

    except Exception as e:
        return JSONResponse(
            {"status": "error", "message": str(e)},
            status_code=500
        )