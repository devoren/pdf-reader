from fastapi import FastAPI, File, UploadFile, Form
from fastapi.responses import JSONResponse
import pdfplumber
import tempfile

app = FastAPI(title="PDF Extractor API", version="1.1")

@app.post("/extract")
async def extract_text(
    file: UploadFile = File(...),
    pages: str = Form(None)  # pages можно не передавать
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

    Возвращает JSON с полным текстом, количеством страниц и другой информацией.
    """
    try:
        # Временное сохранение файла
        with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp:
            tmp.write(await file.read())
            tmp_path = tmp.name

        # Разбор параметра pages (если указан)
        page_numbers = None
        with pdfplumber.open(tmp_path) as pdf:
            total_pages = len(pdf.pages)

            if pages:
                pages_clean = pages.strip().lower()
                if pages_clean == "all":
                    page_numbers = list(range(1, total_pages + 1))
                elif "-" in pages_clean:
                    start, end = map(int, pages_clean.split("-"))
                    page_numbers = list(range(start, min(end, total_pages) + 1))
                elif pages_clean.isdigit():
                    end = int(pages_clean)
                    page_numbers = list(range(1, min(end, total_pages) + 1))
                else:
                    page_numbers = [
                        int(p.strip())
                        for p in pages_clean.split(",")
                        if p.strip().isdigit() and 1 <= int(p.strip()) <= total_pages
                    ]
            else:
                page_numbers = list(range(1, min(6, total_pages) + 1))

            result_text = []
            for i, page in enumerate(pdf.pages, start=1):
                if i in page_numbers:
                    text = page.extract_text() or ""
                    result_text.append(f"\n=== Страница {i} ===\n{text}")

        return JSONResponse({
            "status": "ok",
            "file_name": file.filename,
            "total_pages": total_pages,
            "pages_processed": len(result_text),
            "text_length": sum(len(t) for t in result_text),
            "text": "\n".join(result_text)
        })

    except Exception as e:
        return JSONResponse(
            {"status": "error", "message": str(e)},
            status_code=500
        )