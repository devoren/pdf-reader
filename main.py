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
    Извлекает текст из PDF.
    Если pages не передан — обрабатываются максимум 6 страниц.
    Пример pages: "1-7" или "2,5,6"
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
                if "-" in pages:
                    start, end = map(int, pages.split("-"))
                    page_numbers = list(range(start, end + 1))
                else:
                    page_numbers = [int(p.strip()) for p in pages.split(",") if p.strip().isdigit()]
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