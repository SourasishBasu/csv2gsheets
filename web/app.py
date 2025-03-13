# app.py
from fastapi import FastAPI, File, UploadFile, Form
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
import httpx
import gzip
import tempfile
import os
import shutil
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(title="CSV Compression Service")

# Mount static files
app.mount("/static", StaticFiles(directory="static"), name="static")

# Go backend service URL
GO_BACKEND_URL = "http://localhost:8080/upload"

@app.get("/", response_class=HTMLResponse)
async def get_upload_page():
    with open("static/index.html", "r") as f:
        return f.read()

@app.post("/compress-and-forward/")
async def compress_and_forward(
    file: UploadFile = File(...),
    compression: str = Form("gzip")
):
    # Create a temporary directory
    with tempfile.TemporaryDirectory() as temp_dir:
        # Save the uploaded file
        temp_csv_path = os.path.join(temp_dir, "input.csv")
        with open(temp_csv_path, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)
        
        # Compress the file based on selected algorithm
        compressed_path = os.path.join(temp_dir, "compressed.gz")
        
        if compression == "gzip":
            with open(temp_csv_path, 'rb') as f_in:
                with gzip.open(compressed_path, 'wb', compresslevel=9) as f_out:
                    shutil.copyfileobj(f_in, f_out)
        else:
            # Default to gzip if unsupported compression type is specified
            with open(temp_csv_path, 'rb') as f_in:
                with gzip.open(compressed_path, 'wb', compresslevel=9) as f_out:
                    shutil.copyfileobj(f_in, f_out)
        
        # Forward the compressed file to the Go backend
        with open(compressed_path, 'rb') as f:
            files = {'file': (f"{file.filename}.gz", f, 'application/gzip')}
            async with httpx.AsyncClient(timeout=120.0) as client:
                try:
                    response = await client.post(GO_BACKEND_URL, files=files)
                    return JSONResponse(
                        content={
                            "status": "success",
                            "go_backend_response": response.text,
                            "status_code": response.status_code
                        },
                        status_code=200
                    )
                except httpx.RequestError as e:
                    return JSONResponse(
                        content={
                            "status": "error",
                            "message": f"Error communicating with Go backend: {str(e)}"
                        },
                        status_code=500
                    )

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)