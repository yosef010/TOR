import os
import json
import requests

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload


app = FastAPI(title="File Transfer Worker")


# =========================
# Configuration
# =========================

CHUNK_SIZE = 8 * 1024 * 1024  # 8 MB


# =========================
# Request Model
# =========================

class TransferRequest(BaseModel):
    url: str
    filename: str
    mime_type: str = "application/octet-stream"
    folder_id: str | None = None


# =========================
# Google Drive
# =========================

def get_drive_service():

    credentials_json = os.getenv("GOOGLE_CREDENTIALS")

    if not credentials_json:
        raise RuntimeError(
            "GOOGLE_CREDENTIALS environment variable is missing"
        )

    credentials_data = json.loads(credentials_json)

    credentials = Credentials.from_authorized_user_info(
        credentials_data,
        scopes=[
            "https://www.googleapis.com/auth/drive.file"
        ]
    )

    return build(
        "drive",
        "v3",
        credentials=credentials,
        cache_discovery=False
    )


# =========================
# Streaming Reader
# =========================

class HTTPStream:

    def __init__(self, response):

        self.response = response
        self.iterator = response.iter_content(
            chunk_size=CHUNK_SIZE
        )

        self.buffer = b""


    def read(self, size=-1):

        if size == -1:

            chunks = [self.buffer]

            for chunk in self.iterator:

                if chunk:
                    chunks.append(chunk)

            self.buffer = b""

            return b"".join(chunks)


        while len(self.buffer) < size:

            try:

                chunk = next(self.iterator)

            except StopIteration:

                break

            if chunk:

                self.buffer += chunk


        data = self.buffer[:size]

        self.buffer = self.buffer[size:]

        return data


# =========================
# Health Check
# =========================

@app.get("/")
def health():

    return {
        "status": "ok",
        "service": "file-transfer-worker"
    }


# =========================
# Transfer
# =========================

@app.post("/transfer")
def transfer_file(data: TransferRequest):

    response = None

    try:

        # ---------------------------------
        # Open source URL as a stream
        # ---------------------------------

        response = requests.get(
            data.url,
            stream=True,
            timeout=(30, 300),
            headers={
                "User-Agent": "Mozilla/5.0"
            }
        )

        response.raise_for_status()


        # ---------------------------------
        # Google Drive
        # ---------------------------------

        drive = get_drive_service()


        # ---------------------------------
        # File metadata
        # ---------------------------------

        metadata = {
            "name": data.filename
        }


        # Optional Drive folder

        if data.folder_id:

            metadata["parents"] = [
                data.folder_id
            ]


        # ---------------------------------
        # Streaming upload
        # ---------------------------------

        stream = HTTPStream(response)

        media = MediaIoBaseUpload(
            stream,
            mimetype=data.mime_type,
            chunksize=CHUNK_SIZE,
            resumable=True
        )


        request = drive.files().create(
            body=metadata,
            media_body=media,
            fields="id,name,size,mimeType,webViewLink"
        )


        result = None


      while result is None:

            status, result = request.next_chunk()

            if status:

                progress = int(
                    status.progress() * 100
                )

                print(
                    f"Upload progress: {progress}%"
                )

        return {
            "success": True,
            "file": result
        }

    except requests.exceptions.RequestException as e:

        raise HTTPException(
            status_code=502,
            detail=f"Source download failed: {str(e)}"
        )

    except Exception as e:

        raise HTTPException(
            status_code=500,
            detail=str(e)
        )

    finally:

        if response:

            response.close()
            if __name__ == "__main__":
    print("Starting File Transfer Worker...")
