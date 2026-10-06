import os
import json
import requests

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload


app = FastAPI(title="File Transfer Worker")

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
# Remote Seekable File
# =========================

class RemoteFile:

    def __init__(self, url):

        self.url = url
        self.position = 0

        self.session = requests.Session()

        self.session.headers.update({
            "User-Agent": "Mozilla/5.0"
        })

        # Get file size and verify Range support
        response = self.session.get(
            self.url,
            headers={
                "Range": "bytes=0-0"
            },
            stream=True,
            timeout=(30, 60)
        )

        response.raise_for_status()

        self.range_supported = (
            response.status_code == 206
        )

        content_range = response.headers.get(
            "Content-Range"
        )

        if content_range and "/" in content_range:

            self.size = int(
                content_range.split("/")[-1]
            )

        else:

            content_length = response.headers.get(
                "Content-Length"
            )

            if content_length:
                self.size = int(content_length)

            else:
                response.close()

                raise RuntimeError(
                    "Unable to determine remote file size"
                )

        response.close()

        if not self.range_supported:

            raise RuntimeError(
                "Source server does not support HTTP Range requests"
            )


    def tell(self):

        return self.position


    def seek(self, offset, whence=0):

        if whence == 0:

            new_position = offset

        elif whence == 1:

            new_position = self.position + offset

        elif whence == 2:

            new_position = self.size + offset

        else:

            raise ValueError(
                "Invalid whence"
            )

        if new_position < 0:

            raise ValueError(
                "Negative seek position"
            )

        self.position = new_position

        return self.position


    def read(self, size=-1):

        if self.position >= self.size:

            return b""


        if size is None or size < 0:

            size = self.size - self.position


        end = min(
            self.position + size - 1,
            self.size - 1
        )


        response = self.session.get(
            self.url,
            headers={
                "Range": f"bytes={self.position}-{end}"
            },
            stream=True,
            timeout=(30, 300)
        )

        response.raise_for_status()


        if response.status_code != 206:

            response.close()

            raise RuntimeError(
                "Source server did not honor HTTP Range request"
            )


        data = response.content

        response.close()


        self.position += len(data)

        return data


    def close(self):

        self.session.close()


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

    remote_file = None

    try:

        # -------------------------
        # Open remote file
        # -------------------------

        remote_file = RemoteFile(
            data.url
        )

        print(
            f"Remote file size: "
            f"{remote_file.size / 1024 / 1024:.2f} MB"
        )


        # -------------------------
        # Google Drive
        # -------------------------

        drive = get_drive_service()


        # -------------------------
        # Metadata
        # -------------------------

        metadata = {
            "name": data.filename
        }


        if data.folder_id:

            metadata["parents"] = [
                data.folder_id
            ]


        # -------------------------
        # Resumable Upload
        # -------------------------

        media = MediaIoBaseUpload(
            remote_file,
            mimetype=data.mime_type,
            chunksize=CHUNK_SIZE,
            resumable=True
        )


        request = drive.files().create(
            body=metadata,
            media_body=media,
            fields="id,name,size,mimeType,webViewLink"
        )


        # -------------------------
        # Upload
        # -------------------------

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


        # -------------------------
        # Success
        # -------------------------

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

        if remote_file:

            remote_file.close()
