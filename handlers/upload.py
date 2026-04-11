"""File upload handler. Extracts text from uploaded files for LLM context."""

from __future__ import annotations
import json
import os
import tempfile
import tornado.web

# Max upload 10MB
MAX_UPLOAD = 10 * 1024 * 1024

# Extensions we can extract text from
TEXT_EXTENSIONS = {".py", ".md", ".txt", ".json", ".csv", ".js", ".ts", ".html",
                   ".css", ".toml", ".yaml", ".yml", ".xml", ".sql", ".sh",
                   ".c", ".cpp", ".h", ".java", ".go", ".rs", ".rb", ".r", ".log"}


def extract_text(filename: str, body: bytes) -> str:
    """Extract readable text from file bytes. Returns extracted text or error."""
    ext = os.path.splitext(filename)[1].lower()

    if ext in TEXT_EXTENSIONS:
        try:
            return body.decode("utf-8", errors="replace")
        except Exception:
            return body.decode("latin-1", errors="replace")

    if ext == ".pdf":
        return _extract_pdf(body)

    return f"[Unsupported file type: {ext}. Supported: {', '.join(sorted(TEXT_EXTENSIONS))} and .pdf]"


def _extract_pdf(body: bytes) -> str:
    """Best-effort PDF text extraction without heavy deps."""
    try:
        import subprocess
        with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as f:
            f.write(body)
            tmp = f.name
        try:
            result = subprocess.run(
                ["pdftotext", "-layout", tmp, "-"],
                capture_output=True, text=True, timeout=30,
            )
            if result.returncode == 0 and result.stdout.strip():
                return result.stdout
        finally:
            os.unlink(tmp)
    except FileNotFoundError:
        pass  # pdftotext not installed
    except Exception:
        pass

    # Fallback: naive text extraction from PDF binary
    text_parts = []
    try:
        raw = body.decode("latin-1")
        import re
        # Extract text between BT and ET operators
        for match in re.finditer(r"\(([^)]{2,})\)", raw):
            candidate = match.group(1)
            if any(c.isalpha() for c in candidate):
                text_parts.append(candidate)
    except Exception:
        pass

    if text_parts:
        return "\n".join(text_parts)
    return "[Could not extract text from PDF. Install pdftotext for better results.]"


class FileUploadHandler(tornado.web.RequestHandler):
    def set_default_headers(self):
        self.set_header("Content-Type", "application/json")

    def post(self):
        if not self.request.files:
            self.set_status(400)
            self.write(json.dumps({"error": "No file uploaded"}))
            return

        file_info = None
        for field in self.request.files:
            file_info = self.request.files[field][0]
            break

        if not file_info:
            self.set_status(400)
            self.write(json.dumps({"error": "No file found"}))
            return

        filename = file_info["filename"]
        body = file_info["body"]

        if len(body) > MAX_UPLOAD:
            self.set_status(413)
            self.write(json.dumps({"error": f"File too large. Max {MAX_UPLOAD // (1024*1024)}MB"}))
            return

        text = extract_text(filename, body)
        # Truncate to reasonable context size
        if len(text) > 15000:
            text = text[:15000] + f"\n\n[...truncated, {len(text)} chars total]"

        self.write(json.dumps({
            "filename": filename,
            "size": len(body),
            "text": text,
            "chars": len(text),
        }))
