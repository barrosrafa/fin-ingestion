"""Content-addressed Bronze archive with atomic writes and verified offline replay."""

import hashlib
import json
import os
import re
import tempfile
from pathlib import Path
from uuid import uuid4


class LocalRawArchive:
    def __init__(self, root: str | Path):
        self.root = Path(root)

    @staticmethod
    def _atomic_write(path: Path, content: bytes) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        fd, temporary = tempfile.mkstemp(dir=path.parent, prefix=".raw-")
        try:
            with os.fdopen(fd, "wb") as stream:
                stream.write(content)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, path)
        finally:
            if os.path.exists(temporary):
                os.unlink(temporary)

    def save(self, content: bytes, metadata: dict) -> str:
        digest = hashlib.sha256(content).hexdigest()
        path = self.root / "objects" / digest[:2] / f"{digest}.bin"
        if path.exists():
            self.read(path)
        else:
            self._atomic_write(path, content)
        # Each capture has its own audit entry, even when the body is deduplicated.
        entry = {**metadata, "content_sha256": digest, "object": str(path.resolve())}
        self._atomic_write(
            self.root / "captures" / f"{uuid4().hex}.json",
            json.dumps(entry, sort_keys=True, default=str).encode(),
        )
        return str(path.resolve())

    @staticmethod
    def read(path: str | Path) -> bytes:
        path = Path(path)
        if not re.fullmatch(r"[0-9a-f]{64}", path.stem):
            raise ValueError("Bronze object filename must contain its SHA-256")
        content = path.read_bytes()
        if hashlib.sha256(content).hexdigest() != path.stem:
            raise ValueError("Bronze object checksum mismatch")
        return content
