"""Plain-text extract from docx/xlsx for read-only preview. Not a full Office renderer."""

from __future__ import annotations

import io
import zipfile
import xml.etree.ElementTree as ET

_W = "{http://schemas.openxmlformats.org/wordprocessingml/2006/main}"
_S = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"

# Cap uncompressed zip member size before read/parse (zip-bomb guard).
# Also caps assembled plaintext — xlsx shared strings can amplify far past member size.
# Keep in sync with PREVIEW_MAX_BYTES in api.routes.file_attachments.
MAX_UNCOMPRESSED_MEMBER_BYTES = 1024 * 1024


class _TextBudget:
    """Accumulate UTF-8 plaintext up to max_bytes; stop before materializing more."""

    __slots__ = ("max_bytes", "used", "parts", "truncated")

    def __init__(self, max_bytes: int) -> None:
        self.max_bytes = max_bytes
        self.used = 0
        self.parts: list[str] = []
        self.truncated = False

    def add(self, s: str) -> bool:
        """Append s (or a UTF-8 prefix). False = budget exhausted, caller must stop."""
        if self.truncated or self.used >= self.max_bytes:
            self.truncated = True
            return False
        raw = s.encode("utf-8")
        room = self.max_bytes - self.used
        if len(raw) <= room:
            self.parts.append(s)
            self.used += len(raw)
            return True
        if room:
            self.parts.append(raw[:room].decode("utf-8", errors="ignore"))
            self.used = self.max_bytes
        self.truncated = True
        return False

    def text(self) -> str:
        return "".join(self.parts)


def extract_office_text(
    filename: str,
    data: bytes,
    *,
    max_bytes: int = MAX_UNCOMPRESSED_MEMBER_BYTES,
) -> tuple[str, bool]:
    """Return (plaintext, truncated). Never builds more than max_bytes of UTF-8 text."""
    name = (filename or "").lower()
    budget = _TextBudget(max_bytes)
    if name.endswith(".docx"):
        _docx(data, budget)
    elif name.endswith(".xlsx"):
        _xlsx(data, budget)
    else:
        raise ValueError("not an office file")
    return budget.text(), budget.truncated


def _docx(data: bytes, budget: _TextBudget) -> None:
    xml = _zip_read(data, "word/document.xml")
    if xml is None:
        raise ValueError("docx has no document.xml")
    root = ET.fromstring(xml)
    first = True
    for p in root.iter(f"{_W}p"):
        bits = [t.text or "" for t in p.iter(f"{_W}t")]
        line = "".join(bits).strip()
        if not line:
            continue
        chunk = line if first else f"\n{line}"
        first = False
        if not budget.add(chunk):
            return


def _xlsx(data: bytes, budget: _TextBudget) -> None:
    strings = _xlsx_shared(data)
    sheets = _zip_names(data, prefix="xl/worksheets/sheet", suffix=".xml")
    if not sheets:
        raise ValueError("xlsx has no worksheets")
    first_block = True
    for name in sheets:
        xml = _zip_read(data, name)
        if not xml:
            continue
        label = name.rsplit("/", 1)[-1]
        header = f"# {label}\n" if first_block else f"\n\n# {label}\n"
        if not budget.add(header):
            return
        first_block = False
        if not _xlsx_sheet(xml, strings, budget):
            return


def _xlsx_shared(data: bytes) -> list[str]:
    xml = _zip_read(data, "xl/sharedStrings.xml")
    if not xml:
        return []
    root = ET.fromstring(xml)
    out: list[str] = []
    for si in root.iter(f"{_S}si"):
        out.append("".join(t.text or "" for t in si.iter(f"{_S}t")))
    return out


def _xlsx_sheet(xml: bytes, strings: list[str], budget: _TextBudget) -> bool:
    """Stream cell text into budget. False if budget exhausted."""
    root = ET.fromstring(xml)
    first_line = True
    for row in root.iter(f"{_S}row"):
        # Refs only for shared strings — do not "\t".join the whole row (amplification).
        cells = [_xlsx_cell(c, strings) for c in row.findall(f"{_S}c")]
        if not any(cells):
            continue
        if not first_line and not budget.add("\n"):
            return False
        first_line = False
        for i, val in enumerate(cells):
            if i and not budget.add("\t"):
                return False
            if not budget.add(val):
                return False
    return True


def _xlsx_cell(cell: ET.Element, strings: list[str]) -> str:
    kind = cell.get("t")
    if kind == "s":
        v = cell.find(f"{_S}v")
        try:
            return strings[int(v.text)] if v is not None and v.text else ""
        except (TypeError, ValueError, IndexError):
            return ""
    if kind == "inlineStr":
        return "".join(t.text or "" for t in cell.iter(f"{_S}t"))
    v = cell.find(f"{_S}v")
    return (v.text or "") if v is not None else ""


def _zip_read(
    data: bytes,
    name: str,
    *,
    max_uncompressed: int = MAX_UNCOMPRESSED_MEMBER_BYTES,
) -> bytes | None:
    """Read one zip member with a hard uncompressed-size ceiling (zip-bomb guard)."""
    try:
        with zipfile.ZipFile(io.BytesIO(data)) as zf:
            info = zf.getinfo(name)
            if info.file_size > max_uncompressed:
                raise ValueError(
                    f"zip member {name!r} uncompressed size {info.file_size} "
                    f"exceeds {max_uncompressed}"
                )
            with zf.open(name) as fp:
                chunk = fp.read(max_uncompressed + 1)
            if len(chunk) > max_uncompressed:
                raise ValueError(
                    f"zip member {name!r} expanded past {max_uncompressed} bytes"
                )
            return chunk
    except KeyError:
        return None
    except (zipfile.BadZipFile, OSError):
        return None


def _zip_names(data: bytes, *, prefix: str, suffix: str) -> list[str]:
    try:
        with zipfile.ZipFile(io.BytesIO(data)) as zf:
            names = [n for n in zf.namelist() if n.startswith(prefix) and n.endswith(suffix)]
    except (zipfile.BadZipFile, OSError):
        return []
    return sorted(names)
