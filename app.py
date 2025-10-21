import io
import os
import re
import csv
import json
import time
import zipfile
import unicodedata
from datetime import datetime
from typing import List, Dict, Tuple, Optional

import streamlit as st

# ────────────────────────────────────────────────────────────────────────────────
# Streamlit config
# ────────────────────────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="Notion → Markdown/JSONL/CSV konverter",
    page_icon="📦",
    layout="centered",
)

st.title("📦 Notion → Markdown/JSONL/CSV konverter")
st.caption("Notion Markdown exportból kinyeri a **Videó/Lecke** szöveget, tisztít, chunkol (opcionális), és táblázat-kivonatot készít.")

# ────────────────────────────────────────────────────────────────────────────────
# Kis segédek
# ────────────────────────────────────────────────────────────────────────────────

def run_id() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def normalize(s: str) -> str:
    if not s:
        return ""
    s = s.strip().lower()
    s = "".join(ch for ch in unicodedata.normalize("NFKD", s) if not unicodedata.combining(ch))
    s = re.sub(r"[^a-z0-9]+", " ", s).strip()
    return s


def slugify(s: str) -> str:
    s = normalize(s)
    s = re.sub(r"\s+", "-", s)
    s = re.sub(r"[^a-z0-9\-]+", "", s)
    return s or "doc"


def safe_filename_preserve_accents(s: str) -> str:
    # fájlnévhez engedjük az ékezeteket, de szűrjük az egyéb nem kívánt karaktereket
    s = s.strip().replace("/", "-").replace("\\", "-")
    s = re.sub(r"[^\w\-\.\sÁÉÍÓÖŐÚÜŰáéíóöőúüű]+", "", s, flags=re.UNICODE)
    s = re.sub(r"\s+", " ", s).strip()
    return s or "file"


def build_md_filename(title: str, sorsz_int: Optional[int], page_id: Optional[str], kurzus: Optional[str] = None) -> str:
    """
    Kért séma: 'Kurzus - Sorszám - Név.md'
    - ha bármelyik hiányzik, kulturáltan kihagyjuk
    """
    parts = []
    if kurzus:
        parts.append(safe_filename_preserve_accents(kurzus))
    if sorsz_int is not None:
        parts.append(str(sorsz_int))
    if title:
        parts.append(safe_filename_preserve_accents(title))
    base = " - ".join(parts) if parts else "cikk"
    return f"{base}.md"


def uniquify_filename(name: str, used: set, page_id: Optional[str] = None) -> str:
    """
    Ha már létezik adott név a ZIP-ben, egészítsük ki rövid page_id-vel,
    ha az is ütközik, tegyünk sorszámozott zárójelet.
    """
    if name not in used:
        used.add(name)
        return name
    stem, ext = os.path.splitext(name)
    if page_id:
        candidate = f"{stem} - {page_id[:8]}{ext}"
        if candidate not in used:
            used.add(candidate)
            return candidate
    i = 2
    while True:
        candidate = f"{stem} ({i}){ext}"
        if candidate not in used:
            used.add(candidate)
            return candidate
        i += 1


# ────────────────────────────────────────────────────────────────────────────────
# Markdown szekcionálás
# ────────────────────────────────────────────────────────────────────────────────

HEADING_RE = re.compile(r"^(#{1,6})\s+(.*)$")

def split_markdown_sections(md: str) -> List[Tuple[int, str, List[str]]]:
    """
    Vissza: [(szint, heading, sorok)], ahol 'sorok' a heading utáni tartalom a következő headingig.
    """
    lines = (md or "").splitlines()
    sections: List[Tuple[int, str, List[str]]] = []

    current_level = 0
    current_title = ""
    current_buf: List[str] = []

    def flush():
        nonlocal current_level, current_title, current_buf
        if current_level > 0:
            sections.append((current_level, current_title, current_buf))
        current_level = 0
        current_title = ""
        current_buf = []

    for ln in lines:
        m = HEADING_RE.match(ln)
        if m:
            # új szekció
            flush()
            current_level = len(m.group(1))
            current_title = m.group(2).strip()
            current_buf = []
        else:
            if current_level == 0:
                # heading előtt/után álló tartalom (H1 előtti rész)
                current_title = ""
                current_level = 0
                current_buf = []
            current_buf.append(ln)

    flush()
    return sections

# ────────────────────────────────────────────────────────────────────────────────
# Cél címkék (konfigurálható a UI-ban)
# ────────────────────────────────────────────────────────────────────────────────
DEFAULT_VIDEO_LABELS = [
    "videó szöveg",
]

DEFAULT_LESSON_LABELS = [
    "lecke szöveg"
]

def label_match(title: str, target_tokens: List[str]) -> bool:
    """Fuzzy/normalizált egyezés (ékezet, írásjelek elhagyása; token-alapú részleges match)."""
    tnorm = normalize(title)
    for raw in target_tokens:
        cand = normalize(raw)
        subtoks = cand.split()
        ok = all(tok in tnorm for tok in subtoks if tok)
        if ok:
            return True
    return False


# ────────────────────────────────────────────────────────────────────────────────
# Markdown tisztítás
# ────────────────────────────────────────────────────────────────────────────────

def clean_markdown(md: str) -> str:
    if not md:
        return ""
    # headingek előtt 1 üres sor, kódblokkok megkímélése, üres sorok normalizálása
    out = []
    in_code = False
    fence = re.compile(r"^\s*```")
    prev_blank = True
    for line in md.splitlines():
        if fence.match(line):
            in_code = not in_code
            out.append(line)
            continue
        if in_code:
            out.append(line)
            continue
        if HEADING_RE.match(line):
            if not prev_blank:
                out.append("")
            out.append(line)
            prev_blank = False
            continue
        if line.strip() == "":
            if not prev_blank:
                out.append("")
                prev_blank = True
            continue
        out.append(line)
        prev_blank = False
    return "\n".join(out).strip()


def renumber_ordered_lists(md: str) -> str:
    """
    Számozott listák újraszámozása (kódblokkokon kívül), '1.' formátum támogatott, behúzás-alapú szintek.
    """
    if not md:
        return ""
    lines = md.splitlines()
    out = []
    in_code = False
    fence = re.compile(r"^\s*```")
    list_item = re.compile(r"^(\s*)(\d+)\.\s+")
    counters: Dict[int, int] = {}  # indent → counter

    def level_of(indent: str) -> int:
        return len(indent.replace("\t", "    ")) // 2  # 2 space szint

    for line in lines:
        if fence.match(line):
            in_code = not in_code
            out.append(line)
            continue
        if in_code:
            out.append(line)
            continue

        m = list_item.match(line)
        if m:
            indent = m.group(1)
            lvl = level_of(indent)
            if lvl not in counters:
                counters[lvl] = 1
            else:
                counters[lvl] += 1
            # nullázás mélyebb szinteken
            for k in list(counters.keys()):
                if k > lvl:
                    del counters[k]
            newnum = counters[lvl]
            # FIX: \1 helyett \g<1>, hogy ne legyen \11, \110 stb. csoport hivatkozás
            line = list_item.sub(r"\g<1>{0}. ".format(newnum), line, count=1)
            out.append(line)
        else:
            out.append(line)
    return "\n".join(out).strip()

def strip_bold_emphasis(md: str) -> str:
    """
    Eltávolítja a **…** és __…__ kiemelést kódblokkokon kívül,
    a tartalmat meghagyva (gépi feldolgozást segíti).
    """
    if not md:
        return ""
    lines = md.splitlines()
    out = []
    in_code = False
    fence = re.compile(r"^\s*```")
    bold_ast = re.compile(r"(?<!\*)\*\*(.+?)\*\*(?!\*)")
    bold_uscr = re.compile(r"(?<!_)__(.+?)__(?!_)")
    for line in lines:
        if fence.match(line):
            in_code = not in_code
            out.append(line)
            continue
        if in_code:
            out.append(line)
            continue
        # inline code védelme: daraboljuk backtick alapján
        parts = re.split(r"(`[^`]*`)", line)
        for i, part in enumerate(parts):
            if i % 2 == 0:  # nem inline code
                part = bold_ast.sub(r"\1", part)
                part = bold_uscr.sub(r"\1", part)
            out.append(part)
        # join nélkül, mert már out-hoz appendeltük részenként
        out.append("")  # sorzárás
    # a fenti extra üres sorok eltávolítása
    out = [ln for ln in out if ln != ""]
    return "\n".join(out).strip()

# ────────────────────────────────────────────────────────────────────────────────
# Táblázat kinyerés
# ────────────────────────────────────────────────────────────────────────────────

ALIGN_RE = re.compile(r"^\s*\|?\s*:?-{3,}:?\s*(\|\s*:?-{3,}:?\s*)+\|?\s*$")

def _split_md_row(row: str) -> List[str]:
    # '|' szeparálás, escape-elt \| figyelembevétele
    toks = re.split(r"(?<!\\)\|", row.strip().strip("|"))
    toks = [t.replace(r"\|", "|").strip() for t in toks]
    return toks

def _make_key_like(s: str) -> str:
    s = s.strip().lower()
    s = "".join(ch for ch in unicodedata.normalize("NFKD", s) if not unicodedata.combining(ch))
    s = re.sub(r"[^\w]+", "_", s).strip("_")
    return s or "col"

def extract_tables(md: str) -> Tuple[str, List[Dict]]:
    """
    Kinyeri a GFM táblákat a markdownból és JSON-osítja.
    Vissza: (md_with_tables_json_section, tables_list)
    """
    if not md:
        return md, []

    lines = md.splitlines()
    tables = []
    i = 0
    while i < len(lines) - 1:
        header_line = lines[i]
        sep_line = lines[i + 1]
        if "|" in header_line and ALIGN_RE.match(sep_line or ""):
            headers = _split_md_row(header_line)
            # adat sorok
            rows = []
            j = i + 2
            while j < len(lines):
                ln = lines[j]
                if "|" not in ln or (HEADING_RE.match(ln) or ln.strip() == ""):
                    break
                rows.append(_split_md_row(ln))
                j += 1
            if headers and rows:
                clean_headers = [strip_bold_emphasis(h) for h in headers]
                keys = [_make_key_like(h) for h in clean_headers]
                row_objs = []
                for r in rows:
                    if len(r) < len(keys):
                        r = r + [""] * (len(keys) - len(r))
                    elif len(r) > len(keys):
                        r = r[:len(keys)]
                    vals = [strip_bold_emphasis(c) for c in r]
                    row_objs.append({k: v for k, v in zip(keys, vals)})
                tables.append({
                    "headers_raw": clean_headers,
                    "headers": keys,
                    "rows": row_objs,
                    "start": i,
                    "end": j - 1,
                })
                i = j
                continue
        i += 1

    if tables:
        out = [md.strip(), "", "## Adattáblák (gépi kivonat)", ""]
        for idx, t in enumerate(tables, start=1):
            out.append(f"**Táblázat {idx}**")
            out.append("")
            out.append("```json")
            out.append(json.dumps(t, ensure_ascii=False, indent=2))
            out.append("```")
            out.append("")
        return "\n".join(out).strip(), tables
    return md, []

# ────────────────────────────────────────────────────────────────────────────────
# ZIP fájlnév-dekódolás + tartalom (UTF-8 + BOM támogatás)
# ────────────────────────────────────────────────────────────────────────────────
def _fixed_zip_filename(info: zipfile.ZipInfo) -> str:
    """
    Ha nincs UTF-8 flag a ZIP fejlécben, a python zipfile cp437-t feltételez.
    Visszaalakítjuk a nevet cp437→bytes→utf-8, így az ékezetek helyreállnak.
    """
    name = info.filename
    try:
        if not (info.flag_bits & 0x800):  # bit11 jelzi az UTF-8 flaget
            return name.encode("cp437").decode("utf-8")
    except Exception:
        pass
    return name

def iter_markdown_files(zf: zipfile.ZipFile) -> List[Tuple[str, str]]:
    out: List[Tuple[str, str]] = []
    for info in zf.infolist():
        if info.is_dir():
            continue
        name = _fixed_zip_filename(info)
        if not name.lower().endswith(".md"):
            continue
        try:
            raw = zf.read(info)
            # BOM tolerant
            try:
                txt = raw.decode("utf-8-sig")
            except UnicodeDecodeError:
                txt = raw.decode("utf-8", errors="replace")
            out.append((name, txt))
        except Exception:
            continue
    return out

def extract_page_id_from_filename(filename: str) -> Optional[str]:
    # Notion export fájlnevek végén gyakran ott a 32 hex page_id
    m = re.search(r"([0-9a-f]{32})\.\w+$", filename)
    if m:
        return m.group(1)
    return None

def extract_page_title(md: str, fallback: str) -> str:
    # Első H1 cím (# ...)
    for line in (md or "").splitlines():
        m = HEADING_RE.match(line)
        if m and len(m.group(1)) == 1:
            return m.group(2).strip()
    return fallback

# ────────────────────────────────────────────────────────────────────────────────
# Metaadat-parzolás (fejléc utáni kulcs: érték sorok)
# ────────────────────────────────────────────────────────────────────────────────
_META_ALIASES = {
    "szakasz": ["szakasz", "section", "fejezet", "modul"],
    "video_statusz": ["videó státusz", "video statusz", "videostatusz", "videó status", "videostatus", "státusz", "statusz", "státus", "status"],
    "lecke_hossza": ["lecke hossza", "lesson length", "hossz"],
    "utolso_modositas": ["utolsó módosítás", "utolso modositas", "last modified", "utolsó módosítás dátuma"],
    "tipus": ["típus", "tipus", "type"],
    "kurzus": ["kurzus", "course"],
    "vimeo_link": ["vimeo link", "vimeo url", "vimeo", "videó url", "video url"],
    "sorszam": ["sorszám", "sorszam", "order", "index", "rank"],
}

def _canon_key(raw_key: str) -> Optional[str]:
    nk = normalize(raw_key)
    for can, alist in _META_ALIASES.items():
        for a in alist:
            if normalize(a) == nk:
                return can
    return None

META_LINE_RE = re.compile(r"^\s*([^\:]{1,120})\s*:\s*(.+?)\s*$")

def parse_metadata_block(full_md: str) -> Dict[str, Optional[str]]:
    """
    A H1 után, a következő H2-ig terjedő blokkban keresi a
    'Kulcs: érték' sorokat.
    Vissza: meta dict kanonikus kulcsokkal (stringek), 'sorszam' → int-ként is parse-olható.
    """
    lines = (full_md or "").splitlines()
    i = 0
    # Ugorjunk az első H1 utánra
    while i < len(lines):
        m = HEADING_RE.match(lines[i])
        if m and len(m.group(1)) == 1:
            i += 1
            break
        i += 1

    meta: Dict[str, Optional[str]] = {}
    while i < len(lines):
        ln = lines[i].rstrip("\n")
        m = HEADING_RE.match(ln)
        if m and len(m.group(1)) >= 2:
            break
        if not ln.strip():
            i += 1
            continue
        m = META_LINE_RE.match(ln)
        if m:
            raw_k = m.group(1).strip()
            val = m.group(2).strip()
            ck = _canon_key(raw_k)
            if ck:
                meta[ck] = val
        i += 1
    return meta

def meta_sorszam_as_int(meta: Dict[str, Optional[str]]) -> Optional[int]:
    v = (meta.get("sorszam") or "").strip()
    if not v:
        return None
    m = re.search(r"\d+", v.replace(" ", ""))
    if not m:
        return None
    try:
        return int(m.group(0))
    except Exception:
        return None

# ────────────────────────────────────────────────────────────────────────────────
# Választás logika (Videó → Lecke → None)
# ────────────────────────────────────────────────────────────────────────────────

def choose_section(sections: List[Tuple[int, str, List[str]]], video_labels: List[str], lesson_labels: List[str]) -> Tuple[str, str, str]:
    """
    A címkék alapján kiválasztja a fő szöveget.
    Elsőbbség: videó → lecke → none.
    Vissza: (selected_section, raw_text, selected_heading)
    """
    video_txt = ""
    lesson_txt = ""
    video_heading = ""
    lesson_heading = ""

    for level, heading, lines in sections:
        if 2 <= level <= 4:
            if label_match(heading, video_labels):
                video_txt = "\n".join(lines).strip()
                video_heading = heading
            if label_match(heading, lesson_labels):
                lesson_txt = "\n".join(lines).strip()
                lesson_heading = heading

    if video_txt.strip():
        return "video", video_txt, video_heading or "Videó szöveg"
    if lesson_txt.strip():
        return "lecke", lesson_txt, lesson_heading or "Lecke szöveg"
    return "none", "", ""

# ────────────────────────────────────────────────────────────────────────────────
# Chunkolás (bekezdés-határok mentén)
# ────────────────────────────────────────────────────────────────────────────────

def split_by_paragraph(text: str) -> List[str]:
    # Kódblokkokat nem törjük meg
    out: List[str] = []
    in_code = False
    fence = re.compile(r"^\s*```")
    buf: List[str] = []
    for ln in (text or "").splitlines():
        if fence.match(ln):
            in_code = not in_code
            buf.append(ln)
            continue
        if in_code:
            buf.append(ln)
            continue
        if ln.strip() == "":
            if buf:
                out.append("\n".join(buf).strip())
                buf = []
        else:
            buf.append(ln)
    if buf:
        out.append("\n".join(buf).strip())
    return [p for p in out if p.strip()]

def chunk_markdown(text: str, target_chars: int = 5500, overlap_chars: int = 400) -> List[Dict]:
    parts = split_by_paragraph(text)
    if not parts:
        return [{"text": text, "start": 0, "end": len(text)}]

    chunks: List[Dict] = []
    cur: List[str] = []
    cur_len = 0
    start = 0
    for p in parts:
        pl = len(p) + 1  # +1: bekezdés közti \n
        if cur_len + pl > target_chars and cur:
            s = "\n\n".join(cur).strip()
            chunks.append({"text": s, "start": start, "end": start + len(s)})
            # átfedéshez: vágjuk vissza a végét
            if overlap_chars > 0:
                overlap = s[-overlap_chars:]
                cur = [overlap]
                cur_len = len(overlap)
                start = start + len(s) - overlap_chars
            else:
                cur = []
                cur_len = 0
                start = start + len(s)
        cur.append(p)
        cur_len += pl
    if cur:
        s = "\n\n".join(cur).strip()
        chunks.append({"text": s, "start": start, "end": start + len(s)})
    return chunks

# ────────────────────────────────────────────────────────────────────────────────
# Konverzió (fő logika) – JSONL + CSV + Report + Clean MD + Tables JSONL
# ────────────────────────────────────────────────────────────────────────────────
def convert_zip_to_datasets(
    zip_bytes: bytes,
    video_labels: List[str],
    lesson_labels: List[str],
    do_chunk: bool,
    target_chars: int,
    overlap_chars: int
) -> Tuple[bytes, bytes, bytes, bytes, bytes]:
    """
    Vissza: (jsonl_bytes, csv_bytes_bom, report_csv_bytes_bom, clean_md_zip_bytes, tables_jsonl_bytes)
    """
    rid = run_id()
    zf = zipfile.ZipFile(io.BytesIO(zip_bytes), "r")
    md_files = iter_markdown_files(zf)

    jsonl_buf = io.StringIO()

    # CSV bufferek: Windows/Excel kompatibilis sorvégekkel
    csv_buf = io.StringIO(newline="")
    rep_buf = io.StringIO(newline="")
    csv_w = csv.writer(csv_buf, lineterminator="\n")
    rep_w = csv.writer(rep_buf, lineterminator="\n")

    # CSV fejléc – kiegészítve meta oszlopokkal
    csv_w.writerow([
        "file_name", "page_id", "page_title",
        "selected_section", "selected_heading",
        "char_len", "tartalom",
        "meta_szakasz", "meta_video_statusz", "meta_lecke_hossza", "meta_utolso_modositas",
        "meta_tipus", "meta_kurzus", "meta_vimeo_link", "meta_sorszam"
    ])
    rep_w.writerow(["file_name", "page_id", "page_title", "video_len", "lesson_len", "selected", "selected_len"])

    # Tisztított MD-k külön ZIP-be
    md_zip_buf = io.BytesIO()
    md_zip = zipfile.ZipFile(md_zip_buf, "w", compression=zipfile.ZIP_DEFLATED)
    used_names = set()  # ← ütközéskezelés a ZIP-ben

    # Táblázatok külön JSONL-be (összes dokumentum)
    tables_jsonl_buf = io.StringIO()

    total = len(md_files)
    ok = 0
    skipped = 0  # nincs szűrés, nem nő
    progress = st.progress(0.0, text=f"0/{total} feldolgozva (✅: 0, kihagyva: 0)")

    for idx, (fname, text) in enumerate(md_files, start=1):
        page_id = extract_page_id_from_filename(fname) or ""
        title = extract_page_title(text, fallback=os.path.splitext(os.path.basename(fname))[0])

        # Metaadatok a H1 utáni blokkból
        meta = parse_metadata_block(text)
        sorsz_int = meta_sorszam_as_int(meta)

        # Szétbontás szekciókra és választás
        sections = split_markdown_sections(text)
        video_txt = ""; lesson_txt = ""
        for level, heading, lines in sections:
            if 2 <= level <= 4:
                if label_match(heading, video_labels):  video_txt = "\n".join(lines).strip()
                if label_match(heading, lesson_labels): lesson_txt = "\n".join(lines).strip()

        selected, raw, selected_heading = choose_section(sections, video_labels, lesson_labels)

        # tisztítás
        raw = strip_bold_emphasis(raw)
        raw = clean_markdown(raw)
        raw = renumber_ordered_lists(raw)

        # táblázatok kivonata csak a kiválasztott szövegből
        md_with_tables, tables = extract_tables(raw)
        if tables:
            # táblák JSONL – globális gyűjtő
            for t in tables:
                tables_jsonl_buf.write(json.dumps({
                    "run_id": rid,
                    "page_id": page_id,
                    "file_name": os.path.basename(fname),
                    "page_title": title,
                    "selected_section": selected,
                    "selected_heading": selected_heading,
                    "table": t
                }, ensure_ascii=False) + "\n")

        # JSONL rekord(ok)
        base_rec = {
            "run_id": rid,
            "doc_id": slugify(title) if not page_id else f"{slugify(title)}_{page_id[:8]}",
            "page_id": page_id,
            "file_name": os.path.basename(fname),
            "page_title": title,
            "selected_section": selected,
            "selected_heading": selected_heading,
            "char_len": len(md_with_tables),
            # meta:
            "meta_szakasz": meta.get("szakasz") or "",
            "meta_video_statusz": meta.get("video_statusz") or "",
            "meta_lecke_hossza": meta.get("lecke_hossza") or "",
            "meta_utolso_modositas": meta.get("utolso_modositas") or "",
            "meta_tipus": meta.get("tipus") or "",
            "meta_kurzus": meta.get("kurzus") or "",
            "meta_vimeo_link": meta.get("vimeo_link") or "",
            "meta_sorszam": sorsz_int if sorsz_int is not None else "",
        }

        if do_chunk:
            try:
                parts = chunk_markdown(md_with_tables, target_chars, overlap_chars)
            except Exception as e:
                st.warning(f"Chunkolás közbeni hiba: {e}. Teljes szöveg egy blokkban mentve.")
                parts = [{"text": md_with_tables, "start": 0, "end": len(md_with_tables)}]
            for i, ch in enumerate(parts, start=1):
                rec = dict(base_rec)
                rec.update({"chunk_index": i, "text_markdown": ch["text"], "char_len": len(ch["text"])})
                jsonl_buf.write(json.dumps(rec, ensure_ascii=False) + "\n")
        else:
            rec = dict(base_rec)
            rec.update({"text_markdown": md_with_tables})
            jsonl_buf.write(json.dumps(rec, ensure_ascii=False) + "\n")

        # CSV sor
        csv_w.writerow([
            os.path.basename(fname),
            page_id,
            title,
            selected,
            selected_heading,
            len(md_with_tables),
            md_with_tables,
            base_rec["meta_szakasz"], base_rec["meta_video_statusz"], base_rec["meta_lecke_hossza"],
            base_rec["meta_utolso_modositas"], base_rec["meta_tipus"], base_rec["meta_kurzus"],
            base_rec["meta_vimeo_link"], base_rec["meta_sorszam"]
        ])

        # Riport
        rep_w.writerow([
            os.path.basename(fname),
            page_id,
            title,
            len(video_txt),
            len(lesson_txt),
            selected,
            len(md_with_tables)
        ])

        # ── Tisztított MD készítése meta blokkal a H1 után ──────────────────────
        md_name_base = build_md_filename(title, sorsz_int, page_id, meta.get("kurzus") or "")
        md_name = uniquify_filename(md_name_base, used_names, page_id)

        # Meta címkék megjelenítési sorrendben
        meta_labels = [
            ("Szakasz", "szakasz"),
            ("Videó státusz", "video_statusz"),
            ("Lecke hossza", "lecke_hossza"),
            ("Utolsó módosítás", "utolso_modositas"),
            ("Típus", "tipus"),
            ("Kurzus", "kurzus"),
            ("Vimeo link", "vimeo_link"),
        ]
        meta_lines = []
        for label, key in meta_labels:
            val = (meta.get(key) or "").strip()
            if val:
                meta_lines.append(f"{label}: {val}")

        md_lines = []
        if title:
            md_lines.append(f"# {title}")
        if meta_lines:
            md_lines.append("\n".join(meta_lines))  # meta blokk
        if selected_heading:
            md_lines.append(f"## {selected_heading}")
        if md_with_tables:
            md_lines.append(md_with_tables)
        clean_md_text = "\n\n".join([ln for ln in md_lines if ln]).strip()

        md_zip.writestr(md_name, clean_md_text.encode("utf-8"))

        ok += 1
        pct = idx / max(1, total)  # ← valós feldolgozási előrehaladás
        progress.progress(pct, text=f"{idx}/{total} feldolgozva (✅: {ok}, kihagyva: {skipped})")

    # Zárások és kimenetek előállítása
    md_zip.close()
    clean_md_zip_bytes = md_zip_buf.getvalue()

    jsonl_bytes = (jsonl_buf.getvalue()).encode("utf-8")
    csv_bytes_bom = ("\ufeff" + csv_buf.getvalue()).encode("utf-8")     # BOM
    rep_bytes_bom = ("\ufeff" + rep_buf.getvalue()).encode("utf-8")     # BOM
    tables_jsonl_bytes = (tables_jsonl_buf.getvalue()).encode("utf-8")

    return jsonl_bytes, csv_bytes_bom, rep_bytes_bom, clean_md_zip_bytes, tables_jsonl_bytes


# ────────────────────────────────────────────────────────────────────────────────
# UI
# ────────────────────────────────────────────────────────────────────────────────

with st.expander("Mi ez?"):
    st.markdown(
        "- Tölts fel egy **Notion export ZIP**-et (Markdown & CSV exportból a ZIP-et használd).\n"
        "- A konverter a **„Videó szövege”** (vagy rokon címke) tartalmat vágja ki; ha üres, akkor a **„Lecke szövege”**-t.\n"
        "- A félkövér (**…**) jelölést eltávolítja (kódblokkok érintetlenek).\n"
        "- A táblázatokat (GFM) felismeri és **JSON kivonatot** készít róluk.\n"
        "- **Metaadatok megőrzése**: a *Szakasz, Videó státusz, Lecke hossza, Utolsó módosítás, Típus, Kurzus, Vimeo link* sorok a H1 után bekerülnek a tisztított MD-be.\n"
        "- A tisztított MD fájl **fájlnévének elejére** kerül a **Sorszám** (pl. `20-Cím.md`).\n"
        "- Kimenet: **tisztított MD-k (ajánlott)** + haladó formátumok: JSONL, CSV, riport CSV, táblázatok JSONL.\n"
        "- Opcionális: **chunkolás** átfedéssel (JSONL-hoz)."
    )

st.sidebar.header("Beállítások")
video_labels_str = st.sidebar.text_area(
    "Videó címkék (soronként)",
    value="\n".join(DEFAULT_VIDEO_LABELS),
    height=140
)
lesson_labels_str = st.sidebar.text_area(
    "Lecke címkék (soronként)",
    value="\n".join(DEFAULT_LESSON_LABELS),
    height=120
)
do_chunk = st.sidebar.checkbox("JSONL chunkolása", value=True)
target_chars = st.sidebar.number_input("Chunk célszélesség (karakter)", min_value=1000, max_value=20000, value=5500, step=500)
overlap_chars = st.sidebar.number_input("Chunk átfedés (karakter)", min_value=0, max_value=5000, value=400, step=50)

uploaded = st.file_uploader("Töltsd fel a Notion Markdown ZIP-et", type=["zip"])

if uploaded is not None:
    video_labels = [ln.strip() for ln in video_labels_str.splitlines() if ln.strip()]
    lesson_labels = [ln.strip() for ln in lesson_labels_str.splitlines() if ln.strip()]

    try:
        b = uploaded.read()
        jsonl_bytes, csv_bytes_bom, rep_bytes_bom, md_zip_bytes, tables_jsonl_bytes = convert_zip_to_datasets(
            b, video_labels, lesson_labels, do_chunk, target_chars, overlap_chars
        )
    except zipfile.BadZipFile:
        st.error("Hibás ZIP fájl.")
        st.stop()
    except Exception as e:
        st.error(f"Váratlan hiba: {e}")
        st.stop()

    rid = run_id()

    st.success("Kész! Válaszd ki a letöltést.")

    # Elsődleges letöltés: tisztított MD-k
    st.download_button(
        "⬇️ Tisztított MD-k (ZIP)",
        data=md_zip_bytes,
        file_name=f"clean_md_{rid}.zip",
        mime="application/zip",
        use_container_width=True
    )

    st.divider()

    # ── Másodlagos / haladó formátumok: expanderben ─────────────────────────
    with st.expander("Haladó letöltések (JSONL/CSV/riport/táblázatok/Minden egyben)", expanded=False):
        c1, c2, c3 = st.columns(3)
        with c1:
            st.download_button(
                "⬇️ JSONL (szöveg, RAG/finetune)",
                data=jsonl_bytes,
                file_name=f"output_{rid}.jsonl",
                mime="application/json",
                use_container_width=True
            )
        with c2:
            st.download_button(
                "⬇️ CSV (Excel-barát, BOM)",
                data=csv_bytes_bom,
                file_name=f"output_{rid}.csv",
                mime="text/csv",
                use_container_width=True
            )
        with c3:
            st.download_button(
                "⬇️ Riport CSV",
                data=rep_bytes_bom,
                file_name=f"report_{rid}.csv",
                mime="text/csv",
                use_container_width=True
            )

        st.download_button(
            "⬇️ Táblázatok (JSONL)",
            data=tables_jsonl_bytes,
            file_name=f"tables_{rid}.jsonl",
            mime="application/json",
            use_container_width=True
        )

        # MINDEN EGYBEN ZIP
        with io.BytesIO() as all_buf:
            with zipfile.ZipFile(all_buf, "w", compression=zipfile.ZIP_DEFLATED) as zf:
                zf.writestr(f"output_{rid}.jsonl", jsonl_bytes)
                zf.writestr(f"output_{rid}.csv", csv_bytes_bom)
                zf.writestr(f"report_{rid}.csv", rep_bytes_bom)
                zf.writestr(f"tables_{rid}.jsonl", tables_jsonl_bytes)
                # a tisztított MD ZIP tartalmát al-mappaként bepakoljuk
                with zipfile.ZipFile(io.BytesIO(md_zip_bytes), "r") as mdzf:
                    for info in mdzf.infolist():
                        data = mdzf.read(info.filename)
                        zf.writestr(f"clean_md/{info.filename}", data)
            all_buf.seek(0)

            st.download_button(
                "⬇️ Minden egyben (ZIP)",
                data=all_buf.getvalue(),
                file_name=f"converted_{rid}.zip",
                mime="application/zip",
                use_container_width=True
            )
