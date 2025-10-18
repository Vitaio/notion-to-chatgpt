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
st.set_page_config(page_title="Notion MD → ChatGPT", page_icon="🧩", layout="wide")

# ────────────────────────────────────────────────────────────────────────────────
# Helpers: run_id, normalize (csak belső matchinghez), slug, safe filename
# ────────────────────────────────────────────────────────────────────────────────
def run_id() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")

def normalize(s: str) -> str:
    """Ékezet/írásjel-agnosztikus összehasonlításhoz (belső matching)."""
    if s is None:
        return ""
    s = unicodedata.normalize("NFKD", s)
    s = s.encode("ascii", "ignore").decode("ascii")
    s = s.strip().lower()
    s = re.sub(r"[^\w\s]", " ", s)
    s = re.sub(r"\s+", " ", s)
    return s

def slugify(s: str, maxlen: int = 100) -> str:
    s = normalize(s).replace(" ", "_")
    s = re.sub(r"[^a-z0-9_]+", "", s)
    return s[:maxlen] if len(s) > maxlen else s

def safe_filename(title: str, page_id: Optional[str] = None, maxlen: int = 140) -> str:
    """Ékezeteket meghagyjuk; csak tiltott fájlneveket cserélünk."""
    base = title.strip() if title else "untitled"
    base = re.sub(r'[\\/:*?"<>|]+', "_", base)
    base = re.sub(r"\s+", " ", base).strip()
    if page_id:
        base = f"{base} {page_id}"
    if len(base) > maxlen:
        base = base[:maxlen].rstrip()
    if not base:
        base = page_id or "untitled"
    return base + ".md"

def extract_page_id_from_filename(name: str) -> Optional[str]:
    """'Cím abcdef1234567890abcdef1234567890.md' → 32 hex azonosító"""
    base = os.path.splitext(os.path.basename(name))[0]
    m = re.search(r"([0-9a-fA-F]{32})$", base)
    return m.group(1).lower() if m else None

# ────────────────────────────────────────────────────────────────────────────────
# Markdown → szekciók
# ────────────────────────────────────────────────────────────────────────────────
HEADING_RE = re.compile(r'^(#+)\s+(.*)$')

def split_markdown_sections(md: str) -> List[Tuple[int, str, List[str]]]:
    """A Markdown-t heading-alapú szekciókra bontja. Vissza: (level, title, lines)."""
    lines = (md or "").splitlines()
    sections: List[Tuple[int, str, List[str]]] = []
    current_level = None
    current_title = None
    current_buf: List[str] = []

    def flush():
        nonlocal current_level, current_title, current_buf
        if current_title is not None:
            sections.append((current_level or 0, current_title, current_buf))
        current_level, current_title, current_buf = None, None, []

    for ln in lines:
        m = HEADING_RE.match(ln)
        if m:
            flush()
            level = len(m.group(1))
            title = m.group(2).strip()
            current_level, current_title = level, title
            current_buf = []
        else:
            if current_title is None:
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
    "videó szöveg", "video szoveg", "videó leirat", "video leirat",
    "transcript", "videó", "video",
]
DEFAULT_LESSON_LABELS = [
    "lecke szöveg", "lecke anyag", "leckeszöveg", "tananyag",
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

def choose_section(
    sections: List[Tuple[int, str, List[str]]],
    video_labels: List[str],
    lesson_labels: List[str],
    min_level: int = 2,
    max_level: int = 4
) -> Tuple[str, str, str]:
    """
    Kiválasztás szabály szerint:
      1) Videó-címkés H2–H4 nem üres → 'video'
      2) különben Lecke-címkés H2–H4 nem üres → 'lecke'
      3) különben 'none'
    Vissza: (selected_section, text_markdown, selected_heading).
    """
    candidates: Dict[str, List[Tuple[int,str,List[str]]]] = {"video": [], "lecke": []}
    for level, title, lines in sections:
        if min_level <= level <= max_level:
            if label_match(title, video_labels):
                candidates["video"].append((level, title, lines))
            elif label_match(title, lesson_labels):
                candidates["lecke"].append((level, title, lines))

    def text_of(sec) -> str:
        return "\n".join(sec[2]).strip()

    for sec in candidates["video"]:
        txt = text_of(sec)
        if txt:
            return "video", txt, sec[1]
    for sec in candidates["lecke"]:
        txt = text_of(sec)
        if txt:
            return "lecke", txt, sec[1]
    return "none", "", ""

# ────────────────────────────────────────────────────────────────────────────────
# Markdown tisztítás + listák újraszámozása + félkövér eltávolítás
# ────────────────────────────────────────────────────────────────────────────────
def clean_markdown(md: str) -> str:
    """Kíméletes tisztítás: üres sorok, címsorok előtti üres sor, idézetek stb."""
    if not md:
        return ""
    md = re.sub(r"^(#+)([^\s#])", r"\1 \2", md, flags=re.M)     # ###Cím -> ### Cím
    md = re.sub(r"(\n#+\s)", r"\n\n\1", md)                     # heading elé üres sor
    md = re.sub(r"(\n>\s)", r"\n\n\1", md)                      # idézet elé üres sor
    md = re.sub(r"\n{3,}", "\n\n", md)                          # 3+ üres sor → 1
    md = re.sub(r"^>\s-\s", "- ", md, flags=re.M)               # idézet-lista kisimítás
    return md.strip()

def renumber_ordered_lists(md: str) -> str:
    """Számozott listák újraszámozása kódblokkokon kívül."""
    if not md:
        return ""
    lines = md.splitlines()
    out: List[str] = []
    in_code = False
    fence = re.compile(r"^\s*```")
    num = re.compile(r"^(\s*)(\d+)\.\s+(.*)$")
    counters: Dict[int, int] = {}
    active_indent: Optional[int] = None
    for line in lines:
        if fence.match(line):
            in_code = not in_code
            out.append(line); continue
        if in_code:
            out.append(line); continue
        m = num.match(line)
        if m:
            indent = len(m.group(1))
            content = m.group(3)
            if active_indent is None or indent != active_indent:
                active_indent = indent
                for k in list(counters.keys()):
                    if k >= indent: del counters[k]
                counters[indent] = 1
            else:
                counters[indent] = counters.get(indent, 0) + 1
            out.append(" " * indent + f"{counters[indent]}. " + content)
        else:
            active_indent = None
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
            parts[i] = part
        out.append("".join(parts))
    return "\n".join(out).strip()

# ────────────────────────────────────────────────────────────────────────────────
# Táblázat detektálás és JSON-kivonat
# ────────────────────────────────────────────────────────────────────────────────
ALIGN_RE = re.compile(r'^\s*\|?(?:\s*:?-{3,}:?\s*\|)+\s*:?-{3,}:?\s*\|?\s*$')

def _split_md_row(line: str) -> List[str]:
    """Biztonságos cella-szétválasztó: figyel az escape-elt '|' és az inline code-ra."""
    cells, buf = [], []
    in_code = False
    esc = False
    for ch in line.strip():
        if esc:
            buf.append(ch); esc = False; continue
        if ch == '\\':
            esc = True; continue
        if ch == '`':
            in_code = not in_code
            buf.append(ch); continue
        if ch == '|' and not in_code:
            cells.append("".join(buf).strip()); buf = []; continue
        buf.append(ch)
    cells.append("".join(buf).strip())
    # Távolítsuk el a kezdő/záró pipe miatti üres elemeket
    if cells and cells[0] == "": cells = cells[1:]
    if cells and cells[-1] == "": cells = cells[:-1]
    return cells

def _normalize_key(s: str) -> str:
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")
    s = s.strip().lower()
    s = re.sub(r"[^\w]+", "_", s).strip("_")
    return s or "col"

def extract_tables(md: str) -> Tuple[str, List[Dict]]:
    """
    Kinyeri a GFM táblákat a markdownból és JSON-osítja.
    Vissza: (md_with_tables_json_section, tables_list)
      - md_with_tables_json_section: az eredeti (tisztított) md + "Adattáblák (gépi kivonat)" szekció kódblokkokkal
      - tables_list: [{headers:[..], rows:[{..},..]}, ...]
    """
    if not md:
        return md, []

    lines = md.splitlines()
    tables = []
    i = 0
    used_sections = []
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
                # félkövér tisztítás a cellákban
                clean_headers = [strip_bold_emphasis(h) for h in headers]
                # normalizált kulcsok, ütközések kezelése
                keys = []
                seen = set()
                for h in clean_headers:
                    k = _normalize_key(h)
                    if k in seen:
                        c = 2
                        while f"{k}_{c}" in seen:
                            c += 1
                        k = f"{k}_{c}"
                    keys.append(k); seen.add(k)
                row_objs = []
                for r in rows:
                    # kiegyenlítés (rövidebb/hosszabb sorok)
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
                used_sections.append((i, j - 1))
                i = j
                continue
        i += 1

    if not tables:
        return md, []

    # Az MD végére illesztünk egy összefoglaló szekciót JSON-okkal
    out = [md, "", "## Adattáblák (gépi kivonat)", ""]
    for idx, t in enumerate(tables, start=1):
        out.append(f"### Táblázat {idx}")
        out.append("```json")
        out.append(json.dumps({"headers": t["headers"], "rows": t["rows"]}, ensure_ascii=False, indent=2))
        out.append("```")
        out.append("")
    return "\n".join(out).strip(), tables

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
    """
    Bejárja a ZIP-et, (arcname, text) listát ad .md fájlokra.
    Tartalom dekódolás: 'utf-8-sig' → eltávolítja a BOM-ot.
    """
    md_items: List[Tuple[str, str]] = []
    for info in zf.infolist():
        if info.is_dir():
            continue
        if not info.filename.lower().endswith(".md"):
            continue
        fixed_name = _fixed_zip_filename(info)
        with zf.open(info, "r") as f:
            b = f.read()
        try:
            s = b.decode("utf-8-sig")
        except UnicodeDecodeError:
            s = b.decode("utf-8", errors="replace")
        md_items.append((fixed_name, s))
    return md_items

def extract_page_title(md: str, fallback: str) -> str:
    # Első H1 cím (# ...)
    for line in (md or "").splitlines():
        m = HEADING_RE.match(line)
        if m and len(m.group(1)) == 1:
            return m.group(2).strip()
    return fallback

# ────────────────────────────────────────────────────────────────────────────────
# Konverzió (fő logika) – JSONL + CSV + Report + Clean MD + Tables JSONL
# ────────────────────────────────────────────────────────────────────────────────
def convert_zip_to_datasets(
    zip_bytes: bytes,
    video_labels: List[str],
    lesson_labels: List[str],
    chunk: bool,
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

    csv_w.writerow(["file_name", "page_id", "page_title", "selected_section", "selected_heading", "char_len", "tartalom"])
    rep_w.writerow(["file_name", "page_id", "page_title", "video_len", "lesson_len", "selected", "selected_len"])

    # Tisztított MD-k külön ZIP-be
    md_zip_buf = io.BytesIO()
    md_zip = zipfile.ZipFile(md_zip_buf, "w", compression=zipfile.ZIP_DEFLATED)

    # Táblázatok külön JSONL-be (összes dokumentum)
    tables_jsonl_buf = io.StringIO()

    total = len(md_files)
    ok = 0
    progress = st.progress(0.0, text=f"0/{total} feldolgozva")

    for idx, (fname, text) in enumerate(md_files, start=1):
        page_id = extract_page_id_from_filename(fname) or ""
        title = extract_page_title(text, fallback=os.path.splitext(os.path.basename(fname))[0])

        # Szétbontás szekciókra és választás
        sections = split_markdown_sections(text)
        video_txt = ""; lesson_txt = ""
        for level, heading, lines in sections:
            if 2 <= level <= 4:
                if label_match(heading, video_labels):  video_txt = "\n".join(lines).strip()
                if label_match(heading, lesson_labels): lesson_txt = "\n".join(lines).strip()

        selected, raw, selected_heading = choose_section(sections, video_labels, lesson_labels, 2, 4)

        # Tisztítás + listák + félkövér eltávolítás
        cleaned = renumber_ordered_lists(clean_markdown(raw))
        cleaned = strip_bold_emphasis(cleaned)

        # Táblák kinyerése és MD-hez csatolása gépi kivonattal
        md_with_tables, tables = extract_tables(cleaned)

        # JSONL / CSV írás
        if chunk:
            parts = chunk_markdown(md_with_tables, target_chars, overlap_chars)
            for i, ch in enumerate(parts, start=1):
                rec = {
                    "run_id": rid,
                    "doc_id": f"{slugify(title)}_{(page_id or 'noid')}",
                    "page_id": page_id,
                    "file_name": os.path.basename(fname),
                    "page_title": title,
                    "selected_section": selected,
                    "selected_heading": selected_heading,
                    "chunk_index": i,
                    "text_markdown": ch["text"],
                    "char_len": len(ch["text"]),
                }
                jsonl_buf.write(json.dumps(rec, ensure_ascii=False) + "\n")
        else:
            rec = {
                "run_id": rid,
                "doc_id": f"{slugify(title)}_{(page_id or 'noid')}",
                "page_id": page_id,
                "file_name": os.path.basename(fname),
                "page_title": title,
                "selected_section": selected,
                "selected_heading": selected_heading,
                "text_markdown": md_with_tables,   # a táblák gépi kivonata is benne a végén
                "char_len": len(md_with_tables),
            }
            jsonl_buf.write(json.dumps(rec, ensure_ascii=False) + "\n")

        csv_w.writerow([
            os.path.basename(fname),
            page_id,
            title,
            selected,
            selected_heading,
            len(md_with_tables),
            md_with_tables
        ])

        rep_w.writerow([
            os.path.basename(fname),
            page_id,
            title,
            len(video_txt),
            len(lesson_txt),
            selected,
            len(md_with_tables)
        ])

        # Tisztított MD fájl (emberbarát + végén gépi kivonat)
        md_name = safe_filename(title, page_id=page_id)
        md_lines = []
        if title: md_lines.append(f"# {title}")
        if selected_heading: md_lines.append(f"## {selected_heading}")
        if md_with_tables.strip(): md_lines.append(md_with_tables.strip())
        md_content = "\n\n".join(md_lines).strip() + "\n"
        md_zip.writestr(f"{md_name}", md_content.encode("utf-8"))

        # Táblázatok összegyűjtése JSONL-be
        for t_index, t in enumerate(tables, start=1):
            tables_jsonl_buf.write(json.dumps({
                "run_id": rid,
                "doc_id": f"{slugify(title)}_{(page_id or 'noid')}",
                "page_id": page_id,
                "file_name": os.path.basename(fname),
                "page_title": title,
                "selected_section": selected,
                "selected_heading": selected_heading,
                "table_index": t_index,
                "headers": t["headers"],
                "rows": t["rows"]
            }, ensure_ascii=False) + "\n")

        ok += 1
        pct = ok / max(1, total)
        progress.progress(pct, text=f"{ok}/{total} feldolgozva")

    # Zárások és kimenetek előállítása
    md_zip.close()
    clean_md_zip_bytes = md_zip_buf.getvalue()

    jsonl_bytes = jsonl_buf.getvalue().encode("utf-8")
    tables_jsonl_bytes = tables_jsonl_buf.getvalue().encode("utf-8")

    csv_text = csv_buf.getvalue()
    rep_text = rep_buf.getvalue()

    # UTF-8 BOM a CSV-khez (Excel)
    csv_bytes = ("\ufeff" + csv_text).encode("utf-8")
    rep_bytes = ("\ufeff" + rep_text).encode("utf-8")

    return jsonl_bytes, csv_bytes, rep_bytes, clean_md_zip_bytes, tables_jsonl_bytes

# ────────────────────────────────────────────────────────────────────────────────
# UI
# ────────────────────────────────────────────────────────────────────────────────
st.title("🧩 Notion Markdown → ChatGPT (JSONL/CSV/MD) konverter")
st.caption("Duplikációk kizárása (Videó→Lecke), félkövér tisztítás, táblázatok gépi kivonata. UTF-8/ékezet, CSV BOM-mal.")

with st.expander("Mi ez?"):
    st.markdown(
        "- Tölts fel egy **Notion export ZIP**-et (Markdown & CSV exportból a ZIP-et használd).\n"
        "- A konverter a **„Videó szövege”** (vagy rokon címke) tartalmat vágja ki; ha üres, akkor a **„Lecke szövege”**-t.\n"
        "- A félkövér (**…**) jelölést **eltávolítja** a jobb gépi feldolgozhatóságért.\n"
        "- A táblázatokat (GFM) felismeri és **JSON kivonatot** is készít róluk.\n"
        "- Kimenet: **JSONL** (szöveg), **CSV**, **riport CSV**, **tisztított MD-k (ZIP)**, **táblázatok (JSONL)**.\n"
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
chunk = st.sidebar.checkbox("JSONL chunkolása", value=True)
target_chars = st.sidebar.number_input("Chunk célszélesség (karakter)", min_value=1000, max_value=20000, value=5500, step=500)
overlap_chars = st.sidebar.number_input("Chunk átfedés (karakter)", min_value=0, max_value=5000, value=400, step=50)

uploaded = st.file_uploader("Töltsd fel a Notion Markdown ZIP-et", type=["zip"])

if uploaded is not None:
    st.info("ZIP betöltve. Állítsd be a címkéket/paramétereket, majd indítsd a konvertálást.")
    start = st.button("Konvertálás indítása", type="primary", use_container_width=True)

    if start:
        vlabels = [x.strip() for x in video_labels_str.splitlines() if x.strip()]
        llabels = [x.strip() for x in lesson_labels_str.splitlines() if x.strip()]

        t0 = time.time()
        (jsonl_bytes, csv_bytes, rep_bytes,
         md_zip_bytes, tables_jsonl_bytes) = convert_zip_to_datasets(
            uploaded.read(), vlabels, llabels, chunk, int(target_chars), int(overlap_chars)
        )

        rid = run_id()

        # Minden egyben (ZIP): JSONL + CSV-k + CLEAN MD-k + táblázatok JSONL
        all_buf = io.BytesIO()
        with zipfile.ZipFile(all_buf, "w", compression=zipfile.ZIP_DEFLATED) as zf:
            zf.writestr("output.jsonl", jsonl_bytes)
            zf.writestr("output.csv", csv_bytes)         # BOM-os
            zf.writestr("report.csv", rep_bytes)         # BOM-os
            zf.writestr("tables.jsonl", tables_jsonl_bytes)
            # a tisztított MD ZIP tartalmát al-mappaként bepakoljuk
            with zipfile.ZipFile(io.BytesIO(md_zip_bytes), "r") as mdzf:
                for info in mdzf.infolist():
                    data = mdzf.read(info.filename)
                    zf.writestr(f"clean_md/{info.filename}", data)
        all_buf.seek(0)

        elapsed = int(time.time() - t0)
        st.success(f"Kész! ({elapsed} mp)")

        col1, col2, col3 = st.columns(3)
        with col1:
            st.download_button(
                "⬇️ Letöltés – JSONL (szöveg)",
                data=jsonl_bytes,
                file_name=f"output_{rid}.jsonl",
                mime="application/json"
            )
            st.download_button(
                "⬇️ Letöltés – CSV",
                data=csv_bytes,
                file_name=f"output_{rid}.csv",
                mime="text/csv; charset=utf-8"
            )
        with col2:
            st.download_button(
                "⬇️ Letöltés – Riport CSV",
                data=rep_bytes,
                file_name=f"report_{rid}.csv",
                mime="text/csv; charset=utf-8"
            )
            st.download_button(
                "⬇️ Letöltés – Táblázatok (JSONL)",
                data=tables_jsonl_bytes,
                file_name=f"tables_{rid}.jsonl",
                mime="application/json"
            )
        with col3:
            st.download_button(
                "⬇️ Letöltés – Tisztított MD-k (ZIP)",
                data=md_zip_bytes,
                file_name=f"clean_md_{rid}.zip",
                mime="application/zip"
            )
            st.download_button(
                "⬇️ Letöltés – Minden egyben (ZIP)",
                data=all_buf.getvalue(),
                file_name=f"converted_{rid}.zip",
                mime="application/zip"
            )
