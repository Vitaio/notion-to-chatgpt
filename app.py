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
# Helpers: run_id, normalize, slug, Notion fájlnév-ID
# ────────────────────────────────────────────────────────────────────────────────
def run_id() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")

def normalize(s: str) -> str:
    """Ékezet- és írásjel-agnosztikus összehasonlításhoz (csak belső matchinghez)."""
    if s is None:
        return ""
    s = unicodedata.normalize("NFKD", s)
    s = s.encode("ascii", "ignore").decode("ascii")
    s = s.strip().lower()
    s = re.sub(r"[^\w\s]", " ", s)
    s = re.sub(r"\s+", " ", s)
    return s

def slugify(s: str, maxlen: int = 100) -> str:
    s = normalize(s)
    s = s.replace(" ", "_")
    s = re.sub(r"[^a-z0-9_]+", "", s)
    return s[:maxlen] if len(s) > maxlen else s

def extract_page_id_from_filename(name: str) -> Optional[str]:
    """
    Notion export fájlnevek formátuma gyakran: 'Cím abcdef1234567890abcdef1234567890.md'
    Kinyerjük a 32 hex karakteres azonosítót a végéről (kiterjesztés előtt).
    """
    base = os.path.splitext(os.path.basename(name))[0]
    m = re.search(r"([0-9a-fA-F]{32})$", base)
    return m.group(1).lower() if m else None

# ────────────────────────────────────────────────────────────────────────────────
# Markdown → szekciók
# ────────────────────────────────────────────────────────────────────────────────
HEADING_RE = re.compile(r'^(#+)\s+(.*)$')

def split_markdown_sections(md: str) -> List[Tuple[int, str, List[str]]]:
    """
    A Markdown-t heading-alapú szekciókra bontja.
    Visszaad: listája (level, title, lines).
    """
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
    """
    Fuzzy/normalizált egyezés:
    - ékezet, írásjelek elhagyása
    - részleges egyezés: minden tokennek szerepelnie kell a címben (pl. 'video' és 'szoveg')
    """
    tnorm = normalize(title)
    for raw in target_tokens:
        cand = normalize(raw)
        subtoks = cand.split()
        ok = all(tok in tnorm for tok in subtoks if tok)
        if ok:
            return True
    return False

def choose_section(sections: List[Tuple[int, str, List[str]]],
                   video_labels: List[str],
                   lesson_labels: List[str],
                   min_level: int = 2,
                   max_level: int = 4) -> Tuple[str, str, str]:
    """
    Kiválasztja a legjobb szekciót:
    - először 'Videó' címkével egyező heading (H2–H4),
    - ha az üres, akkor 'Lecke',
    - különben üres.
    Vissza: (selected_section: 'video'|'lecke'|'none', text_markdown, selected_heading).
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
# Markdown tisztítás + listák újraszámozása
# ────────────────────────────────────────────────────────────────────────────────
def clean_markdown(md: str) -> str:
    """Kíméletes tisztítás: üres sorok, címsorok előtti üres sor, idézetek stb."""
    if not md:
        return ""
    # ###Cím -> ### Cím
    md = re.sub(r"^(#+)([^\s#])", r"\1 \2", md, flags=re.M)
    # heading/blockquote elé üres sor
    md = re.sub(r"(\n#+\s)", r"\n\n\1", md)
    md = re.sub(r"(\n>\s)", r"\n\n\1", md)
    # 3+ üres sor → 1
    md = re.sub(r"\n{3,}", "\n\n", md)
    # idézet-lista kisimítás
    md = re.sub(r"^>\s-\s", "- ", md, flags=re.M)
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
            out.append(line)
            continue
        if in_code:
            out.append(line)
            continue

        m = num.match(line)
        if m:
            indent = len(m.group(1))
            content = m.group(3)
            if active_indent is None or indent != active_indent:
                active_indent = indent
                # reset mélyebb szintekre
                for k in list(counters.keys()):
                    if k >= indent:
                        del counters[k]
                counters[indent] = 1
            else:
                counters[indent] = counters.get(indent, 0) + 1
            n = counters[indent]
            out.append(" " * indent + f"{n}. " + content)
        else:
            active_indent = None
            out.append(line)

    return "\n".join(out).strip()

# ────────────────────────────────────────────────────────────────────────────────
# JSONL chunkolás
# ────────────────────────────────────────────────────────────────────────────────
def split_by_paragraph(md: str) -> List[str]:
    out = []
    if not md:
        return out
    lines = md.split("\n")
    buf = []
    in_code = False
    for ln in lines:
        if re.match(r"^\s*```", ln):
            in_code = not in_code
        if not in_code and ln.strip() == "":
            if buf:
                out.append("\n".join(buf))
                buf = []
        else:
            buf.append(ln)
    if buf:
        out.append("\n".join(buf))
    return out

def chunk_markdown(md: str, target_chars: int = 5500, overlap_chars: int = 400) -> List[Dict]:
    if not md:
        return [{"text": "", "start": 0, "end": 0}]
    paras = split_by_paragraph(md)
    chunks: List[Dict] = []
    buf: List[str] = []
    size = 0
    start = 0
    for p in paras:
        plen = len(p) + 2
        if size + plen > target_chars and size > 0:
            text = "\n\n".join(buf).strip()
            end = start + len(text)
            chunks.append({"text": text, "start": start, "end": end})
            # overlap
            back = []
            backsize = 0
            for q in reversed(buf):
                qlen = len(q) + 2
                if backsize + qlen > overlap_chars and back:
                    break
                back.append(q)
                backsize += qlen
            buf = list(reversed(back))
            size = sum(len(x) + 2 for x in buf)
            start = end - size
        buf.append(p)
        size += plen
    if buf:
        text = "\n\n".join(buf).strip()
        end = start + len(text)
        chunks.append({"text": text, "start": start, "end": end})
    return chunks

# ────────────────────────────────────────────────────────────────────────────────
# ZIP feldolgozás (UTF-8 + BOM támogatás)
# ────────────────────────────────────────────────────────────────────────────────
def iter_markdown_files(zf: zipfile.ZipFile) -> List[Tuple[str, str]]:
    """
    Bejárja a ZIP-et, és visszaadja a (arcname, text) listát .md fájlokra.
    Dekódolás: 'utf-8-sig' → eltávolítja a BOM-ot, ha jelen van.
    """
    md_items: List[Tuple[str, str]] = []
    for info in zf.infolist():
        if info.is_dir():
            continue
        if not info.filename.lower().endswith(".md"):
            continue
        with zf.open(info, "r") as f:
            b = f.read()
        try:
            # UTF-8 BOM eltávolítása, ha van
            s = b.decode("utf-8-sig")
        except UnicodeDecodeError:
            s = b.decode("utf-8", errors="replace")
        md_items.append((info.filename, s))
    return md_items

def extract_page_title(md: str, fallback: str) -> str:
    # Első H1 cím (# ...)
    for line in (md or "").splitlines():
        m = HEADING_RE.match(line)
        if m and len(m.group(1)) == 1:
            return m.group(2).strip()
    return fallback

# ────────────────────────────────────────────────────────────────────────────────
# Konverzió (fő logika) – BOM-os CSV, BOM nélküli JSONL
# ────────────────────────────────────────────────────────────────────────────────
def convert_zip_to_datasets(
    zip_bytes: bytes,
    video_labels: List[str],
    lesson_labels: List[str],
    chunk: bool,
    target_chars: int,
    overlap_chars: int
) -> Tuple[bytes, bytes, bytes]:
    """
    Visszaad: (jsonl_bytes, csv_bytes_with_bom, report_csv_bytes_with_bom)
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

    total = len(md_files)
    ok = 0
    progress = st.progress(0.0, text=f"0/{total} feldolgozva")

    for idx, (fname, text) in enumerate(md_files, start=1):
        page_id = extract_page_id_from_filename(fname) or ""
        title = extract_page_title(text, fallback=os.path.splitext(os.path.basename(fname))[0])

        # Szétbontás szekciókra
        sections = split_markdown_sections(text)

        # Mérjük a két fő szekciót is (riport)
        video_txt = ""
        lesson_txt = ""
        for level, heading, lines in sections:
            if 2 <= level <= 4:
                if label_match(heading, video_labels):
                    video_txt = "\n".join(lines).strip()
                if label_match(heading, lesson_labels):
                    lesson_txt = "\n".join(lines).strip()

        # Választás szabály szerint
        selected, raw, selected_heading = choose_section(sections, video_labels, lesson_labels, 2, 4)

        # Tisztítás
        cleaned = renumber_ordered_lists(clean_markdown(raw))

        # JSONL / CSV írás
        if chunk:
            parts = chunk_markdown(cleaned, target_chars, overlap_chars)
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
                "text_markdown": cleaned,
                "char_len": len(cleaned),
            }
            jsonl_buf.write(json.dumps(rec, ensure_ascii=False) + "\n")

        csv_w.writerow([
            os.path.basename(fname),
            page_id,
            title,
            selected,
            selected_heading,
            len(cleaned),
            cleaned
        ])

        rep_w.writerow([
            os.path.basename(fname),
            page_id,
            title,
            len(video_txt),
            len(lesson_txt),
            selected,
            len(cleaned)
        ])

        ok += 1
        pct = ok / max(1, total)
        progress.progress(pct, text=f"{ok}/{total} feldolgozva")

    # ── Kimenetek: CSV-k BOM-mal, JSONL BOM nélkül
    jsonl_bytes = jsonl_buf.getvalue().encode("utf-8")

    csv_text = csv_buf.getvalue()
    rep_text = rep_buf.getvalue()

    # UTF-8 BOM hozzáadása (Excel-barát)
    csv_bytes = ("\ufeff" + csv_text).encode("utf-8")
    rep_bytes = ("\ufeff" + rep_text).encode("utf-8")

    return jsonl_bytes, csv_bytes, rep_bytes

# ────────────────────────────────────────────────────────────────────────────────
# UI
# ────────────────────────────────────────────────────────────────────────────────
st.title("🧩 Notion Markdown → ChatGPT (JSONL/CSV) konverter")
st.caption("Duplikációk kizárása: videó szövege → ha üres, lecke szövege → más szekciók kihagyása.")

with st.expander("Mi ez?"):
    st.markdown(
        "- Tölts fel egy **Notion export ZIP**-et (Markdown & CSV exportból a ZIP-et használd).\n"
        "- A konverter a **„Videó szövege”** (vagy rokon címke) tartalmat vágja ki; ha üres, akkor a **„Lecke szövege”**-t.\n"
        "- Kimenet: **JSONL** (Custom GPT / RAG), **CSV**, és egy **ellenőrző riport**.\n"
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
        jsonl_bytes, csv_bytes, rep_bytes = convert_zip_to_datasets(
            uploaded.read(), vlabels, llabels, chunk, int(target_chars), int(overlap_chars)
        )

        rid = run_id()

        # ZIP csomag az outputokról: a BOM-os CSV-k kerülnek bele
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_DEFLATED) as zf:
            zf.writestr("output.jsonl", jsonl_bytes)
            zf.writestr("output.csv", csv_bytes)     # BOM-os
            zf.writestr("report.csv", rep_bytes)     # BOM-os
        buf.seek(0)
        elapsed = int(time.time() - t0)

        st.success(f"Kész! ({elapsed} mp)")
        col1, col2 = st.columns(2)
        with col1:
            st.download_button(
                "⬇️ Letöltés – JSONL",
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
                "⬇️ Letöltés – Minden egyben (ZIP)",
                data=buf.getvalue(),
                file_name=f"converted_{rid}.zip",
                mime="application/zip"
            )
