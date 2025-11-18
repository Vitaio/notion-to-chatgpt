 (cd "$(git rev-parse --show-toplevel)" && git apply --3way <<'EOF' 
diff --git a/app.py b/app.py
index 47bfb2c0d6c85c8da05b9151734c749565b65a87..f4ff4407b7a86baf1b284b565f83c4718cdeb1d5 100644
--- a/app.py
+++ b/app.py
@@ -1,50 +1,55 @@
 import io
 import os
 import re
 import csv
 import json
 import time
+import html
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
-st.caption("Notion Markdown exportból kinyeri a **Videó/Lecke** szöveget (PONTOS H2 egyezéssel), tisztít, chunkol (opcionális), és táblázat-kivonatot készít.")
+st.caption(
+    "Notion Markdown exportból kinyeri az összes **Videó szöveg** lenyíló blokk tartalmát,"
+    " látványosabb, átláthatóbb MD-t készít (címsorok/listák rendezése), opcionálisan chunkol,"
+    " és táblázat-kivonatot készít."
+)
 
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
 
 
@@ -122,78 +127,145 @@ def split_markdown_sections(md: str) -> List[Tuple[int, str, List[str]]]:
 
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
 # PONTOS H2-egyezéshez szükséges konstansok/függvények
 # ────────────────────────────────────────────────────────────────────────────────
 
 EXACT_VIDEO_HEADING = "Videó szöveg"
-EXACT_LESSON_HEADING = "Lecke szöveg"
-_H2_ANY = re.compile(r"^##\s+.+$", flags=re.MULTILINE)
+_DETAILS_RE = re.compile(
+    r"<details\b[^>]*>\s*(.*?)</details\s*>", flags=re.DOTALL | re.IGNORECASE
+)
+_SUMMARY_RE = re.compile(
+    r"<summary\b[^>]*>\s*(.*?)</summary\s*>", flags=re.DOTALL | re.IGNORECASE
+)
+
 
-def _extract_section_exact_h2(md: str, heading: str) -> str:
+def _html_to_markdownish(fragment: str) -> str:
     """
-    Csak a PONTOSAN '## <heading>' címsor alatti tartalmat adja vissza a következő H2-ig.
-    Ha nincs ilyen címsor vagy nincs érdemi tartalom, üres stringet ad vissza.
+    Egyszerű HTML→Markdown-szerű átalakítás a toggle-blokkokhoz, hogy a sortörések,
+    címsorok és listák olvashatóbbak legyenek.
     """
-    md = md or ""
-    m = re.search(rf"^##\s*{re.escape(heading)}\s*$", md, flags=re.MULTILINE)
-    if not m:
+    if not fragment:
+        return ""
+
+    txt = fragment
+    replacements = [
+        (r"<br\s*/?>", "\n"),
+        (r"</p\s*>", "\n\n"),
+        (r"<p[^>]*>", ""),
+        (r"</li\s*>", "\n"),
+        (r"<li[^>]*>", "- "),
+        (r"</(ul|ol)\s*>", "\n"),
+        (r"<(ul|ol)[^>]*>", ""),
+        (r"<h1[^>]*>(.*?)</h1\s*>", r"# \1\n\n"),
+        (r"<h2[^>]*>(.*?)</h2\s*>", r"## \1\n\n"),
+        (r"<h3[^>]*>(.*?)</h3\s*>", r"### \1\n\n"),
+        (r"<h4[^>]*>(.*?)</h4\s*>", r"#### \1\n\n"),
+        (r"<h5[^>]*>(.*?)</h5\s*>", r"##### \1\n\n"),
+        (r"<h6[^>]*>(.*?)</h6\s*>", r"###### \1\n\n"),
+    ]
+    for pat, repl in replacements:
+        txt = re.sub(pat, repl, txt, flags=re.IGNORECASE)
+
+    # minden más HTML tag eltávolítása, entitások feloldása
+    txt = re.sub(r"<[^>]+>", "", txt)
+    txt = html.unescape(txt)
+
+    lines = [ln.rstrip() for ln in txt.splitlines()]
+    while lines and not lines[0].strip():
+        lines.pop(0)
+    while lines and not lines[-1].strip():
+        lines.pop()
+    return "\n".join(lines).strip()
+
+def _extract_video_toggle(md: str) -> str:
+    """
+    Kizárólag a 'Videó szöveg' feliratú lenyíló (toggle) blokk(ok) tartalmát adja vissza.
+    - tolerálja a <details> és </summary> körüli whitespace-et
+    - a summary HTML-je normalizálva hasonlít, így a díszítő tagek sem zavarják
+    - blockquote / behúzott toggle is működik (a sor eleji '>' és whitespace lecsupaszításával)
+    - a tartalom HTML-ből Markdown-szerűre konvertálva kerül vissza,
+      hogy a címsorok, felsorolások, sortörések megmaradjanak
+    """
+    if not md:
+        return ""
+
+    # Ha a toggle blockquote-ban/behúzva áll, pucoljuk le a sor elejéről a díszítést
+    normalized_md = "\n".join(line.lstrip(" >\t") for line in md.splitlines())
+
+    parts = []
+
+    for details_match in _DETAILS_RE.finditer(normalized_md):
+        block = details_match.group(1)
+        summary_match = _SUMMARY_RE.search(block)
+        if not summary_match:
+            continue
+
+        summary_text = _html_to_markdownish(summary_match.group(1))
+        if normalize(summary_text) != normalize(EXACT_VIDEO_HEADING):
+            continue
+
+        content_html = block[summary_match.end():]
+        content_md = _html_to_markdownish(content_html)
+
+        # ha a konverzió üres lenne (pl. csak tagek), essünk vissza a nyers, tag-mentesített tartalomra
+        if not content_md:
+            content_md = html.unescape(re.sub(r"<[^>]+>", "", content_html)).strip()
+
+        if content_md:
+            parts.append(content_md)
+
+    if not parts:
         return ""
-    start = m.end()
-    m2 = _H2_ANY.search(md, pos=start)
-    end = m2.start() if m2 else len(md)
-    return md[start:end].strip()
+    return "\n\n".join(parts)
 
 def choose_section_exact(md: str) -> Tuple[str, str, str]:
     """
-    Prioritás: Videó szöveg > Lecke szöveg; egyik sincs → none.
+    Csak a 'Videó szöveg' lenyíló blokk tartalmát választja ki.
     Vissza: (selected_section, raw_text, selected_heading)
     """
-    video = _extract_section_exact_h2(md, EXACT_VIDEO_HEADING)
-    lesson = _extract_section_exact_h2(md, EXACT_LESSON_HEADING)
+    video = _extract_video_toggle(md)
     if video:
         return "video", video, EXACT_VIDEO_HEADING
-    if lesson:
-        return "lecke", lesson, EXACT_LESSON_HEADING
     return "none", "", ""
 
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
@@ -233,50 +305,105 @@ def renumber_ordered_lists(md: str) -> str:
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
     return "\n".Join(out).strip() if False else "\n".join(out).strip()  # védőhack: ne töröld a sort
 
+
+def enhance_readability(md: str) -> str:
+    """
+    Egyszerűsített formázás a jobb áttekinthetőséghez:
+    - egységes "- " jelölés a felsorolásoknál,
+    - üres sor beillesztése listák és címsorok elé,
+    - a címsorok után egy üres sort hagy, hogy elkülönüljenek.
+    """
+    if not md:
+        return ""
+
+    lines = md.splitlines()
+    out: List[str] = []
+
+    ul_re = re.compile(r"^(\s*)[-*+]\s+(.*)$")
+    ol_re = re.compile(r"^(\s*)\d+\.\s+(.*)$")
+
+    for i, line in enumerate(lines):
+        heading = HEADING_RE.match(line)
+        ul = ul_re.match(line)
+        ol = ol_re.match(line)
+
+        if heading:
+            if out and out[-1] != "":
+                out.append("")
+            out.append(line.rstrip())
+            out.append("")
+            continue
+
+        if ul:
+            indent, rest = ul.groups()
+            if out and out[-1] != "":
+                out.append("")
+            out.append(f"{indent}- {rest.strip()}")
+            continue
+
+        if ol:
+            indent, rest = ol.groups()
+            if out and out[-1] != "":
+                out.append("")
+            out.append(f"{indent}1. {rest.strip()}")
+            continue
+
+        if line.strip() == "":
+            if out and out[-1] == "":
+                continue
+            out.append("")
+        else:
+            out.append(line.rstrip())
+
+    while out and out[-1] == "":
+        out.pop()
+
+    return "\n".join(out)
+
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
@@ -580,60 +707,60 @@ def convert_zip_to_datasets(
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
 
-        # PONTOS H2 egyezés (csak a két fix cím engedélyezett)
-        video_txt  = _extract_section_exact_h2(text, EXACT_VIDEO_HEADING)
-        lesson_txt = _extract_section_exact_h2(text, EXACT_LESSON_HEADING)
+        # Lenyíló (toggle) Videó szöveg blokk kinyerése
+        video_txt = _extract_video_toggle(text)
 
-        # Kiválasztás prioritással
+        # Kiválasztás: csak a lenyíló Videó szöveg tartalma számít
         selected, raw, selected_heading = choose_section_exact(text)
 
         # tisztítás
         raw_clean = strip_bold_emphasis(raw)
         raw_clean = clean_markdown(raw_clean)
+        raw_clean = enhance_readability(raw_clean)
         raw_clean = renumber_ordered_lists(raw_clean)
 
         # táblázatok kivonata csak a kiválasztott szövegből
         md_with_tables, tables = extract_tables(raw_clean if raw_clean else "")
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
@@ -663,51 +790,51 @@ def convert_zip_to_datasets(
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
-            len(lesson_txt),
+            0,
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
@@ -726,53 +853,53 @@ def convert_zip_to_datasets(
         md_zip.writestr(md_name, clean_md_text.encode("utf-8"))
 
         ok += 1
         pct = idx / max(1, total)
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
-        "- A konverter **PONTOS egyezéssel** csak a `## Videó szöveg` vagy, ha az üres/hiányzik, a `## Lecke szöveg` szakaszt veszi ki.\n"
-        "- Ha egyik sincs, a kimenet: _Ehhez a leckéhez nem készült leírás._\n"
-        "- A félkövér (**…**) jelölést eltávolítja (kódblokkok érintetlenek).\n"
+        "- A konverter az összes `Videó szöveg` lenyíló (toggle) blokk teljes tartalmát veszi ki.\n"
+        "- Ha nincs ilyen lenyíló blokk, a kimenet: _Ehhez a leckéhez nem készült leírás._\n"
+        "- A félkövér (**…**) jelölést eltávolítja (kódblokkok érintetlenek), a címsorokat és listákat jobban tagolja az olvashatóságért.\n"
         "- A táblázatokat (GFM) felismeri és **JSON kivonatot** készít róluk.\n"
         "- **Metaadatok megőrzése**: a *Szakasz, Videó státusz, Lecke hossza, Utolsó módosítás, Típus, Kurzus, Vimeo link* sorok a H1 után bekerülnek a tisztított MD-be.\n"
         "- A tisztított MD fájlnév sémája: `Kurzus - Sorszám - Név.md`.\n"
         "- Kimenet: **tisztított MD-k (ajánlott)** + haladó formátumok: JSONL, CSV, riport CSV, táblázatok JSONL.\n"
         "- Opcionális: **chunkolás** átfedéssel (JSONL-hoz)."
     )
 
 st.sidebar.header("Beállítások")
 do_chunk = st.sidebar.checkbox("JSONL chunkolása", value=True)
 target_chars = st.sidebar.number_input("Chunk célszélesség (karakter)", min_value=1000, max_value=20000, value=5500, step=500)
 overlap_chars = st.sidebar.number_input("Chunk átfedés (karakter)", min_value=0, max_value=5000, value=400, step=50)
 
 uploaded = st.file_uploader("Töltsd fel a Notion Markdown ZIP-et", type=["zip"])
 
 if uploaded is not None:
     try:
         b = uploaded.read()
         jsonl_bytes, csv_bytes_bom, rep_bytes_bom, md_zip_bytes, tables_jsonl_bytes = convert_zip_to_datasets(
             b, do_chunk, target_chars, overlap_chars
         )
     except zipfile.BadZipFile:
         st.error("Hibás ZIP fájl.")
         st.stop()
     except Exception as e:
         st.error(f"Váratlan hiba: {e}")
 
EOF
)
