
# Notion Markdown → ChatGPT (JSONL/CSV) konverter

Ez a Streamlit app egy Notionból exportált **Markdown ZIP**-et fogad, majd a szabály szerint
kivágja a tartalmat:

- ha a „Videó szövege” szekció **nem üres** → azt használja
- ellenkező esetben, ha a „Lecke szövege” **nem üres** → azt használja
- ha egyik sincs vagy üres → üres tartalom (jelzi a riport)

A kimenet:
- `output.jsonl` (chunkolható),
- `output.csv`,
- `report.csv` (minőségellenőrzés),
- mindez egy `converted_<run_id>.zip`-ben.

## Használat

```bash
pip install -r requirements.txt
streamlit run app.py
```

1. Notionban: **Export → Markdown & CSV** (a letöltött ZIP-et használd).
2. Az appban töltsd fel a ZIP-et.
3. (Opcionálisan) állítsd a „Haladó beállítások”-ban a címke-listákat és a chunkolást.
4. Katt a „Konvertálás indítása” gombra → végén töltsd le a `converted_<run_id>.zip`-et.

## Beállítások

- **Cél címkék** (alap):
  - Videó: `videó szöveg`, `video szoveg`, `videó leirat`, `transcript`, `videó`
  - Lecke: `lecke szöveg`, `lecke anyag`, `tananyag`
- **Heading szintek**: H2–H4 (##, ###, ####).
- **Fuzzy egyezés**: ékezet- és írásjel-toleráns, részleges egyezés (pl. „Videó szöveg – vázlat” is jó).
- **JSONL chunkolás**: kb. 5500 karakter/cikk, 400 karakter átfedéssel.

## Kimeneti mezők

- JSONL rekord:
  ```json
  {
    "run_id": "YYYYMMDD_HHMMSS",
    "doc_id": "slug_cim_hexid",
    "page_id": "notion_hexid",
    "file_name": "Eredeti nev.md",
    "page_title": "Cím",
    "selected_section": "video|lecke|none",
    "selected_heading": "A megtalált szakasz címe",
    "text_markdown": "…kivágott, tisztított markdown…",
    "char_len": 1234
  }
  ```
- CSV oszlopok: `file_name,page_id,page_title,selected_section,selected_heading,char_len,tartalom`.

## Megjegyzés
Az app a Notion Markdown **export-struktúrával** dolgozik. Ha egyes oldalaknál nem H2/H3/H4
a címke, a fuzzy egyezés segít; ha mégsem talál tartalmat, a `report.csv` egyértelműen jelzi.
