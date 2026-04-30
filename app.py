import streamlit as st
import subprocess
import base64
import pandas as pd
import os
import json
import tempfile
import shutil
import re
import time
from datetime import datetime
from itertools import combinations
import zipfile
from openpyxl import load_workbook
from io import BytesIO

# ----------------------------
# PAGE CONFIG
# ----------------------------
st.set_page_config(
    page_title="MXLab Tool",
    page_icon=None,
    layout="wide"
)

# Aquí FINAL_OUTPUT_DIR y todo lo dependiente de rutas locales
# NO se usa ya, porque en Streamlit Cloud no puedes escribir fuera del workspace

# Parámetros para PK inference y otros
SAMPLE_ROWS = 1500
MAX_PK_COLS = 4
UNIQ_TARGET = 0.999
TOP_COLS = 10
MAX_COMBOS_PER_K = 300
MAX_FILES_INFERENCE = 250

MEASURE_TOKENS = {
    "pnl", "p&l", "profit", "loss", "gain", "pl",
    "amount", "amt", "cash", "price", "rate", "spread",
    "quantity", "qty", "notional", "npv", "pv", "value",
    "market", "clean", "dirty"
}

STRONG_ID_TOKENS = {
    "nb",
    "trade", "tradeid",
    "contract", "contractid", "contractnumber",
    "deal", "dealid",
    "order", "orderid",
    "transaction", "transactionid",
    "position", "positionid",
    "booking", "bookingid",
}

_num_re = re.compile(r"^-?\d+(\.\d+)?$")

# ----------------------------
# Helpers generales
# ----------------------------

def normalize_header(name: str) -> str:
    if name is None:
        return ""
    s = str(name).strip()
    s = re.sub(r"\s+", "", s)
    s = re.sub(r"[()]", "", s)
    s = s.replace("&", "")
    return s.upper()

def normalize_headers_list(fields):
    out = []
    for x in (fields or []):
        nx = normalize_header(x)
        if nx != "":
            out.append(nx)
    return out

def normalize_csv_headers_df(df: pd.DataFrame) -> pd.DataFrame:
    cols = [normalize_header(c) for c in df.columns]
    seen = {}
    fixed = []
    for c in cols:
        if c not in seen:
            seen[c] = 1
            fixed.append(c)
        else:
            seen[c] += 1
            fixed.append(f"{c}_{seen[c]}")
    df.columns = fixed
    return df

def get_files_from_zip(uploaded_zip):
    # Devuelve los archivos dentro del ZIP en un dict {filename: BytesIO}
    file_dict = dict()
    with zipfile.ZipFile(uploaded_zip) as z:
        for name in z.namelist():
            if not name.lower().endswith(('.csv','.txt','.dat')):
                continue
            file_dict[name] = z.open(name)
    return file_dict

def detect_delimiter_from_bytesio(f):
    # Lee la primera línea para inferir delimitador
    try:
        f.seek(0)
        line = f.readline().decode('utf-8', errors='replace')
        candidates = [",", ";", "|", "\t"]
        counts = {d: line.count(d) for d in candidates}
        return max(counts, key=counts.get)
    except Exception:
        return ","

def numeric_profile(series: pd.Series):
    s = series.astype(str).str.strip()
    s = s[s != ""]
    if len(s) == 0:
        return (0.0, 0.0)
    is_num = s.apply(lambda x: bool(_num_re.match(x)))
    numeric_ratio = float(is_num.mean())
    dec_ratio = float(s[is_num].str.contains(r"\.", regex=True).mean()) if is_num.any() else 0.0
    return (numeric_ratio, dec_ratio)

# ----------------------------
# Subida de archivos ZIP
# ----------------------------

st.markdown("## Suba archivos fuente y destino como ZIP")
st.markdown("""
Por favor suba dos archivos ZIP:
- Uno con todos los archivos **fuente**
- Otro con todos los archivos **destino**

Cada ZIP debe contener solo archivos .csv, .txt o .dat.
""")

col1, col2 = st.columns(2)
with col1:
    uploaded_source_zip = st.file_uploader("ZIP de archivos fuente", type='zip', key='source_zip')
with col2:
    uploaded_target_zip = st.file_uploader("ZIP de archivos destino", type='zip', key='target_zip')

source_files_dict, target_files_dict = {}, {}

if uploaded_source_zip:
    source_files_dict = get_files_from_zip(uploaded_source_zip)
    st.success(f"{len(source_files_dict)} archivos fuente detectados.")

if uploaded_target_zip:
    target_files_dict = get_files_from_zip(uploaded_target_zip)
    st.success(f"{len(target_files_dict)} archivos destino detectados.")

# Para comparar: intersection por nombre
matched_filenames = sorted(list(set(source_files_dict.keys()) & set(target_files_dict.keys())))

if uploaded_source_zip and uploaded_target_zip:
    st.markdown(f"#### Coincidencia entre fuente y destino: {len(matched_filenames)} archivos")
    selected_matched_files = st.multiselect("Selecciona los archivos a comparar", matched_filenames, default=matched_filenames)
else:
    selected_matched_files = []
    
# -------------------------------
# CONFIGURACIÓN DEL PAQUETE
# -------------------------------
st.markdown("## Configuración logística del paquete")

colA, colB = st.columns([2,1])
with colA:
    package_name = st.text_input("Package Name")
    package_version = st.text_input("Package Version")

with colB:
    st.markdown("### Variables globales")
    if "global_vars" not in st.session_state:
        st.session_state.global_vars = pd.DataFrame(columns=["Name", "Value"])
    global_vars_df = st.data_editor(
        st.session_state.global_vars,
        num_rows="dynamic",
        use_container_width=True
    )
    st.session_state.global_vars = global_vars_df

# -------------------------------
# DELIMITADOR Y HEADER DETECTION
# -------------------------------

st.markdown("## Configuración de delimitador y campos por archivo")

delimiters = {}
has_headers = {}
fields_info = {}

for fname in selected_matched_files:
    # Detectar delimitador
    delimiter = detect_delimiter_from_bytesio(source_files_dict[fname])
    delimiters[fname] = delimiter

    # Detectar headers
    f = source_files_dict[fname]
    f.seek(0)
    line = f.readline().decode('utf-8', errors='replace').strip()
    raw_fields = line.split(delimiter)
    numeric_like = sum(x.strip().replace(".", "", 1).isdigit() for x in raw_fields)
    empty_like = sum(x.strip() == "" for x in raw_fields)
    not_header = (numeric_like + empty_like) >= max(1, int(0.6 * len(raw_fields)))
    has_header = not not_header
    has_headers[fname] = has_header

    if has_header:
        fields_norm = normalize_headers_list([x.strip() for x in raw_fields])
        fields_str = ", ".join(fields_norm)
    else:
        fields_norm = [f"COL{i}" for i in range(len(raw_fields))]
        fields_str = ", ".join(fields_norm)

    fields_info[fname] = fields_str

df_files_config = pd.DataFrame({
    "File Name": selected_matched_files,
    "Delimiter": [delimiters[f] for f in selected_matched_files],
    "Has Header": [has_headers[f] for f in selected_matched_files],
    "Fields": [fields_info[f] for f in selected_matched_files]
})

st.dataframe(df_files_config, use_container_width=True)

# Puedes permitir edición si lo deseas:
df_files_config_edit = st.data_editor(
    df_files_config,
    num_rows="dynamic",
    use_container_width=True,
    key="file_config_editor"
)

# Inferencia de PKs para cada archivo seleccionado
st.markdown("## Inferencia de Primary Keys (PK)")

pk_rows = []

with st.spinner("Infiriendo PKs en los archivos seleccionados..."):
    for fname in selected_matched_files:
        # Prepara delimitador y header
        delimiter = delimiters[fname]
        has_header = has_headers[fname]
        f = source_files_dict[fname]
        f.seek(0)
        # Lee algunas filas para muestreo
        df = pd.read_csv(f, sep=delimiter, dtype=str, nrows=SAMPLE_ROWS, header=0 if has_header else None)
        df = normalize_csv_headers_df(df)
        for col in df.columns:
            df[col] = df[col].astype(str).str.strip().replace({"nan": "", "None": ""})

        n = len(df)
        cols = list(df.columns)
        strong_cols = [c for c in cols if any(tok in c.lower() for tok in STRONG_ID_TOKENS)]
        # Calcula unicidad por combos
        best_combo = []
        best_uniq = 0.0
        for k in range(1, min(MAX_PK_COLS, len(cols)) + 1):
            combos = list(combinations(cols, k))
            if len(combos) > MAX_COMBOS_PER_K:
                combos = combos[:MAX_COMBOS_PER_K]
            for combo in combos:
                vals = df[list(combo)].fillna("").astype(str).agg("||".join, axis=1)
                uniq = float(vals.nunique(dropna=False) / n)
                if uniq > best_uniq:
                    best_uniq = uniq
                    best_combo = list(combo)
                if uniq >= UNIQ_TARGET:
                    break
        pk_suggested = ', '.join(best_combo) if best_combo else ""
        pk_rows.append({
            "File Name": fname,
            "Suggested PKs": pk_suggested,
            "Uniqueness": round(best_uniq * 100, 2),
            "Use PK": False,
            "Primary Keys": pk_suggested
        })

df_pks = pd.DataFrame(pk_rows)

st.markdown("### Primary Keys sugeridos")
df_pks_edit = st.data_editor(
    df_pks,
    use_container_width=True,
    key="pk_suggestion_editor",
    column_config={
        "File Name": st.column_config.TextColumn(width="medium"),
        "Suggested PKs": st.column_config.TextColumn(width="large"),
        "Uniqueness": st.column_config.NumberColumn(width="small"),
        "Use PK": st.column_config.CheckboxColumn(width="small"),
        "Primary Keys": st.column_config.TextColumn(width="large"),
    }
)

# Botones para usar/quitar PK en todos
colpk1, colpk2 = st.columns(2)
with colpk1:
    if st.button("✅ Usar PK en TODOS", key="usepkall"):
        df_pks_edit["Use PK"] = True
        st.session_state.pk_edit_df = df_pks_edit
        st.rerun()
with colpk2:
    if st.button("❌ Quitar PK en TODOS", key="nousepkall"):
        df_pks_edit["Use PK"] = False
        st.session_state.pk_edit_df = df_pks_edit
        st.rerun()

df_pks = df_pks_edit.copy()

# -------------------------------
# CONSTRUCCIÓN DEL PAQUETE
# -------------------------------
st.markdown("## Generar y ejecutar paquete de comparación MXLab")

build_button = st.button("🚀 Build MXLab Package", use_container_width=True)

if build_button:
    # Prepara configuración del paquete
    config = {
        "package": {"name": package_name, "version": package_version},
        "test_suite": "File comparisons",
        "global_variables": {},
        "file_comparison": {},
        "files": []
    }
    # Agrega variables globales
    for _, row in st.session_state.global_vars.iterrows():
        if str(row.get("Name", "")).strip() != "":
            config["global_variables"][row["Name"]] = row.get("Value", "")

    # Agrega archivos seleccionados
    for idx, row in df_pks.iterrows():
        fname = row["File Name"]
        delimiter = delimiters[fname]
        fields = [x.strip() for x in fields_info[fname].split(",") if x.strip()]
        use_pk = bool(row["Use PK"])
        primary_keys = [x.strip() for x in row["Primary Keys"].split(",") if x.strip()]
        config["files"].append({
            "name": fname,
            "delimiter": delimiter,
            "fields": fields,
            "use_primary_keys": use_pk,
            "primary_keys": primary_keys
        })

    # Guarda config en un archivo temporal
    with tempfile.NamedTemporaryFile(delete=False, suffix=".json", mode="w", encoding="utf-8") as tmp:
        json.dump(config, tmp, indent=4)
        config_path = tmp.name

    st.success("Configuración del paquete generada correctamente.")
    st.write(config)

    # Simular ejecución de la comparación (no real, para Cloud)
    # Aquí puedes llamar función de comparación que recibe los dicts source_files_dict, target_files_dict, y config
    # (por ejemplo, comparar archivos, devolver mismatches, etc.)
    st.info("Para ejecución real, tu función debe procesar archivos in-memory.")

    # Guardar config para cuando se corra el Run
    st.session_state.mxlab_config_path = config_path
    st.session_state.mxlab_source_files_dict = source_files_dict
    st.session_state.mxlab_target_files_dict = target_files_dict
    st.session_state.mxlab_selected_files = selected_matched_files
    st.session_state.mxlab_config = config

# -------------------------------
# EJECUCIÓN Y RESULTADOS
# -------------------------------
run_button = st.button("▶ Ejecutar comparación MXLab", use_container_width=True)

if run_button:
    st.info("Running comparisons...")

    # Simulación: compara archivos de ambos sets, por filename
    summary_rows = []
    for fname in st.session_state.mxlab_selected_files:
        # Lee ambos archivos
        src_file = st.session_state.mxlab_source_files_dict[fname]
        tgt_file = st.session_state.mxlab_target_files_dict[fname]
        delimiter = delimiters[fname]
        has_header = has_headers[fname]

        src_file.seek(0)
        tgt_file.seek(0)
        try:
            df_src = pd.read_csv(src_file, sep=delimiter, dtype=str, header=0 if has_header else None)
            df_tgt = pd.read_csv(tgt_file, sep=delimiter, dtype=str, header=0 if has_header else None)
        except Exception as e:
            summary_rows.append({
                "File": fname,
                "Status": f"ERROR: {e}",
                "Mismatches": "",
                "Rows Removed": "",
                "Rows Added": "",
                "Comparison Result": "ERROR"
            })
            continue

        # Simple comparación: número de filas
        mismatches = abs(len(df_src) - len(df_tgt))
        comparison_result = "Passed" if mismatches == 0 else "Failed"
        summary_rows.append({
            "File": fname,
            "Status": "OK",
            "Mismatches": mismatches,
            "Rows Removed": "",
            "Rows Added": "",
            "Comparison Result": comparison_result
        })

    summary_df = pd.DataFrame(summary_rows)
    st.session_state.mxlab_summary_df = summary_df

# -------------------------------
# VISUALIZACIÓN Y EXPORTACIÓN DE RESULTADOS
# -------------------------------

st.markdown("## Resultados del Run")
if "mxlab_summary_df" in st.session_state and isinstance(st.session_state.mxlab_summary_df, pd.DataFrame):
    st.dataframe(st.session_state.mxlab_summary_df, use_container_width=True)

    st.markdown("### Exportar resumen")
    c1, c2 = st.columns(2)
    with c1:
        st.download_button(
            label="⬇️ Descargar resumen (CSV)",
            data=st.session_state.mxlab_summary_df.to_csv(index=False).encode("utf-8-sig"),
            file_name="RunResultsSummary.csv",
            mime="text/csv",
            use_container_width=True
        )
    with c2:
        bio = BytesIO()
        with pd.ExcelWriter(bio, engine="openpyxl") as writer:
            st.session_state.mxlab_summary_df.to_excel(writer, index=False, sheet_name="Run Results Summary")
        st.download_button(
            label="⬇️ Descargar resumen (Excel)",
            data=bio.getvalue(),
            file_name="RunResultsSummary.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True
        )
else:
    st.info("No hay resultados para mostrar aún. Genera y ejecuta el paquete primero.")
    
# -----------------------------
# LIMPIEZA Y ESTILO FINAL
# -----------------------------

# Limpia resultados si se vuelve a cargar la página
if st.button("Limpiar todo y reiniciar la sesión"):
    for key in list(st.session_state.keys()):
        del st.session_state[key]
    st.experimental_rerun()

st.markdown("""
---
<center>
<b>Desarrollado por Juan Alberto Villagrán Guerrero</b>
<br>
App adaptada para trabajar en Streamlit Cloud usando ZIP múltiple.
</center>
""", unsafe_allow_html=True)