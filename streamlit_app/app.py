# streamlit_app/app.py
import streamlit as st
import pandas as pd
import glob
import os
from pathlib import Path
import plotly.express as px

st.set_page_config(page_title="Job Market Intelligence", layout="wide")

@st.cache_data
def load_latest_processed(base_path="data/processed"):
    """Carga el Parquet más reciente de la carpeta processed"""
    pattern = os.path.join(base_path, "*", "jobs_processed.parquet")
    files = glob.glob(pattern)
    if not files:
        st.error("No se encontraron datos procesados. Ejecuta el pipeline primero.")
        return pd.DataFrame()
    latest_file = max(files, key=os.path.getctime)
    df = pd.read_parquet(latest_file)
    # Asegurar que 'technologies' sea lista (puede venir como string o lista)
    if 'technologies' in df.columns:
        df['technologies'] = df['technologies'].apply(lambda x: x if isinstance(x, list) else [])
    else:
        df['technologies'] = [[] for _ in range(len(df))]
    return df

st.title("📊 Inteligencia del Mercado Laboral")
st.markdown("Explora ofertas de trabajo usando datos procesados (archivos Parquet)")

df = load_latest_processed()
if df.empty:
    st.stop()

# Extraer lista única de tecnologías
all_techs = sorted(set([tech for sublist in df['technologies'] for tech in sublist]))

# Sidebar: filtros
st.sidebar.header("Filtros")
selected_tech = st.sidebar.selectbox("Tecnología", ["Todas"] + all_techs)
location_filter = st.sidebar.text_input("Ubicación (contiene)")

# Aplicar filtros
filtered_df = df.copy()
if selected_tech != "Todas":
    filtered_df = filtered_df[filtered_df['technologies'].apply(lambda techs: selected_tech in techs)]
if location_filter:
    filtered_df = filtered_df[filtered_df['location'].str.contains(location_filter, case=False, na=False)]

st.subheader(f"📈 Resultados: {len(filtered_df)} ofertas")

col1, col2 = st.columns(2)

with col1:
    # Top tecnologías en el conjunto filtrado
    tech_counts = {}
    for techs in filtered_df['technologies']:
        for t in techs:
            tech_counts[t] = tech_counts.get(t, 0) + 1
    if tech_counts:
        tech_df = pd.DataFrame(list(tech_counts.items()), columns=['Tecnología', 'Conteo']).sort_values('Conteo', ascending=False).head(15)
        fig = px.bar(tech_df, x='Tecnología', y='Conteo', title='Tecnologías más demandadas')
        st.plotly_chart(fig, use_container_width=True)

with col2:
    # Top ubicaciones
    loc_counts = filtered_df['location'].value_counts().reset_index().head(10)
    loc_counts.columns = ['Ubicación', 'Conteo']
    fig2 = px.bar(loc_counts, x='Ubicación', y='Conteo', title='Ofertas por ubicación')
    st.plotly_chart(fig2, use_container_width=True)

# Tabla de resultados
st.subheader("📋 Listado de ofertas")
st.dataframe(
    filtered_df[['title', 'company', 'location', 'technologies', 'posted_date', 'source']],
    use_container_width=True,
    column_config={
        'technologies': st.column_config.ListColumn("Tecnologías"),
        'posted_date': st.column_config.DateColumn("Fecha publicación"),
    }
)

# Descarga del filtrado
csv = filtered_df.to_csv(index=False).encode('utf-8')
st.download_button("⬇️ Descargar filtrado (CSV)", csv, "jobs_filtrados.csv", "text/csv")