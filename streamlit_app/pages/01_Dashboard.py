import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
import plotly.express as px
import sys
sys.path.append('..')
from src.config.settings import get_db_uri

st.set_page_config(page_title="Job Market Intelligence", layout="wide")

@st.cache_resource
def get_engine():
    return create_engine(get_db_uri())

def load_data(query):
    engine = get_engine()
    return pd.read_sql(query, engine)

st.title("📊 Dashboard de Ofertas Tecnológicas")

# Consulta por tecnología
st.sidebar.header("Filtros")
tecnologia = st.sidebar.text_input("Tecnología (ej: Python)")
ubicacion = st.sidebar.text_input("Ubicación")

col1, col2 = st.columns(2)

with col1:
    st.subheader("📈 Ofertas por Tecnología")
    query_tech = """
        SELECT unnest(technology) as tech, COUNT(*) as count
        FROM jobs
        WHERE is_active = TRUE
        GROUP BY tech
        ORDER BY count DESC
        LIMIT 20
    """
    df_tech = load_data(query_tech)
    fig = px.bar(df_tech, x='tech', y='count', title='Top Tecnologías')
    st.plotly_chart(fig, use_container_width=True)

with col2:
    st.subheader("📍 Ofertas por Ubicación")
    query_loc = """
        SELECT location, COUNT(*) as count
        FROM jobs
        WHERE is_active = TRUE
        GROUP BY location
        ORDER BY count DESC
        LIMIT 15
    """
    df_loc = load_data(query_loc)
    fig = px.bar(df_loc, x='location', y='count', title='Ubicaciones con más ofertas')
    st.plotly_chart(fig, use_container_width=True)

# Filtros personalizados
if tecnologia:
    query_filter = f"""
        SELECT title, company, location, technology, posted_date
        FROM jobs
        WHERE '{tecnologia.lower()}' = ANY(technology)
        AND is_active = TRUE
    """
    if ubicacion:
        query_filter += f" AND location ILIKE '%{ubicacion}%'"
    df_filtered = load_data(query_filter)
    st.subheader(f"Resultados para tecnología '{tecnologia}'")
    st.dataframe(df_filtered, use_container_width=True)

# Consulta por ubicación desde la página de análisis