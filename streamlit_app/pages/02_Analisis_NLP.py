import streamlit as st
import pandas as pd
from sqlalchemy import text
from streamlit_app.app import get_engine, load_data

st.title("🔍 Análisis NLP de Descripciones")

engine = get_engine()
query = "SELECT title, description, technology FROM jobs WHERE is_active = TRUE LIMIT 500"
df = load_data(query)

st.subheader("Nube de palabras (próximamente)")
st.info("Integración con wordcloud o extracción de términos clave")