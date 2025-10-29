import streamlit as st
from fire_admin import get_db
import pandas as pd

st.set_page_config(layout="wide", page_title="Visão Geral")

# --- Verificação de Login ---
if "user" not in st.session_state:
    st.error("🔒 Por favor, faça o login primeiro.")
    st.page_link("streamlit_app.py", label="Ir para Login", icon="🔑")
    st.stop()

# --- Função de Cache para carregar KPIs ---
@st.cache_data(ttl=600) # Cache de 10 minutos
def load_consolidated_kpis():
    """
    Lê os KPIs PRÉ-CALCULADOS do Firestore.
    Esta função é RÁPIDA.
    """
    try:
        db = get_db()
        kpis_ref = db.collection("dashboard_agregado").document("kpis_consolidados").get()
        if kpis_ref.exists:
            return kpis_ref.to_dict()
        else:
            return {}
    except Exception as e:
        st.error(f"Erro ao carregar KPIs: {e}")
        return {}

# --- Botão de Refresh ---
if st.button("Atualizar Dados"):
    st.cache_data.clear()
    st.rerun()

st.title("🏠 Visão Geral (Consolidado)")

# --- Carregar KPIs ---
kpis = load_consolidated_kpis()

if not kpis:
    st.warning("Os dados ainda não foram processados. Vá ao 'Painel Admin' para iniciar o processamento.")
    st.stop()

# --- Exibir KPIs ---
st.header("Métricas Chave (Todos os Produtos)")

col1, col2, col3, col4 = st.columns(4)
col1.metric("GMV RovemaPay", f"R$ {kpis.get('rovemapay_gmv', 0):,.2f}")
col2.metric("Receita RovemaPay", f"R$ {kpis.get('rovemapay_receita', 0):,.2f}")
col3.metric("GMV Bionio (Pago)", f"R$ {kpis.get('bionio_gmv', 0):,.2f}")
col4.metric("Crossover Rovema/Bionio", f"{kpis.get('crossover_rovema_bionio', 0)} Clientes")

# ... adicione as outras métricas ...

st.info("Para dados de ASTO e ELIQ, certifique-se de que o processador (`etl_processor.py`) está configurado para buscá-los e agregá-los.")
