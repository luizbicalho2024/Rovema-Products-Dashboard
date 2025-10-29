import streamlit as st
from fire_admin import load_kpis, initialize_firebase

st.set_page_config(layout="wide", page_title="Visão Geral")
initialize_firebase()

# --- Verificação de Login ---
if "user_email" not in st.session_state:
    st.error("🔒 Por favor, faça o login primeiro.")
    st.page_link("streamlit_app.py", label="Ir para Login", icon="🔑")
    st.stop()

st.title(f"🏠 Visão Geral (Consolidado)")
st.caption(f"Logado como: {st.session_state.user_email}")

# --- Botão de Refresh ---
if st.button("Atualizar Dados"):
    st.cache_data.clear()
    st.rerun()

# --- Carregar KPIs (Função Rápida) ---
kpis = load_kpis("kpis_consolidados")

if not kpis:
    st.warning("Os dados ainda não foram processados. Vá ao 'Painel Admin' para iniciar o processamento.")
    st.stop()

# --- Exibir KPIs ---
st.header("Métricas Chave Globais")

col1, col2, col3, col4 = st.columns(4)
col1.metric("GMV RovemaPay", f"R$ {kpis.get('rovemapay_gmv', 0):,.2f}")
col2.metric("Receita RovemaPay", f"R$ {kpis.get('rovemapay_receita', 0):,.2f}")
col3.metric("GMV Bionio (Pago)", f"R$ {kpis.get('bionio_gmv', 0):,.2f}")
col4.metric("Contratado (ELIQ)", f"R$ {kpis.get('eliq_valor_total_contratado', 0):,.2f}")

st.header("Métricas de Clientes")
col1, col2, col3, col4 = st.columns(4)
col1.metric("Clientes RovemaPay", f"{kpis.get('rovemapay_clientes_ativos', 0)}")
col2.metric("Organizações Bionio", f"{kpis.get('bionio_orgs_ativas', 0)}")
col3.metric("Clientes ELIQ", f"{kpis.get('eliq_clientes_total', 0)}")
col4.metric("Estabelecimentos ASTO", f"{kpis.get('asto_estabelecimentos_total', 0)}")

st.header("Métricas de Crossover (Estratégico)")
col1, col2, col3 = st.columns(3)
col1.metric("RovemaPay ∩ Bionio", f"{kpis.get('crossover_rovema_bionio', 0)} Clientes")
col2.metric("ELIQ ∩ Bionio", f"{kpis.get('crossover_eliq_bionio', 0)} Clientes")
col3.metric("ELIQ ∩ RovemaPay", f"{kpis.get('crossover_eliq_rovema', 0)} Clientes")
