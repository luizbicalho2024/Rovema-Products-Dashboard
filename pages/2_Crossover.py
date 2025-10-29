import streamlit as st
from fire_admin import load_kpis, initialize_firebase
import pandas as pd

st.set_page_config(layout="wide", page_title="Crossover")
initialize_firebase()

if "user_email" not in st.session_state:
    st.error("🔒 Por favor, faça o login primeiro."); st.stop()

st.title("🤝 Crossover e Análise Estratégica")

# --- Carrega o documento com a tabela de oportunidades ---
# Note que estamos chamando a mesma função, mas para um documento diferente
oportunidades_data = load_kpis("tabela_oportunidades")

if not oportunidades_data:
    st.warning("Tabela de oportunidades ainda não calculada. Vá ao Painel Admin.")
    st.stop()

st.header("Oportunidades de Cross-Sell")

df_ops = pd.DataFrame(oportunidades_data.get('oportunidade_rovema_nao_bionio', []))

st.subheader(f"Clientes RovemaPay que NÃO usam Bionio ({len(df_ops)})")

if not df_ops.empty:
    st.dataframe(df_ops, use_container_width=True)
else:
    st.info("Nenhuma oportunidade encontrada ou dados não processados.")

# TODO: Adicionar um Diagrama de Venn (ex: matplotlib-venn)
# Você pode pré-calcular os 3 valores (A, B, Interseção)
# no etl_processor.py e passá-los para cá.
