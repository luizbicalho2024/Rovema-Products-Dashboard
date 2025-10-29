# Home.py (ou main_app.py)

import streamlit as st
# ... (outras importações)
import copy # Garanta que esta linha está presente para a cópia profunda
from app import auth, ui_pages, db_utils

# ... (Restante do código de roteamento) ...

# --- Configuração da Página Streamlit ---
st.set_page_config(layout="wide", page_title="BI Estratégia Comercial Pro")

# --- Inicialização de Serviços ---
db_client = db_utils.init_firebase()

if db_client:
    # --- Roteamento da Aplicação Principal ---
    
    if not st.session_state.get('logged_in'):
        # 1. Página de Login
        auth.login_page(db_client)
    else:
        # 2. Menu de Navegação
        
        # Menu Lateral para Navegação
        with st.sidebar:
            st.write(f"Usuário: **{st.session_state.get('user_email')}**")
            page = st.radio("Navegação", ["Dashboard (BI)", "Atualização de Dados", "Gestão de Equipe"])
            st.markdown("---")
            st.button("Sair", on_click=auth.logout)

        # 3. Conteúdo da Página
        if page == "Dashboard (BI)":
            ui_pages.bi_dashboard_page(db_client)
        elif page == "Atualização de Dados":
            ui_pages.data_ingestion_page(db_client)
        elif page == "Gestão de Equipe":
            ui_pages.management_page(db_client)
