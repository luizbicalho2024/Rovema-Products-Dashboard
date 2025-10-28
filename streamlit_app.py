import streamlit as st
import pandas as pd
from fire_admin import initialize_firebase, login_user, logout_user, log_event, auth_service

# Define a configuração da página
st.set_page_config(
    page_title="Rovema | Multi-Produto Dashboard",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded"
)

# 1. Inicializa o Firebase e configura o estado da sessão
auth_service, db, bucket, project_id = initialize_firebase()

# Inicializa o estado de autenticação
if 'authenticated' not in st.session_state:
    st.session_state['authenticated'] = False
    st.session_state['user_role'] = None

# --- Página de Login ---

if not st.session_state['authenticated']:
    st.title("🔒 Sistema de Gestão de Performance - Login")
    st.markdown("---")
    
    col1, col2 = st.columns([1, 2])
    
    with col1:
        st.subheader("Autenticação")
        with st.form("login_form"):
            email = st.text_input("E-mail:")
            password = st.text_input("Senha:", type="password")
            login_button = st.form_submit_button("Entrar")

            if login_button:
                # O log_event é chamado dentro de login_user para registrar a tentativa
                success, message = login_user(email, password)
                if success:
                    st.success(f"Bem-vindo, {st.session_state['user_email']} ({st.session_state['user_role']})!")
                    st.rerun()
                else:
                    st.error(message)

    with col2:
        st.info("Insira suas credenciais para acessar o Dashboard. O acesso é gerenciado e auditado via Firebase.")
        st.markdown(
            """
            * **Nível Admin:** Acesso completo a todas as páginas (Dashboard, Gestão de Acessos e Logs).
            * **Nível Usuário:** Acesso ao Dashboard e Upload de Dados.
            """
        )
        # Aviso para o primeiro setup
        if auth_service and not db.collection('users').limit(1).get():
             st.warning("⚠️ **Alerta de Setup:** Crie seu primeiro usuário 'Admin' manualmente no Console do Firebase (Authentication e Firestore).")
             
# --- Dashboard Principal (Após Login) ---
else:
    # Cabeçalho e Logout na Sidebar
    st.sidebar.title("Rovema Bank Pulse")
    st.sidebar.markdown(f"**Usuário:** `{st.session_state['user_email']}`")
    st.sidebar.markdown(f"**Nível:** **`{st.session_state['user_role']}`**")
    st.sidebar.markdown("---")
    
    # O conteúdo real é carregado pelas páginas, mas o sidebar precisa do botão de logout
    if st.sidebar.button("Logout", help="Sair do sistema com segurança"):
        logout_user()
