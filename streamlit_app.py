import streamlit as st
from fire_admin import initialize_firebase, login_user, logout_user, log_event

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
    st.title("Sistema de Gestão de Performance - Login")
    st.markdown("---")
    
    col1, col2 = st.columns([1, 2])
    
    with col1:
        st.subheader("Autenticação")
        with st.form("login_form"):
            email = st.text_input("E-mail:")
            password = st.text_input("Senha:", type="password")
            login_button = st.form_submit_button("Entrar")

            if login_button:
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
            * **Nível Admin:** Acesso ao Dashboard, Gerenciamento de Acessos e Logs.
            * **Nível Usuário:** Acesso apenas ao Dashboard.
            """
        )
        # Se for a primeira inicialização, uma mensagem para criar um usuário inicial
        if auth_service and not db.collection('users').limit(1).get():
             st.warning("⚠️ **Alerta de Setup:** Crie seu primeiro usuário 'Admin' manualmente no Console do Firebase.")
             
# --- Dashboard Principal (Após Login) ---
else:
    # Cabeçalho e Logout
    st.sidebar.title("Bem-vindo(a), " + st.session_state['user_email'])
    st.sidebar.markdown(f"**Nível de Acesso:** `{st.session_state['user_role']}`")
    
    if st.sidebar.button("Logout", help="Sair do sistema"):
        logout_user()
