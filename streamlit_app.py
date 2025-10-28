import streamlit as st
import pandas as pd
from fire_admin import initialize_firebase, login_user, logout_user, log_event

# Define a configuração da página
st.set_page_config(
    page_title="Rovema | Multi-Produto Dashboard",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- CSS para a Página de Login ---
def load_css():
    """Adiciona CSS para centralizar e estilizar a caixa de login."""
    st.markdown("""
    <style>
        /* Esconde a sidebar na página de login */
        div[data-testid="stSidebarNav"] {
            display: none;
        }

        /* Container do Login */
        .login-container {
            display: flex;
            flex-direction: column;
            align-items: center;
            justify-content: center;
            margin: 0 auto; /* Centraliza horizontalmente */
            max-width: 450px; /* Largura máxima do card de login */
            padding: 2.5rem;
            border-radius: 15px;
            box-shadow: 0 8px 16px rgba(0,0,0,0.1); /* Sombra suave */
            background-color: #ffffff; /* Fundo branco para o card */
            margin-top: 10vh; /* Espaçamento do topo */
        }
        
        /* Título dentro do container */
        .login-container .stTitle {
            text-align: center;
        }
        
        /* Subheader dentro do container */
        .login-container .stSubheader {
            text-align: center;
            color: #4f4f4f; /* Cor mais suave */
        }

        /* Botão de login com largura total */
        .login-container .stButton > button {
            width: 100%;
            background-color: #004080; /* Cor primária (exemplo) */
            color: white;
            border-radius: 8px;
        }
        .login-container .stButton > button:hover {
            background-color: #0059b3; /* Cor no hover */
            color: white;
            border: 1px solid #0059b3;
        }
        
        /* Alertas (erro, warning) */
        .login-container .stAlert {
            width: 100%;
        }

    </style>
    """, unsafe_allow_html=True)

# 1. Inicializa o Firebase
initialization_success = initialize_firebase()

# Inicializa o estado de autenticação
if 'authenticated' not in st.session_state:
    st.session_state['authenticated'] = False
    st.session_state['user_role'] = None

# --- Página de Login ---

if not st.session_state['authenticated']:
    
    # Carrega o CSS customizado
    load_css()
    
    # --- Layout do Container de Login ---
    st.markdown('<div class="login-container">', unsafe_allow_html=True)

    # (Opcional: Adicione sua logo aqui)
    # st.image("path/to/your/logo.png", width=200) 
    
    st.title("Rovema Bank Pulse 📈")
    st.subheader("Sistema de Gestão de Performance")
    st.markdown("---")

    with st.form("login_form"):
        email = st.text_input("E-mail:")
        password = st.text_input("Senha:", type="password")
        st.markdown("<br>", unsafe_allow_html=True) # Adiciona espaço
        login_button = st.form_submit_button("Entrar")

        if login_button:
            success, message = login_user(email, password)
            if success:
                st.success(f"Bem-vindo, {st.session_state['user_email']} ({st.session_state['user_role']})!")
                st.rerun()
            else:
                st.error(message)

    st.markdown("---")
    
    # Informações de Acesso (agora em um expander)
    with st.expander("Informações de Nível de Acesso"):
        st.markdown(
            """
            * **Nível Admin:** Acesso completo.
            * **Nível Usuário:** Acesso ao Dashboard e Upload.
            """
        )

    # Aviso de Setup (só aparece se necessário)
    if initialization_success:
        auth_service = st.session_state.get('auth_service')
        db = st.session_state.get('db')
        
        if auth_service and db and not db.collection('users').limit(1).get():
             st.warning("⚠️ **Alerta de Setup:** Crie seu primeiro usuário 'Admin' manualmente no Console do Firebase.")
    else:
        st.error("Falha na conexão com o Firebase. Verifique os logs e o arquivo secrets.toml.")
    
    # Fecha o container
    st.markdown('</div>', unsafe_allow_html=True)
             
# --- Dashboard Principal (Após Login) ---
else:
    # Mostra a barra lateral
    st.sidebar.title("Rovema Bank Pulse")
    st.sidebar.markdown(f"**Usuário:** `{st.session_state['user_email']}`")
    st.sidebar.markdown(f"**Nível:** **`{st.session_state['user_role']}`**")
    st.sidebar.markdown("---")
    
    if st.sidebar.button("Logout", help="Sair do sistema com segurança"):
        logout_user()
