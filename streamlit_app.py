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
            margin-top: 5vh; /* Espaçamento do topo reduzido */
        }

        /* Logo dentro do container */
        .login-container img {
             margin-bottom: 1.5rem; /* Espaço abaixo da logo */
        }

        /* Título dentro do container */
        .login-container .stTitle {
            text-align: center;
            margin-top: 0; /* Remove margem extra acima do título */
        }

        /* Subheader dentro do container */
        .login-container .stSubheader {
            text-align: center;
            color: #4f4f4f; /* Cor mais suave */
            margin-bottom: 1rem; /* Espaço abaixo do subheader */
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

# --- Função de Verificação de Usuários (Cacheada e Corrigida) ---
@st.cache_data(ttl=3600) # Cacheia o resultado por 1 hora
def check_if_users_exist(): # REMOVIDO o argumento db_conn
    """Verifica (com cache) se algum usuário existe na coleção 'users'."""
    # Pega o DB de dentro da função
    db_conn = st.session_state.get('db')
    if db_conn:
        try:
            # Faz a leitura mínima necessária
            docs = db_conn.collection('users').limit(1).get()
            return len(docs) > 0 # Retorna True se > 0 usuários existirem
        except Exception as e:
            print(f"ERRO Firestore (Check Users): {e}")
            return True # Assume que usuários existem ou erro impede a verificação
    return True # Assume que usuários existem se DB não estiver conectado

# 1. Inicializa o Firebase
initialization_success = initialize_firebase()

# Inicializa o estado de autenticação
if 'authenticated' not in st.session_state:
    st.session_state['authenticated'] = False
    st.session_state['user_role'] = None

# --- Página de Login ---

if not st.session_state['authenticated']:

    load_css()

    # --- Layout do Container de Login ---
    # Abre o container ANTES de qualquer elemento de UI
    st.markdown('<div class="login-container">', unsafe_allow_html=True)

    # Coloca a logo AQUI, dentro do container
    try:
        st.image("assets/logoRB.png", width=250)
    except FileNotFoundError:
        st.error("Erro: Arquivo 'assets/logoRB.png' não encontrado.")
    except Exception as e:
        st.error(f"Erro ao carregar a logo: {e}")

    # Continua com o resto do formulário DENTRO do container
    st.title("Rovema Bank Pulse 📈")
    st.subheader("Sistema de Gestão de Performance")
    st.markdown("---")

    with st.form("login_form"):
        email = st.text_input("E-mail:")
        password = st.text_input("Senha:", type="password")
        st.markdown("<br>", unsafe_allow_html=True)
        login_button = st.form_submit_button("Entrar")

        if login_button:
            success, message = login_user(email, password)
            if success:
                st.success(f"Bem-vindo, {st.session_state['user_email']} ({st.session_state['user_role']})!")
                check_if_users_exist.clear() # Limpa cache após login
                st.rerun()
            else:
                st.error(message)

    st.markdown("---")

    with st.expander("Informações de Nível de Acesso"):
        st.markdown(
            """
            * **Nível Admin:** Acesso completo.
            * **Nível Usuário:** Acesso ao Dashboard e Upload.
            """
        )

    # Aviso de Setup (Usa a função cacheada sem argumento)
    if initialization_success:
        # Chama a função cacheada sem passar o db
        users_exist = check_if_users_exist()

        # Mostra o alerta apenas se a conexão funcionou E a função cacheada retornou False
        # (Precisa pegar o 'db' do state aqui só para a condição 'if db')
        db_conn_check = st.session_state.get('db')
        if db_conn_check and not users_exist:
             st.warning("⚠️ **Alerta de Setup:** Crie seu primeiro usuário 'Admin' manualmente no Console do Firebase.")
    else:
        st.error("Falha na conexão com o Firebase. Verifique os logs e o arquivo secrets.toml.")

    # Fecha o container DEPOIS de todos os elementos da página de login
    st.markdown('</div>', unsafe_allow_html=True)

# --- Dashboard Principal (Após Login) ---
else:
    st.sidebar.title("Rovema Bank Pulse")
    st.sidebar.markdown(f"**Usuário:** `{st.session_state['user_email']}`")
    st.sidebar.markdown(f"**Nível:** **`{st.session_state['user_role']}`**")
    st.sidebar.markdown("---")

    if st.sidebar.button("Logout", help="Sair do sistema com segurança"):
        check_if_users_exist.clear() # Limpa cache ao deslogar
        logout_user()
