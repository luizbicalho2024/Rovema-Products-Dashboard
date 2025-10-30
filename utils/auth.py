import streamlit as st
from utils.firebase_config import get_db, get_auth_client
import time

def login_user(email, password):
    """
    Tenta logar o usuário usando o Pyrebase (Cliente Auth).
    """
    auth_client = get_auth_client()
    try:
        user = auth_client.sign_in_with_email_and_password(email, password)
        id_token = user['idToken']
        
        # Após o login, busca os dados do usuário no Firestore
        db = get_db()
        user_doc = db.collection("users").document(user['localId']).get()
        
        if user_doc.exists:
            user_data = user_doc.to_dict()
            
            # Armazena tudo na sessão do Streamlit
            st.session_state.authenticated = True
            st.session_state.user_uid = user['localId']
            st.session_state.user_email = user['email']
            st.session_state.user_name = user_data.get("name", "Usuário")
            st.session_state.user_role = user_data.get("role", "consultant")
            st.session_state.manager_uid = user_data.get("manager_uid")
            
            return True, "Login realizado com sucesso!"
        else:
            return False, "Usuário autenticado, mas não encontrado no banco de dados."
            
    except Exception as e:
        # Tratamento de erros de login (senha errada, usuário não existe)
        return False, f"Falha no login: {e}"

def logout():
    """Limpa o session_state para deslogar o usuário."""
    if "authenticated" in st.session_state:
        del st.session_state.authenticated
    if "user_uid" in st.session_state:
        del st.session_state.user_uid
    # Adicione outros campos se necessário
    st.session_state.clear()
    st.toast("Você foi desconectado.", icon="👋")
    time.sleep(1)
    st.switch_page("Home.py")

def auth_guard():
    """
    O "Guardião" de Autenticação.
    Redireciona para a Home (Login) se o usuário não estiver autenticado.
    Deve ser chamado no início de CADA página protegida.
    """
    if "authenticated" not in st.session_state or not st.session_state.authenticated:
        st.error("Acesso negado. Por favor, faça o login.")
        time.sleep(2)
        st.switch_page("Home.py")
    
    # Exibe o logo e o botão de logout na sidebar de todas as páginas autenticadas
    st.sidebar.image("logoRB.png", use_column_width=True)
    st.sidebar.button("Logout", on_click=logout, use_container_width=True, type="primary")

def check_role(roles: list):
    """Verifica se o usuário tem a permissão necessária."""
    if st.session_state.get("user_role") not in roles:
        st.error("Você não tem permissão para acessar esta página.")
        st.stop()
