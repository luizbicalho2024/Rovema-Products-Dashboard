import streamlit as st
from utils.firebase_config import get_db
import time
import httpx 

def login_user(email, password):
    """
    Tenta logar o usuário usando a API REST de Autenticação do Firebase.
    """
    try:
        api_key = st.secrets["firebase_web_config"]["apiKey"]
        auth_url = f"https://identitytoolkit.googleapis.com/v1/accounts:signInWithPassword?key={api_key}"
        
        payload = {
            "email": email,
            "password": password,
            "returnSecureToken": True
        }
        
        with httpx.Client() as client:
            response = client.post(auth_url, json=payload)
            response.raise_for_status() 
            
        user_data_auth = response.json()
        user_uid = user_data_auth['localId']
        
        db = get_db()
        user_doc = db.collection("users").document(user_uid).get()
        
        if user_doc.exists:
            user_data_db = user_doc.to_dict()
            
            st.session_state.authenticated = True
            st.session_state.user_uid = user_uid
            st.session_state.user_email = user_data_auth['email']
            st.session_state.user_name = user_data_db.get("name", "Usuário")
            st.session_state.user_role = user_data_db.get("role", "consultant")
            st.session_state.manager_uid = user_data_db.get("manager_uid")
            
            return True, "Login realizado com sucesso!"
        else:
            return False, "Usuário autenticado, mas não encontrado no banco de dados (Firestore)."

    except httpx.HTTPStatusError as e:
        try:
            error_data = e.response.json()
            error_message = error_data.get("error", {}).get("message", "Erro desconhecido")
        except:
             error_message = str(e)
        
        if "INVALID_PASSWORD" in error_message or "EMAIL_NOT_FOUND" in error_message or "INVALID_LOGIN_CREDENTIALS" in error_message:
            return False, "Email ou senha inválidos."
        
        # Este é o erro que você está vendo
        if "INVALID_CERTIFICATE_ARGUMENT" in error_message:
             return False, "Erro de configuração do servidor. (Secrets Inválidos). Verifique o Passo 1."

        return False, f"Falha no login: {error_message}"
    except Exception as e:
        # Pega o erro de certificado se ele acontecer antes do httpx
        if "Invalid certificate argument" in str(e):
            return False, f'Erro: {e}. Verifique se o "Secrets" do Streamlit Cloud está formatado como TOML (Passo 1).'
        return False, f"Falha no login: {e}"

def logout():
    """Limpa o session_state para deslogar o usuário."""
    if "authenticated" in st.session_state:
        del st.session_state.authenticated
    if "user_uid" in st.session_state:
        del st.session_state.user_uid
    st.session_state.clear()
    st.toast("Você foi desconectado.", icon="👋")
    time.sleep(1)
    st.switch_page("Home.py")

def auth_guard():
    """
    O "Guardião" de Autenticação.
    """
    if "authenticated" not in st.session_state or not st.session_state.authenticated:
        st.error("Acesso negado. Por favor, faça o login.")
        time.sleep(2)
        st.switch_page("Home.py")
    
    # CORREÇÃO PARA O LOGO:
    st.sidebar.image("logoRB.png", use_column_width='always')
    
    # CORREÇÃO PARA O BOTÃO:
    st.sidebar.button("Logout", on_click=logout, width='stretch', type="primary")

def check_role(roles: list):
    """Verifica se o usuário tem a permissão necessária."""
    if st.session_state.get("user_role") not in roles:
        st.error("Você não tem permissão para acessar esta página.")
        st.stop()
