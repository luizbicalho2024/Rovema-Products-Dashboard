import streamlit as st
import firebase_admin
from firebase_admin import credentials, auth, firestore, storage
import json
import requests
import pandas as pd # Adicionado para uso em funções de Logs/Gestão
from google.cloud.firestore import SERVER_TIMESTAMP as server_timestamp # Renomeado para uso limpo

# --- 1. CONFIGURAÇÃO E INICIALIZAÇÃO DO FIREBASE ---

@st.cache_resource
def get_web_api_key():
    """Busca a chave da API Web dos secrets e verifica sua existência."""
    api_key = st.secrets.get("FIREBASE_WEB_API_KEY")
    if not api_key:
        # Tenta buscar a chave caso ela tenha sido salva como parte do dicionário JSON
        api_key = st.secrets.get("firebase_service_account", {}).get("FIREBASE_WEB_API_KEY")
    return api_key

FIREBASE_WEB_API_KEY = get_web_api_key()

@st.cache_resource
def initialize_firebase():
    """Inicializa o Firebase Admin SDK usando Streamlit Secrets."""
    
    if "firebase_service_account" not in st.secrets:
        st.error("ERRO: As credenciais do Firebase (JSON) não foram encontradas no Secrets.")
        return None, None, None, None

    try:
        cred_dict = dict(st.secrets["firebase_service_account"])
        
        # Trata quebras de linha na chave privada
        if 'private_key' in cred_dict:
            cred_dict['private_key'] = cred_dict['private_key'].replace('\\n', '\n')
            
        cred = credentials.Certificate(cred_dict)

        if not firebase_admin._apps:
            firebase_admin.initialize_app(cred, {
                'storageBucket': f"{cred_dict['project_id']}.appspot.com"
            })
            
        db = firestore.client()
        bucket = storage.bucket()
        
        st.session_state['db'] = db
        st.session_state['bucket'] = bucket
        
        return auth, db, bucket, cred_dict['project_id']
    except Exception as e:
        st.error(f"Erro ao inicializar o Firebase: {e}")
        return None, None, None, None

auth_service, db, bucket, project_id = initialize_firebase()

# --- 2. FUNÇÕES DE LOGS (Auditoria) ---

def log_event(action: str, details: str = ""):
    """Registra um evento de log no Firestore."""
    if 'db' not in st.session_state:
        return
    
    user_email = st.session_state.get('user_email', 'SISTEMA')
    
    log_entry = {
        "timestamp": server_timestamp,
        "user_email": user_email,
        "action": action,
        "details": details
    }
    try:
        st.session_state['db'].collection('logs').add(log_entry)
    except Exception as e:
        # Usa st.toast para logs não críticos, evitando poluir o console
        st.toast(f"Não foi possível registrar o log: {e}", icon="⚠️")

# --- 3. FUNÇÕES DE AUTENTICAÇÃO (Login e Logout) ---

def login_user(email: str, password: str):
    """
    Autentica o usuário usando a REST API (para verificar a senha) 
    e recupera o papel (role) no Firestore.
    """
    if not FIREBASE_WEB_API_KEY:
        log_event("LOGIN_ERROR", "Chave da API Web não configurada ou vazia nos secrets.")
        return False, "Erro de configuração: Chave da API Web não encontrada ou está vazia."
        
    API_URL = f"https://identitytoolkit.googleapis.com/v1/accounts:signInWithPassword?key={FIREBASE_WEB_API_KEY}"
    
    payload = {
        "email": email,
        "password": password,
        "returnSecureToken": True
    }

    try:
        # 1. Autenticação de Senha (Firebase REST API)
        response = requests.post(API_URL, json=payload)
        response.raise_for_status() # Lança exceção para status HTTP de erro (4xx ou 5xx)
        
        data = response.json()
        user_uid = data.get('localId')

        if not user_uid:
            log_event("LOGIN_FAIL", f"Autenticação falhou para {email}. UID não retornado.")
            return False, "E-mail ou senha incorretos."

        # 2. Busca o papel (Role) no Firestore usando o UID
        user_doc = st.session_state['db'].collection('users').document(user_uid).get()
        if not user_doc.exists:
            log_event("LOGIN_FAIL", f"Usuário {email} autenticado, mas sem papel de acesso (UID: {user_uid}).")
            return False, "Usuário autenticado, mas sem papel de acesso definido. Contate o administrador."
        
        user_data = user_doc.to_dict()
        role = user_data.get('role', 'Usuário')
        
        # 3. Configura a sessão
        st.session_state['authenticated'] = True
        st.session_state['user_email'] = email
        st.session_state['user_role'] = role
        st.session_state['user_uid'] = user_uid
        
        log_event("LOGIN_SUCCESS", f"Usuário {email} logado com sucesso. Papel: {role}.")
        return True, "Login bem-sucedido!"
        
    except requests.exceptions.HTTPError as e:
        error_json = e.response.json()
        error_code = error_json.get('error', {}).get('message', 'UNKNOWN_ERROR')
        
        # Códigos de erro comuns do Firebase Auth
        if error_code in ["EMAIL_NOT_FOUND", "INVALID_PASSWORD"]:
            log_event("LOGIN_FAIL", f"Tentativa de login falhou. Erro: {error_code}.")
            return False, "E-mail ou senha incorretos."
        if error_code == "USER_DISABLED":
             log_event("LOGIN_FAIL", f"Tentativa de login falhou. Usuário desativado: {email}.")
             return False, "Sua conta foi desativada. Contate o administrador."
        
        log_event("LOGIN_ERROR", f"Erro HTTP crítico de login para {email}: {error_code}")
        return False, f"Erro na autenticação. Contate o administrador. (Código: {error_code})"
    
    except Exception as e:
        log_event("LOGIN_ERROR", f"Erro inesperado de login para {email}: {e}")
        return False, "Erro inesperado na autenticação. Tente novamente."

def logout_user():
    """Limpa o estado da sessão e desloga o usuário."""
    log_event("LOGOUT", "Usuário deslogou.")
    st.session_state['authenticated'] = False
    st.session_state['user_email'] = None
    st.session_state['user_role'] = None
    st.session_state['user_uid'] = None
    st.rerun()

# --- 4. FUNÇÕES DE GERENCIAMENTO DE ACESSOS (Admin) ---

def create_user(email, password, role, name):
    """Cria um novo usuário no Firebase Auth e define o papel no Firestore."""
    if 'db' not in st.session_state or not auth_service:
        return False, "Serviços Firebase não inicializados."
    
    try:
        # 1. Cria o usuário no Firebase Authentication (para login)
        user = auth_service.create_user(email=email, password=password)
        
        # 2. Define o papel (role) no Firestore (para autorização)
        st.session_state['db'].collection('users').document(user.uid).set({
            'email': email,
            'role': role,
            'nome': name
        })
        log_event("ADMIN_ACTION", f"Usuário criado: {email} com papel {role}.")
        return True, f"Usuário {email} criado com sucesso! UID: {user.uid}"
    except Exception as e:
        log_event("ADMIN_ACTION_FAIL", f"Falha ao criar usuário {email}: {e}")
        return False, f"Erro ao criar usuário: {e}"

def get_all_users():
    """Retorna a lista completa de usuários e seus papéis."""
    if 'db' not in st.session_state:
        return []
    
    try:
        users_ref = st.session_state['db'].collection('users')
        docs = users_ref.stream()
        user_list = []
        for doc in docs:
            data = doc.to_dict()
            data['uid'] = doc.id
            user_list.append(data)
        return user_list
    except Exception as e:
        st.error(f"Erro ao buscar usuários: {e}")
        return []
        
# --- 5. FUNÇÕES DE UPLOAD E ARMAZENAMENTO ---

def upload_file_and_store_ref(uploaded_file, product_name):
    """
    Armazena o arquivo no Storage e registra a referência no Firestore.
    """
    if 'bucket' not in st.session_state or 'db' not in st.session_state:
        return False, "Storage ou Firestore não inicializados."

    try:
        # Define o caminho do arquivo no Storage
        file_path = f"uploads/{product_name}/{st.session_state['user_uid']}_{uploaded_file.name}"
        
        # Faz o upload para o Storage
        blob = st.session_state['bucket'].blob(file_path)
        # O rewind é crucial após a leitura inicial do Streamlit
        blob.upload_from_file(uploaded_file, content_type=uploaded_file.type, rewind=True) 
        
        # Registra o upload no Firestore
        st.session_state['db'].collection('file_uploads').add({
            'product': product_name,
            'filename': uploaded_file.name,
            'storage_path': file_path,
            'uploaded_by': st.session_state['user_email'],
            'timestamp': server_timestamp
        })
        
        log_event("FILE_UPLOAD", f"Arquivo de {product_name} enviado para {file_path}.")
        return True, f"Arquivo '{uploaded_file.name}' enviado com sucesso para o Firebase Storage!"
    except Exception as e:
        log_event("FILE_UPLOAD_FAIL", f"Falha ao enviar arquivo: {e}")
        return False, f"Erro ao enviar arquivo: {e}"
