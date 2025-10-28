import streamlit as st
import firebase_admin
from firebase_admin import credentials, auth, firestore
import json
import requests
import pandas as pd
from google.cloud.firestore import SERVER_TIMESTAMP as server_timestamp 
from datetime import date # Importa date para tipagem

# --- 1. CONFIGURAÇÃO E INICIALIZAÇÃO DO FIREBASE (ROBUSTA) ---

def get_credentials_dict():
    """Lê todas as credenciais do st.secrets, faz uma cópia segura e garante a limpeza."""
    
    sa_config_readonly = st.secrets.get("firebase_service_account")
    
    if not sa_config_readonly:
        return None, None
        
    sa_config = dict(sa_config_readonly)
        
    if 'private_key' in sa_config:
        key_content = sa_config['private_key']
        if isinstance(key_content, str):
            key_content = key_content.strip().replace('\\n', '\n')
        sa_config['private_key'] = key_content

    api_key = st.secrets.get("FIREBASE_WEB_API_KEY", "")
    api_key = api_key.strip() if isinstance(api_key, str) else None

    return sa_config, api_key

CREDENTIALS_DICT, FIREBASE_WEB_API_KEY = get_credentials_dict()

@st.cache_resource
def initialize_firebase():
    """Inicializa o Firebase Admin SDK e o Firestore."""
    
    if CREDENTIALS_DICT is None:
        return None, None, None, None

    try:
        cred = credentials.Certificate(CREDENTIALS_DICT)

        if not firebase_admin._apps:
            firebase_admin.initialize_app(cred)
            
        db = firestore.client()
        bucket = None 
        
        st.session_state['db'] = db
        st.session_state['bucket'] = bucket
        
        return auth, db, bucket, CREDENTIALS_DICT.get('project_id')
    except Exception as e:
        print(f"Erro Crítico de Inicialização do Firebase: {e}")
        st.error(f"Erro ao inicializar o Firebase: Falha na validação das credenciais.")
        return None, None, None, None

auth_service, db, bucket, project_id = initialize_firebase()


# --- 2. FUNÇÕES DE LOGS (Auditoria) ---

def log_event(action: str, details: str = ""):
    """Registra um evento de log no Firestore."""
    if st.session_state.get('db') is None:
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
    except Exception:
        st.toast(f"Não foi possível registrar o log.", icon="⚠️")


# --- 3. FUNÇÕES DE AUTENTICAÇÃO (Login e Logout) ---

def login_user(email: str, password: str):
    """Autentica o usuário e recupera o papel (role) no Firestore."""
    if not FIREBASE_WEB_API_KEY:
        log_event("LOGIN_ERROR", "Chave da API Web não configurada ou vazia nos secrets.")
        # Se a chave da API Web falhou, o Service Account também pode ter falhado
        return False, "Erro de configuração: Chave da API Web não encontrada ou está vazia."
        
    API_URL = f"https://identitytoolkit.googleapis.com/v1/accounts:signInWithPassword?key={FIREBASE_WEB_API_KEY}"
    
    payload = {"email": email, "password": password, "returnSecureToken": True}

    try:
        response = requests.post(API_URL, json=payload)
        response.raise_for_status() 
        data = response.json()
        user_uid = data.get('localId')

        if not user_uid:
            return False, "E-mail ou senha incorretos."

        if st.session_state.get('db') is None:
            log_event("LOGIN_ERROR", "Firestore indisponível devido a falha nas credenciais iniciais.")
            return False, "Erro crítico de serviço: Contate o administrador."

        user_doc = st.session_state['db'].collection('users').document(user_uid).get()
        if not user_doc.exists:
            log_event("LOGIN_FAIL", f"Usuário {email} autenticado, mas sem papel de acesso (UID: {user_uid}).")
            return False, "Usuário autenticado, mas sem papel de acesso definido. Contate o administrador."
        
        user_data = user_doc.to_dict()
        role = user_data.get('role', 'Usuário')
        
        st.session_state['authenticated'] = True
        st.session_state['user_email'] = email
        st.session_state['user_role'] = role
        st.session_state['user_uid'] = user_uid
        
        log_event("LOGIN_SUCCESS", f"Usuário {email} logado com sucesso. Papel: {role}.")
        return True, "Login bem-sucedido!"
        
    except requests.exceptions.HTTPError as e:
        error_json = e.response.json()
        error_code = error_json.get('error', {}).get('message', 'UNKNOWN_ERROR')
        
        if error_code in ["EMAIL_NOT_FOUND", "INVALID_PASSWORD"]:
            log_event("LOGIN_FAIL", f"Tentativa de login falhou. Erro: {error_code}.")
            return False, "E-mail ou senha incorretos."
        
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
    if st.session_state.get('db') is None or not auth_service:
        return False, "Serviços Firebase indisponíveis. Contate o administrador."
    
    try:
        user = auth_service.create_user(email=email, password=password)
        
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
    if st.session_state.get('db') is None:
        return []
    
    try:
        users_ref = st.session_state['db'].collection('users')
        docs = users_ref.stream()
        user_list = [doc.to_dict() | {'uid': doc.id} for doc in docs]
        return user_list
    except Exception as e:
        st.error(f"Erro ao buscar usuários: {e}")
        return []


# --- 5. FUNÇÃO: SALVAR DADOS NO FIRESTORE (API e Upload) ---

def save_data_to_firestore(product_name: str, df_data: pd.DataFrame, source_type: str):
    """
    Salva o DataFrame processado (de API ou Upload) no Firestore.
    """
    if st.session_state.get('db') is None:
        return False, "Firestore não inicializado."

    data_records = df_data.to_dict('records')
    upload_metadata = {
        'product': product_name,
        'source_type': source_type, 
        'uploaded_by': st.session_state.get('user_email', 'SISTEMA'),
        'timestamp': server_timestamp,
        'total_records': len(data_records),
    }
    
    try:
        upload_doc_ref = st.session_state['db'].collection('data_metadata').add(upload_metadata)
        upload_id = upload_doc_ref[1].id if isinstance(upload_doc_ref, tuple) else upload_doc_ref.id

        collection_name = f"data_{product_name.lower().replace(' ', '_')}"
        
        batch = st.session_state['db'].batch()
        
        for i, record in enumerate(data_records):
            record['data_source_id'] = upload_id 
            record['ingestion_timestamp'] = server_timestamp
            
            for key, value in record.items():
                # Converte Timestamps para ISO string (Firestore não salva objetos Timestamp do Pandas)
                if isinstance(value, pd.Timestamp):
                    record[key] = value.isoformat()
            
            doc_ref = st.session_state['db'].collection(collection_name).document()
            batch.set(doc_ref, record)
            
            if (i + 1) % 499 == 0:
                batch.commit()
                batch = st.session_state['db'].batch() 
                
        batch.commit()
        
        log_event("DATA_SAVE_SUCCESS", f"Dados de {product_name} ({source_type}) salvos em {collection_name}. Total: {len(data_records)} registros.")
        return True, f"Dados de {product_name} ({source_type}) salvos com sucesso."
    
    except Exception as e:
        log_event("DATA_SAVE_FAIL", f"Falha ao salvar dados de {product_name} ({source_type}): {e}")
        return False, f"Erro ao salvar dados de {product_name} no Firestore: {e}"

# Cria aliases
save_processed_data_to_firestore = lambda product_name, df_data: save_data_to_firestore(product_name, df_data, 'UPLOAD')

@st.cache_data(ttl=3600, show_spinner="Buscando dados da API e armazenando...")
def fetch_api_and_save(product_name: str, start_date: date, end_date: date):
    """
    Busca os dados via API Mock (funções em utils/data_processing), salva no Firestore e retorna o DataFrame.
    """
    from utils.data_processing import fetch_asto_data, fetch_eliq_data
    
    if product_name == 'Asto':
        df_raw = fetch_asto_data(start_date, end_date)
    elif product_name == 'Eliq':
        df_raw = fetch_eliq_data(start_date, end_date)
    else:
        return pd.DataFrame()
        
    if df_raw.empty:
        log_event("API_FETCH_EMPTY", f"API de {product_name} retornou dados vazios.")
        return pd.DataFrame()
        
    # Salva o resultado no Firestore
    success, message = save_data_to_firestore(product_name, df_raw, 'API')

    return df_raw
