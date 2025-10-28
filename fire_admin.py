import streamlit as st
import firebase_admin
from firebase_admin import credentials, auth, firestore
import json
import requests
import pandas as pd
from google.cloud.firestore import SERVER_TIMESTAMP as server_timestamp 

# --- 1. CONFIGURAÇÃO E INICIALIZAÇÃO DO FIREBASE ---

@st.cache_resource
def get_web_api_key():
    return st.secrets.get("FIREBASE_WEB_API_KEY")

FIREBASE_WEB_API_KEY = get_web_api_key()

@st.cache_resource
def initialize_firebase():
    """Inicializa o Firebase Admin SDK e o Firestore."""
    
    if "firebase_service_account" not in st.secrets:
        st.error("ERRO: Credenciais do Service Account não encontradas.")
        return None, None, None, None

    try:
        cred_dict = dict(st.secrets["firebase_service_account"])
        
        if 'private_key' in cred_dict:
            cred_dict['private_key'] = cred_dict['private_key'].replace('\\n', '\n')
            
        cred = credentials.Certificate(cred_dict)

        if not firebase_admin._apps:
            # Inicializa o app SEM o storageBucket
            firebase_admin.initialize_app(cred)
            
        db = firestore.client()
        bucket = None # Storage foi removido
        
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
    except Exception:
        st.toast(f"Não foi possível registrar o log.", icon="⚠️")


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
        response = requests.post(API_URL, json=payload)
        response.raise_for_status() 
        
        data = response.json()
        user_uid = data.get('localId')

        if not user_uid:
            log_event("LOGIN_FAIL", f"Autenticação falhou para {email}. UID não retornado.")
            return False, "E-mail ou senha incorretos."

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
    if 'db' not in st.session_state or not auth_service:
        return False, "Serviços Firebase não inicializados."
    
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


# --- 5. FUNÇÃO: SALVAR DADOS PROCESSADOS NO FIRESTORE ---

def save_processed_data_to_firestore(product_name: str, df_data: pd.DataFrame):
    """
    Converte o DataFrame processado em uma lista de dicionários e o salva
    em uma nova coleção no Firestore, usando Batch Writes para eficiência.
    """
    if 'db' not in st.session_state:
        return False, "Firestore não inicializado."

    data_records = df_data.to_dict('records')
    upload_metadata = {
        'product': product_name,
        'uploaded_by': st.session_state['user_email'],
        'timestamp': server_timestamp,
        'total_records': len(data_records),
    }
    
    try:
        # Cria um documento para rastrear este upload
        upload_doc_ref = st.session_state['db'].collection('uploads_metadata').add(upload_metadata)
        upload_id = upload_doc_ref[1].id if isinstance(upload_doc_ref, tuple) else upload_doc_ref.id

        collection_name = f"data_{product_name.lower()}"
        
        batch = st.session_state['db'].batch()
        
        for i, record in enumerate(data_records):
            record['upload_id'] = upload_id 
            record['upload_timestamp'] = server_timestamp
            
            # Adiciona a data de apuração como campo de índice, se existir
            if 'Mês' in record:
                record['period_key'] = record['Mês']

            doc_ref = st.session_state['db'].collection(collection_name).document()
            batch.set(doc_ref, record)
            
            # Limite do batch do Firestore é 500
            if (i + 1) % 499 == 0:
                batch.commit()
                batch = st.session_state['db'].batch() # Inicia um novo batch
                
        # Commita os documentos restantes
        batch.commit()
        
        log_event("DATA_SAVE_SUCCESS", f"Dados de {product_name} salvos em {collection_name}. Total: {len(data_records)} registros.")
        return True, f"Dados processados e salvos com sucesso na coleção '{collection_name}' (Total: {len(data_records)} registros)."
    
    except Exception as e:
        log_event("DATA_SAVE_FAIL", f"Falha ao salvar dados de {product_name}: {e}")
        return False, f"Erro ao salvar dados no Firestore: {e}"
