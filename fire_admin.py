import streamlit as st
import firebase_admin
from firebase_admin import credentials, auth, firestore
import json
import requests
import pandas as pd
from google.cloud.firestore import SERVER_TIMESTAMP as server_timestamp 
from datetime import date
import os

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

    if not api_key:
        api_key = os.environ.get("FIREBASE_WEB_API_KEY", "")
        api_key = api_key.strip() if isinstance(api_key, str) else None

    return sa_config, api_key

# Obter credenciais globalmente
CREDENTIALS_DICT, FIREBASE_WEB_API_KEY = get_credentials_dict()

def initialize_firebase():
    """
    Inicializa o Firebase Admin SDK e o Firestore.
    [ROBUSTO] Verifica se a conexão já existe no st.session_state
    antes de criar uma nova.
    """
    
    # Se o DB já está inicializado no state, não faz nada.
    if 'db' in st.session_state and st.session_state['db'] is not None:
        return True

    if CREDENTIALS_DICT is None:
        st.error("Credenciais do Firebase (firebase_service_account) não encontradas no st.secrets.")
        return False

    try:
        cred = credentials.Certificate(CREDENTIALS_DICT)

        if not firebase_admin._apps:
            firebase_admin.initialize_app(cred)
            
        db = firestore.client()
        
        # Salva os objetos de serviço no st.session_state
        st.session_state['db'] = db
        st.session_state['auth_service'] = auth
        st.session_state['bucket'] = None # Bucket não está sendo usado
        st.session_state['project_id'] = CREDENTIALS_DICT.get('project_id')
        
        return True
        
    except Exception as e:
        print(f"Erro Crítico de Inicialização do Firebase: {e}")
        st.error(f"Erro ao inicializar o Firebase: Falha na validação das credenciais. Verifique o secrets.toml. Erro: {e}")
        # Garante que o estado seja limpo se a inicialização falhar
        st.session_state['db'] = None
        st.session_state['auth_service'] = None
        return False

# --- 2. FUNÇÕES DE LOGS (Auditoria) ---

def log_event(action: str, details: str = ""):
    """Registra um evento de log no Firestore."""
    db = st.session_state.get('db')
    if db is None:
        return
    
    user_email = st.session_state.get('user_email', 'SISTEMA')
    
    log_entry = {
        "timestamp": server_timestamp,
        "user_email": user_email,
        "action": action,
        "details": details
    }
    try:
        db.collection('logs').add(log_entry)
    except Exception:
        st.toast(f"Não foi possível registrar o log.", icon="⚠️")


# --- 3. FUNÇÕES DE AUTENTICAÇÃO (Login e Logout) ---

def login_user(email: str, password: str):
    """Autentica o usuário e recupera o papel (role) no Firestore."""
    
    db = st.session_state.get('db')

    if not FIREBASE_WEB_API_KEY:
        log_event("LOGIN_ERROR", "Chave da API Web não configurada ou vazia nos secrets.")
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

        if db is None:
            if not initialize_firebase():
                log_event("LOGIN_ERROR", "Firestore indisponível devido a falha nas credenciais iniciais.")
                return False, "Erro crítico de serviço: Contate o administrador."
            db = st.session_state.get('db')

        user_doc = db.collection('users').document(user_uid).get()
        if not user_doc.exists:
            log_event("LOGIN_FAIL", f"Usuário {email} autenticado, mas sem papel de acesso (UID: {user_uid}).")
            return False, "Usuário autenticado, mas sem papel de acesso definido. Contate o administrador."
        
        user_data = user_doc.to_dict()
        role = user_data.get('role', 'Usuário')
        
        # **NOVA VERIFICAÇÃO DE STATUS**
        status = user_data.get('status', 'ativo')
        if status == 'inativo':
            log_event("LOGIN_FAIL", f"Tentativa de login de usuário inativo: {email}.")
            return False, "Esta conta de usuário está desabilitada. Contate o administrador."
        
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
    
    keys_to_preserve = ['db', 'auth_service', 'bucket', 'project_id']
    preserved_state = {k: st.session_state[k] for k in keys_to_preserve if k in st.session_state}
    
    st.session_state.clear()
    
    st.session_state.update(preserved_state)
    
    st.session_state['authenticated'] = False
    
    st.rerun()

# --- 4. FUNÇÕES DE GERENCIAMENTO DE ACESSOS (Admin) ---

def create_user(email, password, role, name):
    """
    Cria um novo usuário no Firebase Auth e define o papel no Firestore.
    [ATUALIZADO] Adiciona campos padrão de status e carteira.
    """
    db = st.session_state.get('db')
    auth_service = st.session_state.get('auth_service')

    if db is None or auth_service is None:
        return False, "Serviços Firebase indisponíveis. Contate o administrador."
    
    try:
        user = auth_service.create_user(email=email, password=password)
        
        # Adiciona os novos campos padrão
        db.collection('users').document(user.uid).set({
            'email': email,
            'role': role,
            'nome': name,
            'status': 'ativo',       # Novo
            'carteira_cnpjs': []   # Novo
        })
        log_event("ADMIN_ACTION", f"Usuário criado: {email} com papel {role}.")
        return True, f"Usuário {email} criado com sucesso! UID: {user.uid}"
    except Exception as e:
        log_event("ADMIN_ACTION_FAIL", f"Falha ao criar usuário {email}: {e}")
        return False, f"Erro ao criar usuário: {e}"

def update_user_details(uid: str, data_dict: dict):
    """
    [NOVO] Atualiza os dados de um usuário na coleção 'users' do Firestore.
    """
    db = st.session_state.get('db')
    if db is None:
        return False, "Firestore indisponível."
        
    try:
        db.collection('users').document(uid).update(data_dict)
        log_event("ADMIN_ACTION", f"Detalhes do usuário {uid} atualizados. Dados: {data_dict}")
        return True, "Usuário atualizado com sucesso."
    except Exception as e:
        log_event("ADMIN_ACTION_FAIL", f"Falha ao atualizar usuário {uid}: {e}")
        return False, f"Erro ao atualizar usuário: {e}"


def get_all_users(role_filter: str = None):
    """
    Retorna a lista completa de usuários e seus papéis.
    [ATUALIZADO] Permite filtrar por 'role'.
    """
    db = st.session_state.get('db')
    if db is None:
        return []
    
    try:
        users_ref = db.collection('users')
        
        # Aplica o filtro se fornecido
        if role_filter:
            query = users_ref.where('role', '==', role_filter)
        else:
            query = users_ref
            
        docs = query.stream()
        user_list = []
        for doc in docs:
            user_data = doc.to_dict()
            user_data['uid'] = doc.id
            user_list.append(user_data)
            
        return user_list
    except Exception as e:
        st.error(f"Erro ao buscar usuários: {e}")
        return []

def get_all_consultores():
    """Retorna uma lista de nomes de usuários para filtros."""
    # Filtra apenas por usuários ativos com a role 'Usuário'
    users = get_all_users(role_filter='Usuário')
    if users:
        nomes = sorted([
            user.get('nome', 'N/A') for user in users 
            if user.get('nome') and user.get('status', 'ativo') == 'ativo'
        ])
        return ['Todos'] + nomes
    return ['Todos']


# --- 5. FUNÇÃO: SALVAR DADOS NO FIRESTORE (API e Upload) ---

def save_data_to_firestore(product_name: str, df_data: pd.DataFrame, source_type: str):
    """Salva o DataFrame processado (de API ou Upload) no Firestore."""
    db = st.session_state.get('db')
    if db is None:
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
        upload_doc_ref = db.collection('data_metadata').add(upload_metadata)
        upload_id = upload_doc_ref[1].id if isinstance(upload_doc_ref, tuple) else upload_doc_ref.id

        collection_name = f"data_{product_name.lower().replace(' ', '_')}"
        
        batch = db.batch()
        
        for i, record in enumerate(data_records):
            record['data_source_id'] = upload_id 
            record['ingestion_timestamp'] = server_timestamp
            
            cleaned_record = {}
            for key, value in record.items():
                if pd.isna(value):
                    cleaned_record[key] = None
                else:
                    cleaned_record[key] = value
            
            doc_ref = db.collection(collection_name).document()
            batch.set(doc_ref, cleaned_record)
            
            if (i + 1) % 499 == 0:
                batch.commit()
                batch = db.batch() 
                
        batch.commit()
        
        log_event("DATA_SAVE_SUCCESS", f"Dados de {product_name} ({source_type}) salvos em {collection_name}. Total: {len(data_records)} registros.")
        return True, f"Dados de {product_name} ({source_type}) salvos com sucesso. Total: {len(data_records)} registros."
    
    except Exception as e:
        log_event("DATA_SAVE_FAIL", f"Falha ao salvar dados de {product_name} ({source_type}): {e}")
        return False, f"Erro ao salvar dados de {product_name} no Firestore: {e}"

# Cria aliases
save_processed_data_to_firestore = lambda product_name, df_data: save_data_to_firestore(product_name, df_data, 'UPLOAD')
