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
        print(f"Erro Crítico de Inicialização do Firebase: {e}") # Já imprime o erro aqui
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
        print(f"WARN: DB nulo ao tentar logar: {action} - {details}") # Log no console
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
    except Exception as e:
        st.toast(f"Não foi possível registrar o log.", icon="⚠️")
        print(f"ERRO ao registrar log: {e}") # Adicionado


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

        # Garante que o DB esteja inicializado ANTES de tentar ler
        if db is None:
            print("WARN Login: DB era nulo, tentando inicializar...")
            if not initialize_firebase():
                log_event("LOGIN_ERROR", "Firestore indisponível devido a falha nas credenciais iniciais.")
                return False, "Erro crítico de serviço: Contate o administrador."
            db = st.session_state.get('db') # Tenta pegar novamente após reinicializar
            if db is None: # Se ainda assim falhar
                 log_event("LOGIN_ERROR", "Falha ao obter DB mesmo após re-inicialização.")
                 return False, "Erro crítico de serviço: Falha na conexão com DB."


        user_doc = db.collection('users').document(user_uid).get()
        if not user_doc.exists:
            log_event("LOGIN_FAIL", f"Usuário {email} autenticado, mas sem papel de acesso (UID: {user_uid}).")
            return False, "Usuário autenticado, mas sem papel de acesso definido. Contate o administrador."

        user_data = user_doc.to_dict()
        role = user_data.get('role', 'Usuário')

        # **VERIFICAÇÃO DE STATUS**
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
        print(f"ERRO HTTP Login ({email}): {error_code} | Response: {e.response.text}") # Adicionado
        return False, f"Erro na autenticação. Contate o administrador. (Código: {error_code})"

    except Exception as e:
        log_event("LOGIN_ERROR", f"Erro inesperado de login para {email}: {e}")
        print(f"ERRO Inesperado Login ({email}): {e}") # Adicionado
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
        print(f"ERRO Create User ({email}): {e}") # Adicionado
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
        print(f"ERRO Update User ({uid}): {e}") # Adicionado
        return False, f"Erro ao atualizar usuário: {e}"


def get_all_users(role_filter: str = None):
    """
    Retorna a lista completa de usuários e seus papéis.
    [ATUALIZADO] Permite filtrar por 'role'.
    """
    db = st.session_state.get('db')
    if db is None:
         print("ERRO Firestore (Users): Conexão DB nula.") # Adicionado
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
        print(f"ERRO Firestore (Users): {e}") # Adicionado para visibilidade no console
        return []

def get_all_consultores():
    """Retorna uma lista de nomes de usuários ATIVOS para filtros."""
    # Filtra apenas por usuários ativos com a role 'Usuário'
    users = get_all_users(role_filter='Usuário')
    if users:
        nomes = sorted([
            user.get('nome', 'N/A') for user in users
            if user.get('nome') and user.get('status', 'ativo') == 'ativo' # Filtra por status ativo
        ])
        return ['Todos'] + nomes
    return ['Todos']


# --- 5. FUNÇÃO: SALVAR DADOS NO FIRESTORE (API e Upload) ---

def save_data_to_firestore(product_name: str, df_data: pd.DataFrame, source_type: str):
    """Salva o DataFrame processado (de API ou Upload) no Firestore."""
    db = st.session_state.get('db')
    if db is None:
        print(f"ERRO Save Data ({product_name}): Conexão DB nula.") # Adicionado
        return False, "Firestore não inicializado."

    # Remove colunas que são apenas índices do Pandas, se existirem
    df_data = df_data.reset_index(drop=True)
    if 'index' in df_data.columns:
        df_data = df_data.drop(columns=['index'])
    if 'level_0' in df_data.columns:
         df_data = df_data.drop(columns=['level_0'])


    # Converte Timestamps do Pandas para Datetime do Python antes de salvar
    for col in df_data.select_dtypes(include=['datetime64[ns]', 'datetime64[ns, UTC]']).columns:
         # Usa .to_pydatetime() para converter corretamente, tratando NaT
         df_data[col] = df_data[col].apply(lambda x: x.to_pydatetime() if pd.notna(x) else None)


    try:
        # Tenta converter para dict records *depois* da limpeza de tipos
        data_records = df_data.to_dict('records')
    except Exception as e:
         log_event("DATA_SAVE_FAIL", f"Erro ao converter DataFrame para dict ({product_name}): {e}")
         print(f"ERRO Convert to Dict ({product_name}): {e}") # Adicionado
         return False, f"Erro interno ao preparar dados para salvar: {e}"


    upload_metadata = {
        'product': product_name,
        'source_type': source_type,
        'uploaded_by': st.session_state.get('user_email', 'SISTEMA'),
        'timestamp': server_timestamp,
        'total_records': len(data_records),
    }

    try:
        upload_doc_ref = db.collection('data_metadata').add(upload_metadata)
        # Firestore retorna uma tupla (timestamp, DocumentReference) no add()
        upload_id = upload_doc_ref[1].id if isinstance(upload_doc_ref, tuple) else upload_doc_ref.id

        collection_name = f"data_{product_name.lower().replace(' ', '_')}"

        batch = db.batch()
        commit_count = 0

        for i, record in enumerate(data_records):
            # Adiciona metadados a cada registro
            record['data_source_id'] = upload_id
            record['ingestion_timestamp'] = server_timestamp # Timestamp do Firestore

            # Limpeza final de valores NaN/NaT para None (Firestore aceita None)
            cleaned_record = {}
            for key, value in record.items():
                # Verifica se é NaN do Pandas/Numpy ou NaT do Pandas
                if pd.isna(value):
                    cleaned_record[key] = None
                # Converte explicitamente np.int64 para int nativo se necessário (raro, mas pode ocorrer)
                elif isinstance(value, np.integer):
                     cleaned_record[key] = int(value)
                elif isinstance(value, np.floating):
                     cleaned_record[key] = float(value)
                else:
                    cleaned_record[key] = value

            # Cria uma referência de documento vazia para obter um ID automático
            doc_ref = db.collection(collection_name).document()
            batch.set(doc_ref, cleaned_record)

            # Commita o batch a cada 499 operações para evitar limites
            if (i + 1) % 499 == 0:
                batch.commit()
                commit_count += 1
                batch = db.batch() # Inicia um novo batch

        # Commita o restante do batch (se houver)
        if (i + 1) % 499 != 0:
            batch.commit()
            commit_count += 1

        log_event("DATA_SAVE_SUCCESS", f"Dados de {product_name} ({source_type}) salvos em {collection_name}. Total: {len(data_records)} registros em {commit_count} commits.")
        return True, f"Dados de {product_name} ({source_type}) salvos com sucesso. Total: {len(data_records)} registros."

    except Exception as e:
        log_event("DATA_SAVE_FAIL", f"Falha ao salvar dados de {product_name} ({source_type}) no Firestore: {e}")
        print(f"ERRO Save Data Batch ({product_name}): {e}") # Adicionado
        return False, f"Erro ao salvar dados de {product_name} no Firestore: {e}"

# Cria alias (lambda não precisa mudar)
save_processed_data_to_firestore = lambda product_name, df_data: save_data_to_firestore(product_name, df_data, 'UPLOAD')
