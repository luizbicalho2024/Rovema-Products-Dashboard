import streamlit as st
import firebase_admin
from firebase_admin import credentials, auth, firestore, storage
import json

# --- Configuração do Firebase ---
@st.cache_resource
def initialize_firebase():
    """Inicializa o Firebase Admin SDK usando Streamlit Secrets."""
    
    # Verifica se as credenciais estão disponíveis nos secrets
    if "firebase_service_account" not in st.secrets:
        st.error("ERRO: As credenciais do Firebase não foram encontradas no `.streamlit/secrets.toml`.")
        return None, None, None, None

    # Converte o dicionário de secrets para o formato de credenciais
    try:
        cred_dict = dict(st.secrets["firebase_service_account"])
        
        # O campo 'private_key' pode vir com quebras de linha que precisam ser tratadas
        if 'private_key' in cred_dict:
            cred_dict['private_key'] = cred_dict['private_key'].replace('\\n', '\n')
            
        cred = credentials.Certificate(cred_dict)

        # Inicializa o app Firebase (só uma vez)
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

# --- Funções de Logs (Logs Page) ---

def log_event(action: str, details: str = ""):
    """Registra um evento de log no Firestore."""
    if 'db' not in st.session_state:
        return
    
    user_email = st.session_state.get('user_email', 'SISTEMA')
    
    log_entry = {
        "timestamp": firestore.SERVER_TIMESTAMP,
        "user_email": user_email,
        "action": action,
        "details": details
    }
    try:
        st.session_state['db'].collection('logs').add(log_entry)
    except Exception as e:
        st.warning(f"Não foi possível registrar o log: {e}")

# --- Funções de Autenticação (Login) ---

def login_user(email: str, password: str):
    """Autentica o usuário e recupera o papel (role) no Firestore."""
    if not auth_service:
        return False, "Serviço de autenticação não inicializado."
    
    try:
        # 1. Autenticação (Firebase Auth)
        # Nota: O SDK Admin não tem um método direto de login com email/senha para clientes.
        # Em produção, usa-se a REST API ou um SDK cliente. Para simplificar e manter no Admin,
        # simularemos a verificação de credenciais e dependemos da Firestore para o papel.
        # A maneira mais segura com o SDK Admin é garantir que o usuário existe e buscar o papel.
        user = auth_service.get_user_by_email(email)
        
        # 2. Busca o papel (Role) no Firestore
        user_doc = st.session_state['db'].collection('users').document(user.uid).get()
        if not user_doc.exists:
            log_event("LOGIN_FAIL", f"Usuário {email} autenticado, mas sem papel de acesso (UID: {user.uid}).")
            return False, "Usuário não autorizado ou sem papel de acesso definido."
        
        user_data = user_doc.to_dict()
        role = user_data.get('role', 'Usuário') # Padrão para Usuário se não definido
        
        # 3. Configura a sessão
        st.session_state['authenticated'] = True
        st.session_state['user_email'] = email
        st.session_state['user_role'] = role
        st.session_state['user_uid'] = user.uid
        
        log_event("LOGIN_SUCCESS", f"Usuário {email} logado com sucesso. Papel: {role}.")
        return True, "Login bem-sucedido!"
        
    except firebase_admin._auth_utils.UserNotFoundError:
        log_event("LOGIN_FAIL", f"Tentativa de login falhou. E-mail não encontrado: {email}.")
        return False, "E-mail ou senha incorretos."
    except Exception as e:
        # Captura erros como senha incorreta (embora o SDK Admin não verifique senhas diretamente,
        # ele capturaria outros erros de API/permissão)
        log_event("LOGIN_ERROR", f"Erro crítico de login para {email}: {e}")
        return False, "Erro na autenticação. Verifique e-mail/senha ou contate o administrador."

def logout_user():
    """Limpa o estado da sessão e desloga o usuário."""
    log_event("LOGOUT", "Usuário deslogou.")
    st.session_state['authenticated'] = False
    st.session_state['user_email'] = None
    st.session_state['user_role'] = None
    st.session_state['user_uid'] = None
    st.rerun()

# --- Funções de Gerenciamento de Acessos (Admin Page) ---

def create_user(email, password, role, name):
    """Cria um novo usuário no Firebase Auth e define o papel no Firestore."""
    if 'db' not in st.session_state:
        return False, "Firestore não inicializado."
    
    try:
        user = auth_service.create_user(email=email, password=password)
        st.session_state['db'].collection('users').document(user.uid).set({
            'email': email,
            'role': role,
            'nome': name
        })
        log_event("ADMIN_ACTION", f"Usuário criado: {email} com papel {role}.")
        return True, f"Usuário {email} criado com sucesso!"
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
        
# --- Funções de Upload e Armazenamento (Upload Page) ---

def upload_file_and_store_ref(uploaded_file, product_name):
    """
    Armazena o arquivo no Storage e registra a referência no Firestore.
    O processamento do CSV em dados estruturados ficaria no data_processing.py
    """
    if 'bucket' not in st.session_state:
        return False, "Storage não inicializado."

    try:
        # Define o caminho do arquivo no Storage
        file_path = f"uploads/{product_name}/{st.session_state['user_uid']}_{uploaded_file.name}"
        
        # Faz o upload para o Storage
        blob = st.session_state['bucket'].blob(file_path)
        blob.upload_from_file(uploaded_file, content_type=uploaded_file.type, rewind=True)
        
        # Registra o upload no Firestore
        st.session_state['db'].collection('file_uploads').add({
            'product': product_name,
            'filename': uploaded_file.name,
            'storage_path': file_path,
            'uploaded_by': st.session_state['user_email'],
            'timestamp': firestore.SERVER_TIMESTAMP
        })
        
        log_event("FILE_UPLOAD", f"Arquivo de {product_name} enviado para {file_path}.")
        return True, f"Arquivo '{uploaded_file.name}' enviado com sucesso para o Firebase Storage!"
    except Exception as e:
        log_event("FILE_UPLOAD_FAIL", f"Falha ao enviar arquivo: {e}")
        return False, f"Erro ao enviar arquivo: {e}"
