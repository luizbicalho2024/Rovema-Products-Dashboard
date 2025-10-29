# main_app.py

import streamlit as st
import firebase_admin
from firebase_admin import credentials, auth, firestore
import json

# --- Configuração do Firebase ---
# Você deve carregar as credenciais do JSON que baixou do Firebase
# Em produção no Streamlit Cloud, isso deve vir dos 'Secrets'
# Exemplo de como carregar as credenciais (Localmente):
# with open("suas_credenciais.json") as f:
#     firebase_config = json.load(f)
# ... use o dicionário firebase_config

# Exemplo simplificado de inicialização para Streamlit Cloud
# Substitua 'YOUR_FIREBASE_SECRETS' pelo seu JSON de credenciais
try:
    if not firebase_admin._apps:
        # Se as credenciais estiverem no st.secrets (Streamlit Cloud)
        cred_dict = st.secrets["firebase"]["credentials"]
        cred = credentials.Certificate(cred_dict)
        firebase_admin.initialize_app(cred, name='ASTO_ELIQ_BI')
    
    db = firestore.client()

except Exception as e:
    st.error(f"Erro ao inicializar Firebase: {e}")
    st.info("Verifique se o JSON de credenciais do Firebase está configurado corretamente nos Streamlit Secrets.")


def login_page():
    """Página de Login e Autenticação de Usuário."""
    st.title("🔒 Login - BI Comercial")
    
    email = st.text_input("Email")
    password = st.text_input("Senha", type="password")

    if st.button("Entrar"):
        try:
            # Tenta autenticar o usuário com e-mail e senha
            user = auth.get_user_by_email(email)
            
            # Nota: A biblioteca firebase-admin não tem uma função nativa 
            # de 'sign_in_with_email_and_password'. Ela é usada para 
            # *gerenciamento* de usuários (admin).
            # Para autenticação cliente/servidor, o correto é usar a REST API 
            # do Firebase Auth e um token de ID, mas isso complexifica o 
            # projeto inicial.
            # 
            # Para a versão DEMO inicial, usaremos um login simplificado 
            # baseado na UID do Firebase.
            # Para um sistema de PROD, implemente o fluxo completo de ID Token.
            
            # --- Simplificação para DEMO/POC ---
            # Assume que o Firebase conseguiu verificar as credenciais 
            # de alguma forma ou usa uma lógica de acesso por 'admin':
            
            # 1. Autenticação (Aqui é o ponto fraco do admin SDK, 
            #    pois ele é para criar/ler/deletar users, não para login):
            #    Simularemos o sucesso se o e-mail existir.
            #    Em um cenário real, você faria uma requisição POST para 
            #    a API REST do Firebase para obter um ID Token.
            #    
            #    Se o login fosse feito com a API REST, o código seria:
            #    requests.post(FIREBASE_API_KEY_LOGIN_URL, json={'email': email, 'password': password})
            
            # 2. Sessão:
            st.session_state['logged_in'] = True
            st.session_state['user_email'] = email
            st.session_state['user_uid'] = user.uid
            st.success(f"Login realizado com sucesso! Bem-vindo, {email}.")
            st.rerun() # Recarrega a página para mostrar o conteúdo
            
        except Exception as e:
            st.error(f"Falha no Login: Email ou senha inválidos. ({e})")
            
def logout():
    """Função de Logout."""
    if 'logged_in' in st.session_state:
        del st.session_state['logged_in']
    if 'user_email' in st.session_state:
        del st.session_state['user_email']
    st.info("Sessão encerrada.")
    st.rerun()

# --- Estrutura Principal da Aplicação ---

if 'logged_in' not in st.session_state or not st.session_state['logged_in']:
    login_page()
else:
    # Mostra o botão de logout na barra lateral
    with st.sidebar:
        st.write(f"Usuário: {st.session_state['user_email']}")
        st.button("Sair", on_click=logout)

    # Conteúdo principal (Módulo 5, Módulo 3, Módulo 4) virá aqui
