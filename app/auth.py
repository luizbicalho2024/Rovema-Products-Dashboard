# app/auth.py

import streamlit as st
from firebase_admin import auth

def login_user(db_client, email, password):
    """
    Função de login mais robusta (ainda com MOCK de senha).
    Em PROD, esta função usaria a API REST do Firebase para login de usuário.
    """
    try:
        # 1. Busca o usuário pelo e-mail
        user = auth.get_user_by_email(email)
        
        # 2. MOCK de Senha e Sessão (Simulando sucesso de token)
        if password == "logpay123": 
            st.session_state['logged_in'] = True
            st.session_state['user_email'] = email
            st.session_state['user_uid'] = user.uid
            st.session_state['user_name'] = user.display_name or "Gestor"
            st.success(f"Login realizado com sucesso! Bem-vindo(a), {st.session_state['user_name']}.")
            st.rerun()
        else:
            st.error("Falha no Login: Senha inválida (MOCK: use 'logpay123').")

    except Exception:
        st.error("Falha no Login: Usuário não encontrado ou credenciais inválidas.")

def logout():
    """Limpa o estado da sessão."""
    keys_to_delete = ['logged_in', 'user_email', 'user_uid', 'user_name']
    for key in keys_to_delete:
        if key in st.session_state:
            del st.session_state[key]
    st.info("Sessão encerrada.")
    st.rerun()

def login_page(db_client):
    """Renderiza a interface de login."""
    st.title("🔒 Login - Sistema de BI Estratégico")
    
    email = st.text_input("Email")
    password = st.text_input("Senha", type="password")

    if st.button("Entrar"):
        if email and password:
            login_user(db_client, email, password)
        else:
            st.warning("Preencha e-mail e senha.")
