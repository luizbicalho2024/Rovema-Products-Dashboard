import streamlit as st
from fire_admin import get_auth, initialize_firebase
import time

st.set_page_config(layout="centered", page_title="Login")

# Inicializa o Firebase (necessário em todas as páginas)
initialize_firebase()

def login_user(email, password):
    """
    Autentica o usuário. Esta é uma implementação SIMULADA.
    O Firebase Admin SDK (backend) não pode verificar senhas.
    Você precisa do Firebase Client SDK (frontend) para um login real
    ou pode apenas verificar se o usuário existe no Auth.
    """
    try:
        auth = get_auth()
        user = auth.get_user_by_email(email)
        
        # SIMULAÇÃO: Se o usuário existe, o login é bem-sucedido.
        # Em um app real, você não faria isso.
        
        st.session_state.user_email = user.email
        st.session_state.user_uid = user.uid
        st.rerun()

    except Exception as e:
        st.error(f"Erro de login: {e}")
        st.error("Verifique seu e-mail. Para esta demo, a senha não é validada.")

# --- Interface de Login ---
if "user_email" not in st.session_state:
    st.title("Painel de Controle Unificado 🚀")
    st.subheader("Por favor, faça o login para continuar")

    with st.form("login_form"):
        email = st.text_input("Email")
        password = st.text_input("Senha", type="password") # Senha é coletada mas não validada aqui
        submitted = st.form_submit_button("Entrar")

        if submitted:
            if email and password:
                with st.spinner("Autenticando..."):
                    login_user(email, password)
            else:
                st.warning("Por favor, preencha email e senha.")
else:
    st.success(f"Login como **{st.session_state.user_email}** realizado!")
    st.write("Selecione um dashboard na barra lateral para começar.")
    st.page_link("pages/1_🏠_Visao_Geral.py", label="Ir para o Dashboard", icon="🏠")

    if st.button("Sair"):
        for key in st.session_state.keys():
            del st.session_state[key]
        st.rerun()
