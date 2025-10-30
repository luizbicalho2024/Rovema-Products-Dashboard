import streamlit as st
from utils.auth import login_user

# --- Configuração da Página ---
st.set_page_config(
    page_title="BI Comercial - Login",
    page_icon="logoRB.png",
    layout="centered",
    initial_sidebar_state="collapsed"
)

# --- Lógica de Redirecionamento ---
# Se o usuário já está logado, manda direto para o Dashboard
if "authenticated" in st.session_state and st.session_state.authenticated:
    st.switch_page("pages/1_📈_Dashboard_Geral.py")

# --- Layout da Página de Login ---
col1, col2, col3 = st.columns([1, 2, 1])

with col2:
    st.image("logoRB.png", use_column_width=True)
    st.title("BI Comercial")
    st.markdown("Por favor, faça o login para continuar.")

    with st.form(key="login_form"):
        email = st.text_input("Email", placeholder="seu.email@empresa.com")
        password = st.text_input("Senha", type="password", placeholder="********")
        submit_button = st.form_submit_button("Entrar", use_container_width=True)

        if submit_button:
            if not email or not password:
                st.error("Por favor, preencha todos os campos.")
            else:
                with st.spinner("Autenticando..."):
                    success, message = login_user(email, password)
                    if success:
                        st.toast("Login bem-sucedido!", icon="🎉")
                        # Redirecionamento após o login
                        st.switch_page("pages/1_📈_Dashboard_Geral.py")
                    else:
                        st.error(f"Erro: {message}")
