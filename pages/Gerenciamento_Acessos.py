import streamlit as st
import pandas as pd
from fire_admin import get_all_users, create_user, log_event

def access_management_page():
    if not st.session_state.get('authenticated'):
        st.error("Acesso negado. Por favor, faça login na página principal.")
        return

    # Verificação de Autorização (Admin)
    if st.session_state.get('user_role') != 'Admin':
        st.error("Permissão negada. Apenas usuários 'Admin' podem acessar esta página.")
        log_event("ACCESS_ATTEMPT", "Tentativa de acesso à Gestão de Acessos por não-Admin.")
        return

    st.title("🔒 Gerenciamento de Acessos")
    log_event("VIEW_ACCESS_MANAGEMENT", "Acessando a página de gestão de usuários.")

    tab1, tab2 = st.tabs(["Criar Novo Usuário", "Visualizar Usuários Existentes"])

    # --- TAB 1: Criar Novo Usuário ---
    with tab1:
        st.subheader("Cadastro de Novo Acesso")
        with st.form("create_user_form"):
            new_email = st.text_input("E-mail do Usuário (Deve ser único)")
            new_name = st.text_input("Nome Completo")
            new_password = st.text_input("Senha Inicial", type="password")
            new_role = st.selectbox("Nível de Acesso", ['Usuário', 'Admin'])
            
            if st.form_submit_button("Criar Usuário"):
                if new_email and new_password and new_role and new_name:
                    with st.spinner("Criando usuário e definindo papel..."):
                        success, message = create_user(new_email, new_password, new_role, new_name)
                    
                    if success:
                        st.success(message)
                    else:
                        st.error(message)
                else:
                    st.warning("Por favor, preencha todos os campos.")

    # --- TAB 2: Visualizar Usuários Existentes ---
    with tab2:
        st.subheader("Usuários Registrados (Firestore)")
        
        # O cache é importante para não sobrecarregar o Firestore
        @st.cache_data(ttl=30) 
        def fetch_users():
            return get_all_users()
            
        users = fetch_users()
        
        if users:
            df = pd.DataFrame(users)
            df_display = df[['nome', 'email', 'role', 'uid']]
            df_display.columns = ['Nome', 'E-mail', 'Papel', 'UID Firebase']
            st.dataframe(df_display, use_container_width=True)
            st.info(f"Total de usuários: {len(users)}")
        else:
            st.info("Nenhum usuário encontrado na base de dados.")
            st.warning("Se você acabou de criar um usuário, verifique se a criação no Firestore foi bem-sucedida.")

# Garante que a função da página é chamada
if st.session_state.get('authenticated'):
    access_management_page()
else:
    pass
