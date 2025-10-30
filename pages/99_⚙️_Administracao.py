import streamlit as st
import pandas as pd
import sys
import os
from datetime import datetime
import asyncio 

# CORREÇÃO PARA 'KeyError: utils': Adiciona o diretório raiz ao path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from utils.auth import auth_guard, check_role
from utils.firebase_config import get_db, get_admin_auth
from utils.data_processing import (
    process_bionio_csv, 
    process_rovema_csv,
    process_asto_api,
    process_eliq_api
)

# --- 1. Proteção da Página ---
auth_guard()
# Apenas Admins podem ver esta página
check_role(["admin"])

st.title("⚙️ Painel de Administração")

# --- 2. Carrega dados de suporte (usuários, clientes) ---
@st.cache_data(ttl=300)
def get_all_users_and_clients():
    db = get_db()
    
    # Busca todos os usuários (consultores e gestores)
    users_ref = db.collection("users").stream()
    users_list = []
    for user in users_ref:
        data = user.to_dict()
        users_list.append({
            "uid": user.id,
            "name": data.get("name"),
            "email": data.get("email"),
            "role": data.get("role")
        })
    
    # Busca todos os clientes
    clients_ref = db.collection("clients").stream()
    clients_list = []
    for client in clients_ref:
        data = client.to_dict()
        clients_list.append({
            "cnpj": client.id,
            "name": data.get("client_name"),
            "consultant_uid": data.get("consultant_uid")
        })
        
    return users_list, clients_list

try:
    users, clients = get_all_users_and_clients()
except Exception as e:
    st.error(f"Erro crítico ao conectar ao Firestore: {e}")
    st.info("Verifique se as credenciais [firebase_service_account] estão corretas nos Secrets do Streamlit Cloud.")
    st.stop()
    

# --- 3. Layout em Abas ---
tab1, tab2, tab3, tab4 = st.tabs([
    "📊 Gestão de Clientes (Carteiras)",
    "👥 Gestão de Usuários",
    "📄 Carga de Dados (CSV)",
    "☁️ Carga de Dados (API)"
])


# --- ABA 1: GESTÃO DE CLIENTES (Obrigatório para o sistema funcionar) ---
with tab1:
    st.header("Atribuir Clientes a Consultores")
    st.info("""
    Este é o módulo mais importante. As vendas dos produtos de API (ASTO, ELIQ) e CSVs 
    só serão atribuídas a um consultor se o CNPJ do cliente estiver mapeado aqui.
    """)
    
    # Listas para os filtros
    consultant_dict = {u['uid']: f"{u['name']} ({u['email']})" for u in users if u['role'] == 'consultant'}
    client_dict = {c['cnpj']: c['name'] for c in clients}

    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Atribuir/Atualizar Cliente")
        
        # Filtro para encontrar clientes
        client_cnpj_list = ["Novo Cliente"] + [f"{name} ({cnpj})" for cnpj, name in client_dict.items()]
        selected_client_str = st.selectbox("Buscar Cliente por Nome", client_cnpj_list, index=0)
        
        if selected_client_str == "Novo Cliente":
            client_cnpj = st.text_input("CNPJ do Novo Cliente (apenas números)", max_chars=14)
            client_name = st.text_input("Nome Fantasia do Novo Cliente")
        else:
            client_cnpj = selected_client_str.split('(')[-1].replace(')', '')
            client_name = client_dict[client_cnpj]
            st.text_input("CNPJ", value=client_cnpj, disabled=True)
            st.text_input("Nome Fantasia", value=client_name, disabled=True)
            
        selected_consultant_uid = st.selectbox(
            "Atribuir ao Consultor:",
            options=list(consultant_dict.keys()),
            format_func=lambda uid: consultant_dict[uid]
        )
        
        if st.button("Salvar Associação", type="primary"):
            if not client_cnpj or not client_name or not selected_consultant_uid:
                st.error("Preencha todos os campos!")
            else:
                db = get_db()
                admin_auth = get_admin_auth()
                
                # Busca o gestor do consultor
                try:
                    consultant_doc = db.collection("users").document(selected_consultant_uid).get()
                    manager_uid = consultant_doc.to_dict().get("manager_uid")
                except Exception as e:
                    st.error(f"Erro ao buscar gestor do consultor: {e}")
                    manager_uid = None
                
                client_data = {
                    "client_name": client_name,
                    "consultant_uid": selected_consultant_uid,
                    "manager_uid": manager_uid, # Denormalizado para performance
                    "updated_at": datetime.now()
                }
                
                # Salva no Firestore usando o CNPJ limpo como ID do documento
                clean_cnpj_val = "".join(filter(str.isdigit, client_cnpj))
                db.collection("clients").document(clean_cnpj_val).set(client_data, merge=True)
                
                st.success(f"Cliente '{client_name}' salvo e associado a {consultant_dict[selected_consultant_uid]}!")
                st.cache_data.clear() # Limpa o cache para recarregar a lista
                st.rerun()

    with col2:
        st.subheader("Carteiras Atuais")
        clients_df = pd.DataFrame(clients)
        # Mapeia UID para Nome para visualização
        consultant_name_map = {u['uid']: u['name'] for u in users}
        clients_df['consultant_name'] = clients_df['consultant_uid'].map(consultant_name_map).fillna("N/A")
        # CORREÇÃO PARA O DATAFRAME:
        st.dataframe(clients_df[['cnpj', 'name', 'consultant_name']], width='stretch')


# --- ABA 2: GESTÃO DE USUÁRIOS ---
with tab2:
    st.header("Criar e Gerenciar Usuários")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Criar Novo Usuário")
        
        # Lista de Gestores para atribuição
        manager_dict = {u['uid']: u['name'] for u in users if u['role'] == 'manager'}
        
        with st.form("new_user_form", clear_on_submit=True):
            name = st.text_input("Nome Completo")
            email = st.text_input("Email")
            password = st.text_input("Senha Temporária", type="password")
            role = st.selectbox("Nível de Acesso", ["consultant", "manager", "admin"])
            
            manager_uid = None
            if role == "consultant":
                manager_uid = st.selectbox(
                    "Gestor Responsável",
                    options=list(manager_dict.keys()),
                    format_func=lambda uid: manager_dict[uid]
                )
            
            # CORREÇÃO PARA O BOTÃO:
            submit_user = st.form_submit_button("Criar Usuário", width='stretch')
            
            if submit_user:
                if not name or not email or not password or not role:
                    st.error("Preencha todos os campos!")
                else:
                    try:
                        admin_auth = get_admin_auth()
                        db = get_db()
                        
                        # 1. Cria o usuário no Firebase Authentication
                        user_record = admin_auth.create_user(
                            email=email,
                            password=password,
                            display_name=name
                        )
                        
                        # 2. Salva os dados (role, manager) no Firestore
                        user_data = {
                            "name": name,
                            "email": email,
                            "role": role,
                            "manager_uid": manager_uid if role == "consultant" else None
                        }
                        db.collection("users").document(user_record.uid).set(user_data)
                        
                        st.success(f"Usuário '{name}' criado com sucesso (UID: {user_record.uid})!")
                        st.cache_data.clear()
                        st.rerun()
                        
                    except Exception as e:
                        st.error(f"Erro ao criar usuário: {e}")

    with col2:
        st.subheader("Usuários Existentes")
        users_df = pd.DataFrame(users)
        # CORREÇÃO PARA O DATAFRAME:
        st.dataframe(users_df, width='stretch')


# --- ABA 3: CARGA DE DADOS (CSV) ---
with tab3:
    st.header("Upload de Arquivos CSV")
    
    st.subheader("Produto: Bionio")
    uploaded_bionio = st.file_uploader("Selecione o arquivo Bionio.csv", type="csv", key="bionio_uploader")
    if uploaded_bionio:
        if st.button("Processar Bionio"):
            with st.spinner("Processando Bionio... Isso pode levar alguns minutos."):
                total = process_bionio_csv(uploaded_bionio)
                if total:
                    st.success(f"Processamento Bionio concluído! {total} registros salvos.")
                    
    st.divider()

    st.subheader("Produto: Rovema Pay")
    uploaded_rovema = st.file_uploader("Selecione o arquivo RovemaPay.csv", type="csv", key="rovema_uploader")
    if uploaded_rovema:
        if st.button("Processar Rovema Pay"):
            with st.spinner("Processando Rovema Pay... Isso pode levar alguns minutos."):
                total = process_rovema_csv(uploaded_rovema)
                if total:
                    st.success(f"Processamento Rovema Pay concluído! {total} registros salvos.")


# --- ABA 4: CARGA DE DADOS (API) ---
with tab4:
    st.header("Carga de Dados via API")
    st.info("""
    As credenciais das APIs são lidas automaticamente dos Secrets do Streamlit Cloud.
    Basta selecionar o período e carregar.
    """)
    
    # Define o período para ambas as APIs
    st.subheader("Selecione o Período de Carga")
    col1, col2 = st.columns(2)
    api_start_date = col1.date_input("Data Inicial", datetime.now().replace(day=1))
    api_end_date = col2.date_input("Data Final", datetime.now())
    
    st.divider()

    st.subheader("Produto: ASTO (Logpay)")
    st.markdown(f"Usando usuário: `{st.secrets.get('api_credentials', {}).get('asto_username', 'N/A')}`")
    
    if st.button("Carregar Dados ASTO"):
        with st.spinner("Buscando dados na API ASTO..."):
            # A função agora lê os secrets internamente
            total = asyncio.run(process_asto_api(api_start_date, api_end_date))
            if total:
                st.success(f"Carga ASTO concluída! {total} registros salvos.")
                    
    st.divider()
    
    st.subheader("Produto: ELIQ (Uzzipay)")
    st.markdown(f"Usando URL: `{st.secrets.get('api_credentials', {}).get('eliq_url', 'N/A')}`")

    if st.button("Carregar Dados ELIQ"):
        with st.spinner("Buscando dados na API ELIQ..."):
            # A função agora lê os secrets internamente
            total = asyncio.run(process_eliq_api(api_start_date, api_end_date))
            if total:
                st.success(f"Carga ELIQ concluída! {total} registros salvos.")
