import streamlit as st
import pandas as pd
import sys
import os
from datetime import datetime
import asyncio 
import calendar

# CORREÇÃO PARA 'KeyError: utils': Adiciona o diretório raiz ao path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from utils.auth import auth_guard, check_role
from utils.firebase_config import get_db, get_admin_auth
from utils.logger import log_audit
from utils.data_processing import (
    process_bionio_csv, 
    process_rovema_csv,
    process_asto_api,
    process_eliq_api
)

# --- 1. Proteção da Página ---
auth_guard()
check_role(["admin"])
st.title("⚙️ Painel de Administração")

# --- 2. Funções de Busca (para todas as abas) ---

@st.cache_data(ttl=300)
def get_all_users_and_clients():
    db = get_db()
    
    # Busca todos os usuários
    users_ref = db.collection("users").stream()
    users_list = []
    consultants_map = {}
    consultants_list_dict = {}
    
    for user in users_ref:
        data = user.to_dict()
        user_data = {
            "uid": user.id,
            "name": data.get("name"),
            "email": data.get("email"),
            "role": data.get("role"),
            "manager_uid": data.get("manager_uid")
        }
        users_list.append(user_data)
        
        # Cria mapas de consultores para as outras funções
        if user_data['role'] == 'consultant':
            consultants_map[user.id] = user_data
            consultants_list_dict[user.id] = user_data['name']
            
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
        
    return pd.DataFrame(users_list), pd.DataFrame(clients_list), consultants_map, consultants_list_dict

@st.cache_data(ttl=300)
def get_goals(month_id):
    """Busca metas do mês (ex: '2025-10')"""
    db = get_db()
    goals_doc = db.collection("goals").document(month_id).get()
    if goals_doc.exists:
        return goals_doc.to_dict()
    return {}

@st.cache_data(ttl=60) # Cache curto
def get_orphan_sales():
    """Busca vendas onde consultant_uid é Nulo."""
    db = get_db()
    sales_ref = db.collection("sales_data")
    query = sales_ref.where("consultant_uid", "==", None).limit(500)
    
    try:
        docs = query.stream()
        orphans = []
        for doc in docs:
            data = doc.to_dict()
            orphans.append({
                "doc_id": doc.id,
                "client_name": data.get("client_name", "N/A"),
                "client_cnpj": data.get("client_cnpj", "N/A"),
                "source": data.get("source", "N/A"),
                "date": data.get("date").strftime("%Y-%m-%d"),
                "revenue_net": data.get("revenue_net", 0)
            })
        if not orphans:
            return pd.DataFrame()
        return pd.DataFrame(orphans)
    except Exception as e:
        st.error(f"Erro ao consultar vendas órfãs: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=60) # Cache curto
def get_audit_logs(limit=100):
    """Busca os logs de auditoria mais recentes."""
    db = get_db()
    logs_ref = db.collection("audit_logs")
    query = logs_ref.order_by("timestamp", direction="DESCENDING").limit(limit)
    
    try:
        docs = query.stream()
        logs = []
        for doc in docs:
            data = doc.to_dict()
            logs.append({
                "timestamp": data.get("timestamp"),
                "user_email": data.get("user_email", "N/A"),
                "action": data.get("action", "N/A"),
                "details": str(data.get("details", {}))
            })
        if not logs:
            return pd.DataFrame()
        return pd.DataFrame(logs)
    except Exception as e:
        st.error(f"Erro ao consultar logs: {e}")
        return pd.DataFrame()

# --- 3. Carregamento de Dados Principal ---
try:
    df_users, df_clients, consultants_map, consultants_list_dict = get_all_users_and_clients()
    df_consultants = df_users[df_users['role'] == 'consultant'].copy()
except Exception as e:
    st.error(f"Erro crítico ao conectar ao Firestore: {e}")
    st.info("Verifique se as credenciais [firebase_service_account] estão corretas nos Secrets.")
    st.stop()
    

# --- 4. Layout em Abas (Reformulado) ---
tab_assign, tab_clients, tab_users, tab_goals, tab_csv, tab_api, tab_logs = st.tabs([
    "🧑‍💼 Atribuir Clientes (Reativo)",
    "📊 Carteiras Atuais (Visão)",
    "👥 Gestão de Usuários",
    "🎯 Gestão de Metas",
    "📄 Carga de Dados (CSV)",
    "☁️ Carga de Dados (API)",
    "📜 Logs de Auditoria"
])


# --- ABA 1: ATRIBUIR CLIENTES (Sua sugestão!) ---
with tab_assign:
    st.header("Atribuir Clientes (Vendas Órfãs)")
    st.info("""
    **Este é o principal módulo de gestão de clientes.**
    Ele lista todas as vendas de produtos que não foram associadas a um consultor,
    pois o CNPJ do cliente não estava em nenhuma carteira no momento da importação.
    
    Ao atribuir uma venda a um consultor aqui, o sistema automaticamente:
    1.  Atualiza esta e todas as futuras vendas desse CNPJ.
    2.  Adiciona o cliente à carteira do consultor.
    """)
    
    with st.spinner("Buscando vendas órfãs..."):
        df_orphans = get_orphan_sales()

    if df_orphans.empty:
        st.success("🎉 Nenhuma venda órfã encontrada no sistema!")
    else:
        st.warning(f"Encontradas **{len(df_orphans)}** vendas órfãs (limitado a 500 por vez).")
        
        # Usando st.data_editor para uma interface de atribuição rápida
        df_orphans["assign_to_uid"] = "" # Adiciona coluna vazia

        edited_df = st.data_editor(
            df_orphans,
            column_config={
                "doc_id": None, # Esconde o ID
                "client_name": st.column_config.TextColumn("Cliente"),
                "client_cnpj": st.column_config.TextColumn("CNPJ"),
                "source": st.column_config.TextColumn("Produto"),
                "date": st.column_config.TextColumn("Data Venda"),
                "revenue_net": st.column_config.NumberColumn("Receita", format="R$ %.2f"),
                "assign_to_uid": st.column_config.SelectboxColumn(
                    "Atribuir ao Consultor",
                    options=consultants_list_dict.keys(),
                    format_func=lambda uid: consultants_list_dict.get(uid, "Selecione...")
                )
            },
            use_container_width=True,
            num_rows="dynamic"
        )
        
        st.divider()

        if st.button("Salvar Atribuições", type="primary"):
            with st.spinner("Processando atribuições..."):
                db = get_db()
                batch = db.batch()
                count = 0
                assigned_count = 0
                
                for index, row in edited_df.iterrows():
                    consultant_uid = row["assign_to_uid"]
                    
                    if consultant_uid and consultant_uid in consultants_map:
                        doc_id = row["doc_id"]
                        consultant_data = consultants_map[consultant_uid]
                        manager_uid = consultant_data["manager_uid"]
                        
                        # 1. Atualiza o documento da VENDA (sales_data)
                        sale_ref = db.collection("sales_data").document(doc_id)
                        batch.update(sale_ref, {
                            "consultant_uid": consultant_uid,
                            "manager_uid": manager_uid
                        })
                        
                        # 2. Atualiza (ou cria) o cadastro do CLIENTE
                        client_cnpj = row["client_cnpj"]
                        client_name = row["client_name"]
                        
                        if client_cnpj and client_cnpj != "N/A":
                            client_ref = db.collection("clients").document(client_cnpj)
                            batch.set(client_ref, {
                                "client_name": client_name,
                                "consultant_uid": consultant_uid,
                                "manager_uid": manager_uid,
                                "updated_at": datetime.now()
                            }, merge=True)
                        
                        count += 2 # Duas operações
                        assigned_count += 1
                        
                        if count >= 490:
                            batch.commit()
                            batch = db.batch()
                            count = 0
                
                if count > 0:
                    batch.commit()
                
                st.success(f"{assigned_count} vendas foram corrigidas e atribuídas!")
                log_audit(action="assign_orphans", details={"count": assigned_count})
                st.cache_data.clear()
                st.rerun()


# --- ABA 2: CARTEIRAS ATUAIS (Visão) ---
with tab_clients:
    st.header("Visão das Carteiras Atuais")
    st.info("Esta é uma visão de todos os clientes que já foram atribuídos a um consultor.")
    
    if df_clients.empty:
        st.warning("Nenhum cliente cadastrado no sistema ainda.")
    else:
        # Mapeia UID para Nome para visualização
        consultant_name_map = {u['uid']: u['name'] for _, u in df_users.iterrows()}
        df_clients['consultant_name'] = df_clients['consultant_uid'].map(consultant_name_map).fillna("N/A")
        
        # Filtro
        search_name = st.text_input("Buscar por nome ou CNPJ")
        
        if search_name:
            df_display = df_clients[
                df_clients['name'].str.contains(search_name, case=False) |
                df_clients['cnpj'].str.contains(search_name, case=False)
            ]
        else:
            df_display = df_clients
        
        st.dataframe(df_display[['cnpj', 'name', 'consultant_name']], use_container_width=True)


# --- ABA 3: GESTÃO DE USUÁRIOS ---
with tab_users:
    st.header("Criar e Gerenciar Usuários")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Criar Novo Usuário")
        
        manager_dict = {u['uid']: u['name'] for _, u in df_users[df_users['role'] == 'manager'].iterrows()}
        
        with st.form("new_user_form", clear_on_submit=True):
            name = st.text_input("Nome Completo")
            email = st.text_input("Email")
            password = st.text_input("Senha Temporária", type="password")
            role = st.selectbox("Nível de Acesso", ["consultant", "manager", "admin"])
            
            manager_uid = None
            if role == "consultant" and manager_dict:
                manager_uid = st.selectbox(
                    "Gestor Responsável",
                    options=list(manager_dict.keys()),
                    format_func=lambda uid: manager_dict[uid]
                )
            elif role == "consultant":
                st.warning("Crie um usuário 'manager' primeiro para poder associar.")
            
            submit_user = st.form_submit_button("Criar Usuário", use_container_width=True)
            
            if submit_user:
                if not name or not email or not password or not role:
                    st.error("Preencha todos os campos!")
                elif role == "consultant" and not manager_uid:
                     st.error("Consultores precisam de um gestor associado.")
                else:
                    try:
                        admin_auth = get_admin_auth()
                        db = get_db()
                        
                        user_record = admin_auth.create_user(
                            email=email,
                            password=password,
                            display_name=name
                        )
                        
                        user_data = {
                            "name": name,
                            "email": email,
                            "role": role,
                            "manager_uid": manager_uid if role == "consultant" else None
                        }
                        db.collection("users").document(user_record.uid).set(user_data)
                        
                        log_audit("create_user", {"new_user_email": email, "role": role})
                        st.success(f"Usuário '{name}' criado com sucesso (UID: {user_record.uid})!")
                        st.cache_data.clear()
                        
                    except Exception as e:
                        st.error(f"Erro ao criar usuário: {e}")

    with col2:
        st.subheader("Usuários Existentes")
        st.dataframe(df_users, use_container_width=True)


# --- ABA 4: GESTÃO DE METAS ---
with tab_goals:
    st.header("🎯 Gestão de Metas Mensais")
    st.info("Defina as metas de Receita Líquida (R$) para os consultores.")
    
    today = datetime.now()
    col1, col2 = st.columns(2)
    selected_month = col1.selectbox(
        "Mês da Meta", 
        options=range(1, 13), 
        format_func=lambda m: calendar.month_name[m],
        index=today.month - 1
    )
    selected_year = col2.number_input("Ano da Meta", value=today.year, min_value=2024, max_value=2030)
    
    month_id = f"{selected_year}-{selected_month:02d}"
    
    st.subheader(f"Definindo Metas para: {calendar.month_name[selected_month]} / {selected_year}")
    
    current_goals = get_goals(month_id)
    
    df_consultants['meta'] = df_consultants['uid'].map(lambda uid: current_goals.get(uid, 0.0))
    
    edited_goals_df = st.data_editor(
        df_consultants[['name', 'email', 'meta', 'uid']],
        column_config={
            "uid": None, # Esconde UID
            "name": st.column_config.TextColumn("Consultor", disabled=True),
            "email": st.column_config.TextColumn("Email", disabled=True),
            "meta": st.column_config.NumberColumn(
                "Meta (R$)",
                min_value=0.0,
                format="R$ %.2f"
            )
        },
        use_container_width=True
    )
    
    if st.button("Salvar Metas", type="primary"):
        goals_to_save = pd.Series(
            edited_goals_df.meta.values, 
            index=edited_goals_df.uid
        ).to_dict()
        
        goals_to_save = {uid: float(meta) for uid, meta in goals_to_save.items() if pd.notna(meta)}
        
        try:
            db = get_db()
            doc_ref = db.collection("goals").document(month_id)
            doc_ref.set(goals_to_save)
            
            log_audit("set_goals", {"month_id": month_id, "goals_count": len(goals_to_save)})
            st.success(f"Metas de {month_id} salvas com sucesso!")
            st.cache_data.clear()
            st.rerun()
            
        except Exception as e:
            st.error(f"Erro ao salvar metas: {e}")


# --- ABA 5: CARGA DE DADOS (CSV) ---
with tab_csv:
    st.header("Upload de Arquivos CSV")
    
    st.subheader("Produto: Bionio")
    uploaded_bionio = st.file_uploader("Selecione o arquivo Bionio.csv", type="csv", key="bionio_uploader")
    if uploaded_bionio:
        if st.button("Processar Bionio"):
            with st.spinner("Processando Bionio..."):
                result = process_bionio_csv(uploaded_bionio)
                if result:
                    total_saved, total_orphans = result
                    st.success(f"Processamento Bionio concluído! {total_saved} registros salvos.")
                    if total_orphans > 0:
                        st.warning(f"**{total_orphans} vendas órfãs** detectadas.")
                        st.info("Acesse a aba 'Atribuir Clientes' para corrigi-las.")
                    
    st.divider()

    st.subheader("Produto: Rovema Pay")
    uploaded_rovema = st.file_uploader("Selecione o arquivo RovemaPay.csv", type="csv", key="rovema_uploader")
    if uploaded_rovema:
        if st.button("Processar Rovema Pay"):
            with st.spinner("Processando Rovema Pay..."):
                result = process_rovema_csv(uploaded_rovema)
                if result:
                    total_saved, total_orphans = result
                    st.success(f"Processamento Rovema Pay concluído! {total_saved} registros salvos.")
                    if total_orphans > 0:
                        st.warning(f"**{total_orphans} vendas órfãs** detectadas.")
                        st.info("Acesse a aba 'Atribuir Clientes' para corrigi-las.")


# --- ABA 6: CARGA DE DADOS (API) ---
with tab_api:
    st.header("Carga de Dados via API")
    st.info("As credenciais das APIs são lidas automaticamente dos Secrets.")
    
    st.subheader("Selecione o Período de Carga")
    col1, col2 = st.columns(2)
    api_start_date = col1.date_input("Data Inicial", datetime.now().replace(day=1))
    api_end_date = col2.date_input("Data Final", datetime.now())
    
    st.divider()

    st.subheader("Produto: ASTO (Logpay)")
    st.markdown(f"Usando usuário: `{st.secrets.get('api_credentials', {}).get('asto_username', 'N/A')}`")
    
    if st.button("Carregar Dados ASTO"):
        with st.spinner("Buscando dados na API ASTO..."):
            result = asyncio.run(process_asto_api(api_start_date, api_end_date))
            if result:
                total_saved, total_orphans = result
                st.success(f"Carga ASTO concluída! {total_saved} registros salvos.")
                if total_orphans > 0:
                    st.warning(f"**{total_orphans} vendas órfãs** detectadas. Acesse 'Atribuir Clientes'.")

    st.divider()
    
    st.subheader("Produto: ELIQ (Uzzipay)")
    st.markdown(f"Usando URL: `{st.secrets.get('api_credentials', {}).get('eliq_url', 'N/A')}`")

    if st.button("Carregar Dados ELIQ"):
        with st.spinner("Buscando dados na API ELIQ..."):
            result = asyncio.run(process_eliq_api(api_start_date, api_end_date))
            if result:
                total_saved, total_orphans = result
                st.success(f"Carga ELIQ concluída! {total_saved} registros salvos.")
                if total_orphans > 0:
                    st.warning(f"**{total_orphans} vendas órfãs** detectadas. Acesse 'Atribuir Clientes'.")


# --- ABA 7: LOGS DE AUDITORIA ---
with tab_logs:
    st.header("📜 Logs de Auditoria do Sistema")
    st.info("Exibe as ações mais recentes realizadas no sistema.")
    
    with st.spinner("Carregando logs..."):
        df_logs = get_audit_logs(limit=200)

    if df_logs.empty:
        st.info("Nenhum log de auditoria encontrado.")
    else:
        # Reordena colunas para melhor visualização
        df_logs = df_logs[["timestamp", "user_email", "action", "details"]]

        st.dataframe(
            df_logs,
            use_container_width=True,
            column_config={
                "timestamp": st.column_config.DatetimeColumn(
                    "Data/Hora",
                    format="YYYY-MM-DD HH:mm:ss"
                ),
                "user_email": st.column_config.TextColumn("Usuário"),
                "action": st.column_config.TextColumn("Ação"),
                "details": st.column_config.TextColumn("Detalhes")
            }
        )

    if st.button("Recarregar Logs"):
        st.cache_data.clear()
        st.rerun()
