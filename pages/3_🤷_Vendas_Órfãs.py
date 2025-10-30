import streamlit as st
import pandas as pd
import sys
import os
from datetime import datetime

# CORREÇÃO PARA 'KeyError: utils': Adiciona o diretório raiz ao path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from utils.auth import auth_guard, check_role
from utils.firebase_config import get_db
from utils.logger import log_audit

# --- 1. Proteção da Página ---
auth_guard()
check_role(["admin"])  # Apenas Admins
st.title("🤷 Vendas Órfãs")
st.warning("""
Esta página lista todas as vendas no banco de dados que **não estão associadas a nenhum consultor**.
Isso geralmente acontece quando uma venda é importada (via API ou CSV) e o CNPJ do cliente 
ainda não estava cadastrado na página de Administração.
""")

# --- 2. Funções de Busca ---

@st.cache_data(ttl=600)
def get_orphan_sales():
    """Busca vendas onde consultant_uid é Nulo."""
    db = get_db()
    sales_ref = db.collection("sales_data")
    
    query = sales_ref.where("consultant_uid", "==", None).limit(500) # Limita para performance
    
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

@st.cache_data(ttl=600)
def get_all_consultants():
    """Busca todos os usuários consultores."""
    db = get_db()
    users_ref = db.collection("users").where("role", "==", "consultant").stream()
    
    consultants = {}
    for user in users_ref:
        data = user.to_dict()
        consultants[user.id] = {
            "name": data.get("name", "N/A"),
            "manager_uid": data.get("manager_uid")
        }
    return consultants

# --- 3. Carregamento de Dados ---
with st.spinner("Buscando vendas órfãs..."):
    df_orphans = get_orphan_sales()
    consultants_map = get_all_consultants()

if df_orphans.empty:
    st.success("🎉 Nenhuma venda órfã encontrada no sistema!")
    st.stop()

st.info(f"Encontradas **{len(df_orphans)}** vendas órfãs (limitado a 500 por vez).")

# --- 4. Interface de Correção ---

# Dicionário de consultores para o selectbox
consultants_list = {uid: data["name"] for uid, data in consultants_map.items()}

# Usando st.data_editor para uma interface de atribuição rápida
df_orphans["assign_to_uid"] = "" # Adiciona coluna vazia

edited_df = st.data_editor(
    df_orphans,
    column_config={
        "doc_id": None, # Esconde o ID do documento
        "client_name": st.column_config.TextColumn("Cliente"),
        "client_cnpj": st.column_config.TextColumn("CNPJ"),
        "source": st.column_config.TextColumn("Produto"),
        "date": st.column_config.TextColumn("Data Venda"),
        "revenue_net": st.column_config.NumberColumn("Receita", format="R$ %.2f"),
        "assign_to_uid": st.column_config.SelectboxColumn(
            "Atribuir ao Consultor",
            options=consultants_list.keys(),
            format_func=lambda uid: consultants_list.get(uid, "Selecione...")
        )
    },
    use_container_width=True,
    num_rows="dynamic" # Permite adicionar/remover, embora não usemos
)

st.divider()

if st.button("Salvar Atribuições", type="primary"):
    with st.spinner("Processando atribuições..."):
        db = get_db()
        batch = db.batch()
        count = 0
        
        for index, row in edited_df.iterrows():
            consultant_uid = row["assign_to_uid"]
            
            # Se um consultor foi selecionado na linha
            if consultant_uid and consultant_uid in consultants_map:
                doc_id = row["doc_id"] # Pega o ID original
                
                # Pega os dados do consultor selecionado
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
                    }, merge=True) # merge=True para não sobrescrever outros dados
                
                count += 1
                
                # Limite de batch
                if count >= 490: # (2 operações por linha)
                    batch.commit()
                    batch = db.batch()
                    count = 0
        
        # Commit final
        if count > 0:
            batch.commit()
        
        st.success(f"{int(count/2)} vendas foram corrigidas e atribuídas!")
        
        # Log
        log_audit(action="assign_orphans", details={"count": int(count/2)})
        
        # Limpa o cache e recarrega
        st.cache_data.clear()
        st.rerun()
