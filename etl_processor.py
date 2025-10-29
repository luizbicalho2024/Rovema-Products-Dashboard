import streamlit as st
import pandas as pd
import requests
from requests.auth import HTTPBasicAuth
from fire_admin import get_db
import re # Para limpar CNPJs

# --- Configuração das APIs (lendo dos secrets) ---
ASTO_API_URL = "https://services.host.logpay.com.br/api"
ASTO_USER = st.secrets.get("asto", {}).get("user")
ASTO_PASS = st.secrets.get("asto", {}).get("pass")

ELIQ_API_URL = "https://sigyo.uzzipay.com/api"
ELIQ_TOKEN = st.secrets.get("eliq", {}).get("token")

# --- MÓDULOS DE EXTRAÇÃO (ETL) ---

def clean_cnpj(cnpj):
    """Limpa um CNPJ deixando apenas dígitos."""
    if not isinstance(cnpj, str):
        return None
    return re.sub(r'\D', '', cnpj)

def fetch_asto_data(db):
    """Busca dados da API ASTO e salva no Firestore RAW."""
    st.write("Iniciando ETL: ASTO...")
    if not ASTO_USER or not ASTO_PASS:
        st.error("Credenciais da API ASTO não configuradas nos Segredos.")
        return False
        
    auth = HTTPBasicAuth(ASTO_USER, ASTO_PASS)
    
    # Adicione mais endpoints conforme sua necessidade
    endpoints_asto = {
        "raw_asto_estabelecimentos": "/Estabelecimento"
        # "raw_asto_clientes": "/Cliente" # Adicione se existir
    }
    
    try:
        total_items = 0
        for collection, endpoint in endpoints_asto.items():
            response = requests.get(f"{ASTO_API_URL}{endpoint}", auth=auth, params={"api-version": "1"})
            response.raise_for_status()
            data = response.json()
            
            batch = db.batch()
            count = 0
            for item in data:
                doc_id = str(item.get('id', item.get('Id', f"item_{count}")))
                batch.set(db.collection(collection).document(doc_id), item)
                count += 1
            batch.commit()
            total_items += count
            
        st.success(f"ASTO: {total_items} itens carregados.")
        return True
    except Exception as e:
        st.error(f"Erro no ETL ASTO: {e}")
        return False

def fetch_eliq_data(db):
    """Busca dados da API ELIQ e salva no Firestore RAW."""
    st.write("Iniciando ETL: ELIQ...")
    if not ELIQ_TOKEN:
        st.error("Token da API ELIQ não configurado nos Segredos.")
        return False
        
    headers = {"Authorization": f"Bearer {ELIQ_TOKEN}"}
    
    endpoints_eliq = {
        "raw_eliq_clientes": "/clientes?expand=municipio,modulos,organizacao",
        "raw_eliq_credenciados": "/credenciados?expand=municipio,modulos",
        "raw_eliq_contratos": "/contratos?expand=empresa,situacao",
        "raw_eliq_empenhos": "/empenhos?expand=contrato.empresa",
        "raw_eliq_produtos": "/produtos?expand=categoria",
        "raw_eliq_faturas_recebimento": "/fatura-recebimentos?expand=cliente,status",
        "raw_eliq_faturas_pagamento": "/fatura-pagamentos?expand=credenciado,status"
    }
    
    try:
        total_items = 0
        for collection, endpoint in endpoints_eliq.items():
            response = requests.get(f"{ELIQ_API_URL}{endpoint}", headers=headers)
            response.raise_for_status()
            data = response.json()
            
            if not isinstance(data, list):
                st.warning(f"Endpoint ELIQ {endpoint} não retornou uma lista.")
                continue

            batch = db.batch()
            count = 0
            for item in data:
                doc_id = str(item.get('id', f"item_{count}"))
                batch.set(db.collection(collection).document(doc_id), item)
                count += 1
            batch.commit()
            total_items += count
            
        st.success(f"ELIQ: {total_items} itens de {len(endpoints_eliq)} endpoints carregados.")
        return True
    except Exception as e:
        st.error(f"Erro no ETL ELIQ: {e}")
        return False

def read_raw_data_from_firestore(db):
    """Lê TODOS os dados brutos (API e CSV) do Firestore para processar."""
    st.write("Lendo dados brutos do Firestore para agregação...")
    dataframes = {}
    
    # Coleções dos CSVs
    raw_collections_csv = ["raw_rovemapay", "raw_bionio"]
    # Coleções das APIs
    raw_collections_api = [
        "raw_eliq_clientes", "raw_eliq_credenciados", 
        "raw_eliq_contratos", "raw_eliq_empenhos", 
        "raw_asto_estabelecimentos"
    ]
    
    try:
        for collection in raw_collections_csv + raw_collections_api:
            docs = db.collection(collection).stream()
            df = pd.DataFrame([doc.to_dict() for doc in docs])
            if not df.empty:
                dataframes[collection] = df
                st.write(f"-> {len(df)} documentos lidos de `{collection}`")
            else:
                st.write(f"-> 0 documentos em `{collection}`")
        
        return dataframes
    except Exception as e:
        st.error(f"Erro ao ler dados brutos do Firestore: {e}")
        return {}

# --- MÓDULO DE AGREGAÇÃO (O "CÉREBRO") ---

def aggregate_and_save_kpis(db, dataframes):
    """
    Executa todos os cálculos pesados e salva os resultados
    na coleção 'dashboard_agregado'.
    """
    st.write("Iniciando agregação de KPIs...")
    kpis = {} # Documento principal de KPIs
    
    # Referências aos DataFrames
    df_rovema = dataframes.get("raw_rovemapay", pd.DataFrame())
    df_bionio = dataframes.get("raw_bionio", pd.DataFrame())
    df_eliq_clientes = dataframes.get("raw_eliq_clientes", pd.DataFrame())
    df_eliq_contratos = dataframes.get("raw_eliq_contratos", pd.DataFrame())
    df_eliq_empenhos = dataframes.get("raw_eliq_empenhos", pd.DataFrame())
    df_asto_estab = dataframes.get("raw_asto_estabelecimentos", pd.DataFrame())
    
    try:
        # --- 1. Processar RovemaPay ---
        if not df_rovema.empty:
            df_rovema['Bruto'] = pd.to_numeric(df_rovema['Bruto'].str.replace('"', '').str.replace(',', '.', regex=False).str.strip(), errors='coerce').fillna(0)
            df_rovema['Mdr'] = pd.to_numeric(df_rovema['Mdr'].str.replace('"', '').str.replace(',', '.', regex=False).str.strip(), errors='coerce').fillna(0)
            
            kpis['rovemapay_gmv'] = df_rovema['Bruto'].sum()
            kpis['rovemapay_receita'] = df_rovema['Mdr'].sum()
            kpis['rovemapay_ticket_medio'] = df_rovema.groupby('ID Venda')['Bruto'].sum().mean()
            kpis['rovemapay_clientes_ativos'] = df_rovema['CNPJ'].nunique()
            df_rovema['cnpj_limpo'] = df_rovema['CNPJ'].apply(clean_cnpj)

        # --- 2. Processar Bionio ---
        if not df_bionio.empty:
            df_bionio['Valor total do pedido'] = pd.to_numeric(df_bionio['Valor total do pedido'].str.replace('"', '').str.replace('.', '', regex=False).str.replace(',', '.', regex=False).str.strip(), errors='coerce').fillna(0)
            
            status_pago = ['Pago', 'Transferido', 'Pago e Agendado']
            df_bionio_pago = df_bionio[df_bionio['Status do pedido'].isin(status_pago)]
            
            kpis['bionio_gmv'] = df_bionio_pago['Valor total do pedido'].sum()
            kpis['bionio_pedidos_total'] = df_bionio['Número do pedido'].nunique()
            kpis['bionio_orgs_ativas'] = df_bionio['CNPJ da organização'].nunique()
            df_bionio['cnpj_limpo'] = df_bionio['CNPJ da organização'].apply(clean_cnpj)

        # --- 3. Processar ELIQ ---
        if not df_eliq_contratos.empty:
            df_eliq_contratos['valor'] = pd.to_numeric(df_eliq_contratos['valor'], errors='coerce').fillna(0)
            # Extrai o status do dict aninhado
            df_eliq_contratos['status_nome'] = df_eliq_contratos['situacao'].apply(lambda x: x['nome'] if isinstance(x, dict) else 'Desconhecido')
            kpis['eliq_valor_total_contratado'] = df_eliq_contratos[df_eliq_contratos['status_nome'] == 'Ativo']['valor'].sum()

        if not df_eliq_empenhos.empty:
            df_eliq_empenhos['saldo'] = pd.to_numeric(df_eliq_empenhos['saldo'], errors='coerce').fillna(0)
            kpis['eliq_valor_empenhado_saldo'] = df_eliq_empenhos[df_eliq_empenhos['situacao'] == 'aprovado']['saldo'].sum()

        if not df_eliq_clientes.empty:
            kpis['eliq_clientes_total'] = df_eliq_clientes['id'].nunique()

        # --- 4. Processar ASTO ---
        if not df_asto_estab.empty:
            kpis['asto_estabelecimentos_total'] = df_asto_estab['id'].nunique()

        # --- 5. Crossover (A Mágica) ---
        st.write("Calculando Crossovers...")
        cnpjs_rovema = set(df_rovema['cnpj_limpo'].dropna())
        cnpjs_bionio = set(df_bionio['cnpj_limpo'].dropna())
        
        # Limpa CNPJs do ELIQ
        if not df_eliq_clientes.empty:
            df_eliq_clientes['cnpj_limpo'] = df_eliq_clientes['cnpj'].apply(clean_cnpj)
            cnpjs_eliq_clientes = set(df_eliq_clientes['cnpj_limpo'].dropna())
        else:
            cnpjs_eliq_clientes = set()
            
        kpis['crossover_rovema_bionio'] = len(cnpjs_rovema.intersection(cnpjs_bionio))
        kpis['crossover_eliq_bionio'] = len(cnpjs_eliq_clientes.intersection(cnpjs_bionio))
        kpis['crossover_eliq_rovema'] = len(cnpjs_eliq_clientes.intersection(cnpjs_rovema))
        
        # Tabela de Oportunidade: Clientes Rovema que NÃO são Bionio
        oportunidades = cnpjs_rovema - cnpjs_bionio
        df_ops = df_rovema[df_rovema['cnpj_limpo'].isin(oportunidades)][['EC', 'CNPJ']].drop_duplicates().to_dict('records')
        
        # Salvar tabela de crossover (write separado)
        doc_ops_ref = db.collection("dashboard_agregado").document("tabela_oportunidades")
        doc_ops_ref.set({"oportunidade_rovema_nao_bionio": df_ops})
        st.write(f"Encontradas {len(df_ops)} oportunidades de crossover (Rovema -> Bionio).")

        # --- 6. Salvar KPIs Agregados ---
        doc_ref = db.collection("dashboard_agregado").document("kpis_consolidados")
        doc_ref.set(kpis)
        
        st.success("SUCESSO: KPIs consolidados foram calculados e salvos!")
        return True
    
    except Exception as e:
        st.error(f"Erro fatal na agregação de KPIs: {e}")
        st.exception(e)
        return False
