import streamlit as st
import pandas as pd
import requests
from requests.auth import HTTPBasicAuth
from fire_admin import get_db

# --- CONFIGURAÇÃO DAS APIS (USE SECRETS!) ---
ASTO_API_URL = "https://services.host.logpay.com.br/api"
ASTO_USER = st.secrets.get("asto", {}).get("user", "SEU_USER_ASTO")
ASTO_PASS = st.secrets.get("asto", {}).get("pass", "SUA_SENHA_ASTO")

ELIQ_API_URL = "https://sigyo.uzzipay.com/api"
ELIQ_TOKEN = st.secrets.get("eliq", {}).get("token", "SEU_TOKEN_ELIQ")

# --- MÓDULOS DE EXTRAÇÃO (ETL) ---

def fetch_asto_data(db):
    """Busca dados da API ASTO e salva no Firestore RAW."""
    st.write("Buscando dados ASTO...")
    auth = HTTPBasicAuth(ASTO_USER, ASTO_PASS)
    
    # Exemplo: Buscando Estabelecimentos.
    # Você precisará expandir isso para os endpoints de transações.
    try:
        response = requests.get(f"{ASTO_API_URL}/Estabelecimento", auth=auth, params={"api-version": "1"})
        response.raise_for_status()
        estabelecimentos = response.json()
        
        batch = db.batch()
        for item in estabelecimentos:
            doc_ref = db.collection("raw_asto_estabelecimentos").document(str(item['id']))
            batch.set(doc_ref, item)
        batch.commit()
        
        st.success(f"ASTO: {len(estabelecimentos)} estabelecimentos carregados.")
        return True
    except Exception as e:
        st.error(f"Erro no ETL ASTO: {e}")
        return False

def fetch_eliq_data(db):
    """Busca dados da API ELIQ e salva no Firestore RAW."""
    st.write("Buscando dados ELIQ...")
    headers = {"Authorization": f"Bearer {ELIQ_TOKEN}"}
    
    # Endpoints que analisamos
    endpoints = {
        "raw_eliq_clientes": "/clientes?expand=municipio,modulos,organizacao",
        "raw_eliq_credenciados": "/credenciados?expand=municipio,modulos",
        "raw_eliq_contratos": "/contratos?expand=empresa,situacao",
        "raw_eliq_empenhos": "/empenhos?expand=contrato.empresa",
        "raw_eliq_produtos": "/produtos?expand=categoria"
        # Adicione aqui os endpoints de transação com filtro de data
    }
    
    try:
        total_items = 0
        for collection_name, endpoint in endpoints.items():
            response = requests.get(f"{ELIQ_API_URL}{endpoint}", headers=headers)
            response.raise_for_status()
            
            data = response.json()
            if not isinstance(data, list):
                st.warning(f"Endpoint {endpoint} não retornou uma lista.")
                continue

            batch = db.batch()
            for item in data:
                if 'id' not in item:
                    continue
                doc_ref = db.collection(collection_name).document(str(item['id']))
                batch.set(doc_ref, item)
            batch.commit()
            total_items += len(data)
        
        st.success(f"ELIQ: {total_items} itens de {len(endpoints)} endpoints carregados.")
        return True
    except Exception as e:
        st.error(f"Erro no ETL ELIQ: {e}")
        return False

def read_raw_csv_data(db):
    """Lê os dados brutos dos CSVs que já estão no Firestore."""
    st.write("Lendo dados brutos dos CSVs do Firestore...")
    try:
        # 1. RovemaPay
        rovemapay_docs = db.collection("raw_rovemapay").stream()
        df_rovema = pd.DataFrame([doc.to_dict() for doc in rovemapay_docs])
        st.write(f"Lidas {len(df_rovema)} linhas de RovemaPay.")

        # 2. Bionio
        bionio_docs = db.collection("raw_bionio").stream()
        df_bionio = pd.DataFrame([doc.to_dict() for doc in bionio_docs])
        st.write(f"Lidas {len(df_bionio)} linhas de Bionio.")
        
        return {"rovema": df_rovema, "bionio": df_bionio}
    except Exception as e:
        st.error(f"Erro ao ler dados brutos do Firestore: {e}")
        return {"rovema": pd.DataFrame(), "bionio": pd.DataFrame()}

# --- MÓDULO DE AGREGAÇÃO (O "CÉREBRO") ---

def aggregate_and_save_kpis(db, dataframes):
    """
    Executa todos os cálculos pesados e salva os resultados
    na coleção 'dashboard_agregado'.
    """
    st.write("Iniciando agregação de KPIs...")
    kpis = {}
    
    df_rovema = dataframes.get("rovema", pd.DataFrame())
    df_bionio = dataframes.get("bionio", pd.DataFrame())
    
    try:
        # --- 1. Processar RovemaPay ---
        if not df_rovema.empty:
            df_rovema['Bruto'] = pd.to_numeric(df_rovema['Bruto'].str.replace('"', '').str.replace(',', '.').str.strip(), errors='coerce')
            df_rovema['Mdr'] = pd.to_numeric(df_rovema['Mdr'].str.replace('"', '').str.replace(',', '.').str.strip(), errors='coerce')
            
            kpis['rovemapay_gmv'] = df_rovema['Bruto'].sum()
            kpis['rovemapay_receita'] = df_rovema['Mdr'].sum()
            kpis['rovemapay_ticket_medio'] = df_rovema.groupby('ID Venda')['Bruto'].sum().mean()
            kpis['rovemapay_clientes_ativos'] = df_rovema['CNPJ'].nunique()
            
            # Limpa CNPJ para crossover
            df_rovema['cnpj_limpo'] = df_rovema['CNPJ'].astype(str).str.replace(r'[\D]', '', regex=True)

        # --- 2. Processar Bionio ---
        if not df_bionio.empty:
            df_bionio['Valor total do pedido'] = pd.to_numeric(df_bionio['Valor total do pedido'].str.replace('"', '').str.replace('.', '', regex=False).str.replace(',', '.').str.strip(), errors='coerce')
            
            status_pago = ['Pago', 'Transferido', 'Pago e Agendado']
            df_bionio_pago = df_bionio[df_bionio['Status do pedido'].isin(status_pago)]
            
            kpis['bionio_gmv'] = df_bionio_pago['Valor total do pedido'].sum()
            kpis['bionio_pedidos_total'] = df_bionio['Número do pedido'].nunique()
            kpis['bionio_orgs_ativas'] = df_bionio['CNPJ da organização'].nunique()

            # Limpa CNPJ para crossover
            df_bionio['cnpj_limpo'] = df_bionio['CNPJ da organização'].astype(str).str.replace(r'[\D]', '', regex=True)

        # --- 3. Processar ELIQ (Exemplo) ---
        # (Idealmente, você leria as coleções raw_eliq_* aqui também)
        # Para este exemplo, vamos assumir que os dados da API já foram agregados
        # em uma etapa anterior (não mostrado no ETL para simplificar)
        
        # --- 4. Crossover (O mais importante) ---
        if not df_rovema.empty and not df_bionio.empty:
            cnpjs_rovema = set(df_rovema['cnpj_limpo'].dropna())
            cnpjs_bionio_orgs = set(df_bionio['cnpj_limpo'].dropna())
            
            kpis['crossover_rovema_bionio'] = len(cnpjs_rovema.intersection(cnpjs_bionio_orgs))
            
            # Tabela de Oportunidade: Clientes Rovema que NÃO são Bionio
            oportunidades = cnpjs_rovema - cnpjs_bionio_orgs
            df_ops = df_rovema[df_rovema['cnpj_limpo'].isin(oportunidades)][['EC', 'CNPJ']].drop_duplicates().to_dict('records')
            
            # Salvar tabela de crossover (exige um write separado)
            doc_ops_ref = db.collection("dashboard_agregado").document("tabela_oportunidades")
            doc_ops_ref.set({"oportunidade_rovema_nao_bionio": df_ops})
            st.write(f"Encontradas {len(df_ops)} oportunidades de crossover (Rovema -> Bionio).")

        # --- 5. Salvar KPIs Agregados ---
        doc_ref = db.collection("dashboard_agregado").document("kpis_consolidados")
        doc_ref.set(kpis)
        
        st.success("SUCESSO: KPIs consolidados foram calculados e salvos!")
        return True
    
    except Exception as e:
        st.error(f"Erro fatal na agregação de KPIs: {e}")
        return False
