# functions/main.py
# (Você precisará de um requirements.txt para suas functions também)

import functions_framework
import requests
import pandas as pd
import base64
from requests.auth import HTTPBasicAuth
from google.cloud import firestore

# --- Configuração ---
db = firestore.Client()
ASTO_API_URL = "https://services.host.logpay.com.br/api"
ELIQ_API_URL = "https://sigyo.uzzipay.com/api"

# --- FUNÇÃO 1: ETL (Extração) para ASTO ---
# Agende esta função para rodar 1x por dia (ex: 2h da manhã)
@functions_framework.http
def etl_asto(request):
    """
    Busca dados da API ASTO e salva no Firestore RAW.
    """
    # Use Segredos (Secret Manager) para suas credenciais
    ASTO_USER = "SEU_USUARIO_API"
    ASTO_PASS = "SUA_SENHA_API"
    
    auth = HTTPBasicAuth(ASTO_USER, ASTO_PASS)
    
    try:
        # 1. Buscar Clientes
        # Nota: A API do ASTO (swagger) não mostra um endpoint "GET /api/Cliente" sem ID.
        # Estou assumindo que existe um ou que você tem uma lista de IDs.
        # Para este exemplo, vou focar no que posso buscar: Estabelecimentos.
        batch = db.batch()
        
        estab_response = requests.get(f"{ASTO_API_URL}/Estabelecimento", auth=auth, params={"api-version": "1"})
        estab_response.raise_for_status()
        
        for estab in estab_response.json():
            doc_ref = db.collection("raw_asto_estabelecimentos").document(str(estab['id']))
            batch.set(doc_ref, estab)
        
        batch.commit()
        print(f"ASTO: {len(estab_response.json())} estabelecimentos carregados.")

        # TODO: Adicionar a busca de transações aqui
        # A API /api/Ticket/{...} requer IDs de cliente e data.
        # Você precisará de uma lógica para iterar sobre seus clientes
        # e buscar os tickets do dia anterior.

        return "OK", 200

    except Exception as e:
        print(f"Erro no ETL ASTO: {e}")
        return "Erro", 500

# --- FUNÇÃO 2: ETL (Extração) para ELIQ ---
# Agende esta função para rodar 1x por dia (ex: 2h30 da manhã)
@functions_framework.http
def etl_eliq(request):
    """
    Busca dados da API ELIQ e salva no Firestore RAW.
    """
    # Use Segredos (Secret Manager) para seu Token
    ELIQ_TOKEN = "SEU_BEARER_TOKEN"
    headers = {"Authorization": f"Bearer {ELIQ_TOKEN}"}
    
    endpoints = {
        "raw_eliq_clientes": "/clientes?expand=municipio,modulos,organizacao",
        "raw_eliq_credenciados": "/credenciados?expand=municipio,modulos",
        "raw_eliq_contratos": "/contratos?expand=empresa,situacao",
        "raw_eliq_empenhos": "/empenhos?expand=contrato.empresa",
        "raw_eliq_faturas_recebimento": "/fatura-recebimentos?expand=cliente,status",
        "raw_eliq_faturas_pagamento": "/fatura-pagamentos?expand=credenciado,status",
        "raw_eliq_produtos": "/produtos?expand=categoria"
        # Adicione transações com filtro de data
    }
    
    try:
        batch = db.batch()
        for collection_name, endpoint in endpoints.items():
            response = requests.get(f"{ELIQ_API_URL}{endpoint}", headers=headers)
            response.raise_for_status()
            
            data = response.json()
            # A API ELIQ retorna uma lista (baseado nos seus JSONs)
            for item in data:
                doc_ref = db.collection(collection_name).document(str(item['id']))
                batch.set(doc_ref, item)
        
        batch.commit()
        print(f"ELIQ: {len(endpoints)} endpoints carregados.")
        return "OK", 200
        
    except Exception as e:
        print(f"Erro no ETL ELIQ: {e}")
        return "Erro", 500

# --- FUNÇÃO 3: AGREGADOR (A mais importante) ---
# Agende para rodar 1x por dia (ex: 4h da manhã) OU
# Faça seu app Streamlit chamá-la após o upload de CSVs
@functions_framework.http
def aggregate_kpis(request):
    """
    Lê TODOS os dados RAW do Firestore e pré-calcula os KPIs
    para o dashboard.
    """
    try:
        kpis = {}

        # --- 1. Processar RovemaPay (do raw_rovemapay) ---
        rovemapay_docs = db.collection("raw_rovemapay").stream()
        df_rovema = pd.DataFrame([doc.to_dict() for doc in rovemapay_docs])
        
        if not df_rovema.empty:
            # Limpeza de dados (CSV vem com strings)
            df_rovema['Bruto'] = pd.to_numeric(df_rovema['Bruto'].str.replace('"', '').str.replace(',', '.').str.strip())
            df_rovema['Mdr'] = pd.to_numeric(df_rovema['Mdr'].str.replace('"', '').str.replace(',', '.').str.strip())
            
            kpis['rovemapay_gmv'] = df_rovema['Bruto'].sum()
            kpis['rovemapay_receita'] = df_rovema['Mdr'].sum()
            kpis['rovemapay_ticket_medio'] = df_rovema.groupby('ID Venda')['Bruto'].sum().mean()
            kpis['rovemapay_clientes_ativos'] = df_rovema['CNPJ'].nunique()

        # --- 2. Processar Bionio (do raw_bionio) ---
        bionio_docs = db.collection("raw_bionio").stream()
        df_bionio = pd.DataFrame([doc.to_dict() for doc in bionio_docs])
        
        if not df_bionio.empty:
            df_bionio['Valor total do pedido'] = pd.to_numeric(df_bionio['Valor total do pedido'].str.replace('"', '').str.replace('.', '').str.replace(',', '.').str.strip())
            
            df_bionio_pago = df_bionio[df_bionio['Status do pedido'].isin(['Pago', 'Transferido', 'Pago e Agendado'])]
            
            kpis['bionio_gmv'] = df_bionio_pago['Valor total do pedido'].sum()
            kpis['bionio_pedidos_total'] = df_bionio['Número do pedido'].nunique()
            kpis['bionio_orgs_ativas'] = df_bionio['CNPJ da organização'].nunique()

        # --- 3. Processar ELIQ (das coleções raw_eliq_*) ---
        contratos_docs = db.collection("raw_eliq_contratos").stream()
        df_contratos = pd.DataFrame([doc.to_dict() for doc in contratos_docs])
        empenhos_docs = db.collection("raw_eliq_empenhos").stream()
        df_empenhos = pd.DataFrame([doc.to_dict() for doc in empenhos_docs])

        if not df_contratos.empty:
            df_contratos['valor'] = pd.to_numeric(df_contratos['valor'])
            kpis['eliq_valor_total_contratado'] = df_contratos[df_contratos['situacao']['nome'] == 'Ativo']['valor'].sum()

        if not df_empenhos.empty:
            df_empenhos['saldo'] = pd.to_numeric(df_empenhos['saldo'])
            kpis['eliq_valor_empenhado_saldo'] = df_empenhos[df_empenhos['situacao'] == 'aprovado']['saldo'].sum()

        # --- 4. Crossover (A Mágica) ---
        # (Requer que os CNPJs estejam limpos e padronizados)
        if not df_rovema.empty and not df_bionio.empty:
            cnpjs_rovema = set(df_rovema['CNPJ'])
            cnpjs_bionio_orgs = set(df_bionio['CNPJ da organização'])
            
            # Clientes que são ECs (Rovema) E Organizações (Bionio)
            kpis['crossover_rovema_bionio'] = len(cnpjs_rovema.intersection(cnpjs_bionio_orgs))

        # --- 5. Salvar KPIs Agregados ---
        # Salva tudo em um único documento para leitura rápida no Streamlit
        doc_ref = db.collection("dashboard_agregado").document("kpis_consolidados")
        doc_ref.set(kpis)
        
        print("KPIs agregados com sucesso!")
        return "OK", 200

    except Exception as e:
        print(f"Erro ao agregar KPIs: {e}")
        return "Erro", 500
