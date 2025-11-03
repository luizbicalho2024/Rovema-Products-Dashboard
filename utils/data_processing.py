import streamlit as st
import pandas as pd
from utils.firebase_config import get_db
from utils.logger import log_audit
import httpx
from datetime import datetime
import urllib.parse
import time
import json

# --- FUNÇÕES DE LIMPEZA (ETL) ---

def clean_value(value_str):
    """Limpa valores monetários em string (formato BR)."""
    if pd.isna(value_str):
        return 0.0
    if isinstance(value_str, (int, float)):
        return float(value_str)
    
    value_str = str(value_str).strip()
    value_str = value_str.replace("R$", "").replace("%", "")
    value_str = value_str.replace(".", "").replace(",", ".")
    try:
        return float(value_str)
    except ValueError:
        return 0.0

def clean_cnpj(cnpj_str):
    """Limpa e padroniza CNPJs."""
    if pd.isna(cnpj_str):
        return None
    
    cnpj_str = str(cnpj_str)
    
    if 'E' in cnpj_str.upper():
        try:
            cnpj_str = "{:.0f}".format(float(cnpj_str.replace(',', '.')))
        except:
            pass 

    cleaned_cnpj = "".join(filter(str.isdigit, cnpj_str))
    return cleaned_cnpj.zfill(14)


# --- MAPPER DE CARTEIRA (O CORAÇÃO DO SISTEMA) ---

@st.cache_data(ttl=600)
def get_client_portfolio_map():
    """
    Busca no Firestore e cria um dicionário (mapa) de:
    { "cnpj_limpo": {"consultant_uid": "...", "manager_uid": "..."} }
    """
    db = get_db()
    clients_ref = db.collection("clients").stream()
    client_map = {}
    for client in clients_ref:
        data = client.to_dict()
        cnpj = client.id
        client_map[cnpj] = {
            "consultant_uid": data.get("consultant_uid"),
            "manager_uid": data.get("manager_uid")
        }
    return client_map

def map_sale_to_consultant(cnpj):
    """Mapeia uma venda (via CNPJ) ao consultor/gestor."""
    client_map = get_client_portfolio_map()
    cnpj_limpo = clean_cnpj(cnpj)
    
    if cnpj_limpo in client_map:
        return client_map[cnpj_limpo]["consultant_uid"], client_map[cnpj_limpo]["manager_uid"]
    else:
        return None, None

# --- FUNÇÕES DE CARGA (POR PRODUTO) ---

def batch_write_to_firestore(records):
    """
    Escreve os registros em lotes no Firestore.
    Esta função agora é chamada pela PÁGINA ADMIN, não pelas funções de fetch.
    """
    if not records:
        st.warning("Nenhum registro para salvar.")
        return 0, 0
        
    db = get_db()
    batch = db.batch()
    count = 0
    total_written = 0
    total_orphans = 0
    
    total_records = len(records)
    progress_bar = st.progress(0, text=f"Salvando {total_records} registros no banco... (Isso pode levar vários minutos)")
    
    # records é um dict, iteramos sobre os items
    for i, (doc_id, data) in enumerate(records.items()):
        doc_ref = db.collection("sales_data").document(doc_id)
        batch.set(doc_ref, data)
        count += 1
        
        if data.get("consultant_uid") is None:
            total_orphans += 1
        
        if count == 499:
            batch.commit()
            total_written += count
            time.sleep(1) # Pausa para evitar Rate Limit
            batch = db.batch()
            count = 0
            
            progress_percentage = (i + 1) / total_records
            progress_bar.progress(progress_percentage, text=f"Salvando dados... ({total_written} / {total_records} registros)")
            
    if count > 0:
        batch.commit()
        total_written += count
        
    progress_bar.progress(1.0, text=f"Concluído! {total_written} registros salvos.")
    
    # Invalida o cache
    st.cache_data.clear()

    return total_written, total_orphans

# --- Funções de CSV (Elas não usam o fluxo de 2 botões por enquanto) ---

def process_bionio_csv(uploaded_file):
    """Processa o CSV Bionio."""
    try:
        df = pd.read_csv(
            uploaded_file, 
            sep=';', 
            dtype={'CNPJ da organização': str}, 
            encoding='latin-1'
        )
    except Exception as e:
        st.error(f"Erro ao ler o CSV: {e}")
        return
        
    st.write(f"Arquivo Bionio lido: {len(df)} linhas encontradas.")
    df_paid = df[df['Status do pedido'].isin(['Transferido', 'Pago e Agendado'])].copy()
    st.write(f"{len(df_paid)} registros de vendas válidas ('Transferido' ou 'Pago e Agendado').")
    
    if df_paid.empty:
        st.warning("Nenhum registro de venda válida encontrado no arquivo.")
        return 0, 0

    records_to_write = {}
    
    for _, row in df_paid.iterrows():
        cnpj = clean_cnpj(row['CNPJ da organização'])
        data_pagamento_str = row['Data do pagamento do pedido']
        
        try:
            data_pagamento = datetime.strptime(data_pagamento_str, "%d/%m/%Y")
        except:
            continue

        revenue = clean_value(row['Valor total do pedido'])
        consultant_uid, manager_uid = map_sale_to_consultant(cnpj)
        doc_id = f"BIONIO_{row['Número do pedido']}_{data_pagamento_str.replace('/', '-')}"
        
        unified_record = {
            "source": "Bionio",
            "client_cnpj": cnpj,
            "client_name": row['Nome fantasia'],
            "consultant_uid": consultant_uid,
            "manager_uid": manager_uid,
            "date": data_pagamento,
            "revenue_gross": revenue,
            "revenue_net": revenue,
            "product_name": row['Nome do benefício'],
            "status": row['Status do pedido'],
            "payment_type": row['Tipo de pagamento'],
            "raw_id": str(row['Número do pedido']),
        }
        records_to_write[doc_id] = unified_record
        
    total_saved, total_orphans = batch_write_to_firestore(records_to_write)
    log_audit("upload_csv", {"product": "Bionio", "rows_saved": total_saved, "rows_orphaned": total_orphans})
    return total_saved, total_orphans

def process_rovema_csv(uploaded_file):
    """Processa o CSV Rovema Pay."""
    try:
        df = pd.read_csv(
            uploaded_file, 
            sep=';', 
            dtype=str, 
            encoding='latin-1'
        )
    except Exception as e:
        st.error(f"Erro ao ler o CSV: {e}")
        return

    st.write(f"Arquivo Rovema Pay lido: {len(df)} linhas encontradas.")
    df_paid = df[df['Status'].isin(['Pago', 'Antecipado'])].copy()
    st.write(f"{len(df_paid)} registros de vendas válidas ('Pago' ou 'Antecipado').")

    if df_paid.empty:
        st.warning("Nenhum registro de venda válida encontrado no arquivo.")
        return 0, 0

    records_to_write = {}
    
    for _, row in df_paid.iterrows():
        cnpj = clean_cnpj(row['CNPJ'])
        data_venda_str = row['Venda']
        
        try:
            data_venda = datetime.strptime(data_venda_str, "%d/%m/%Y %H:%M:%S")
        except:
            continue

        revenue_gross = clean_value(row['Bruto'])
        revenue_net = clean_value(row['Spread']) 
        consultant_uid, manager_uid = map_sale_to_consultant(cnpj)
        doc_id = f"ROVEMA_{row['ID Venda']}_{row['ID Parcela']}"
        
        unified_record = {
            "source": "Rovema Pay",
            "client_cnpj": cnpj,
            "client_name": row['EC'],
            "consultant_uid": consultant_uid,
            "manager_uid": manager_uid,
            "date": data_venda,
            "revenue_gross": revenue_gross,
            "revenue_net": revenue_net,
            "product_name": row['Tipo'],
            "product_detail": row['Bandeira'],
            "status": row['Status'],
            "raw_id": f"{row['ID Venda']}-{row['ID Parcela']}",
        }
        records_to_write[doc_id] = unified_record
        
    total_saved, total_orphans = batch_write_to_firestore(records_to_write)
    log_audit("upload_csv", {"product": "Rovema Pay", "rows_saved": total_saved, "rows_orphaned": total_orphans})
    return total_saved, total_orphans


# --- Funções da API (AGORA SÓ BUSCAM E PROCESSAM) ---

async def fetch_asto_data(start_date, end_date):
    """
    Busca dados ASTO e retorna o dict de registros.
    (Atualmente desativado e retorna None)
    """
    
    # --- MUDANÇA APLICADA ---
    # Interrompe a execução e informa o usuário
    st.error("Integração ASTO (Manutenção) Pausada")
    st.warning("""
    Não foi possível carregar os dados do ASTO (Manutenção).
    
    **Motivo:** Nenhuma das APIs ASTO/Logpay testadas fornece os dados necessários.
    - A API de Fatura (`.../FaturaPagamentoFechadaApuracao`) funciona, mas **não retorna o CNPJ do Cliente**, impedindo a atribuição.
    - A API de Transações (`.../ManutencoesAnalitico`) **retorna erro 404** (Não Encontrado).
    
    **Ação Necessária:** Por favor, entre em contato com o suporte da ASTO/Logpay e solicite um **endpoint de transações analíticas de manutenção** que inclua o `cnpjCliente`, `valor` e `data` de cada transação.
    """)
    
    return None # Retorna None para indicar falha
    # --- FIM DA MUDANÇA ---


async def fetch_eliq_data(start_date, end_date):
    """
    Busca dados ELIQ e retorna o dict de registros.
    """
    try:
        creds = st.secrets["api_credentials"]
        URL_ELIQ = creds["eliq_url"]
        api_token = creds["eliq_token"]
    except KeyError as e:
        st.error(f"Secret 'api_credentials.{e.args[0]}' não encontrado. Verifique seus Secrets.")
        return None
    except Exception as e:
        st.error(f"Erro ao ler Secrets da API: {e}")
        return None

    date_range_str = f"{start_date.strftime('%d/%m/%Y')} - {end_date.strftime('%d/%m/%Y')}"
    params = {
        "TransacaoSearch[data_cadastro]": date_range_str
    }
    
    headers = {
        "Authorization": f"Bearer {api_token}"
    }
    
    records_to_write = {}
    
    full_url_for_log = f"{URL_ELIQ}?{urllib.parse.urlencode(params)}"
    st.info(f"Tentando chamar a API ELIQ (Abastecimento) no endpoint: {full_url_for_log}")

    try:
        async with httpx.AsyncClient(headers=headers, timeout=120.0) as client:
            response = await client.get(URL_ELIQ, params=params)
            response.raise_for_status()
            data = response.json() 

        st.write(f"API ELIQ: {len(data)} transações (abastecimentos) encontradas.")
        if not data:
            st.warning("Nenhum dado retornado pela API ELIQ para o período.")
            return {} # Retorna dict vazio

        for sale in data:
            if sale.get('status') != 'confirmada':
                continue 

            cliente_info = sale.get('cliente', {})
            if not cliente_info:
                cliente_info = sale.get('informacao', {}).get('cliente', {})
            
            if not cliente_info:
                continue 
            
            cnpj = clean_cnpj(cliente_info.get('cnpj'))
            if not cnpj: continue

            data_venda = datetime.strptime(sale['data_cadastro'], "%Y-%m-%d %H:%M:%S")
            revenue_gross = clean_value(sale.get('valor_total', 0))
            
            revenue_net_raw = sale.get('valor_taxa_cliente', sale.get('desconto', 0))
            revenue_net = abs(clean_value(revenue_net_raw))
            
            produto_info = sale.get('produto', {})
            if not produto_info:
                produto_info = sale.get('informacao', {}).get('produto', {})
            
            consultant_uid, manager_uid = map_sale_to_consultant(cnpj)
            doc_id = f"ELIQ_{sale['id']}"
            
            unified_record = {
                "source": "ELIQ",
                "client_cnpj": cnpj,
                "client_name": cliente_info.get('nome', 'N/A'),
                "consultant_uid": consultant_uid,
                "manager_uid": manager_uid,
                "date": data_venda,
                "revenue_gross": revenue_gross,
                "revenue_net": revenue_net,
                "product_name": produto_info.get('nome', 'N/A'),
                "product_detail": produto_info.get('categoria', 'N/A'),
                "volume": clean_value(sale.get('quantidade', 0)),
                "status": sale['status'],
                "raw_id": str(sale.get('id', 'N/A')),
            }
            records_to_write[doc_id] = unified_record
            
        # --- MUDANÇA APLICADA ---
        # Não salva mais, apenas retorna o dict de registros
        return records_to_write
        # ------------------------

    except httpx.HTTPStatusError as e:
        st.error(f"Erro na API ELIQ: {e.response.status_code} - {e.response.text}")
        st.error(f"O URL completo que falhou foi: {full_url_for_log}")
        return None
    except httpx.TimeoutException:
        st.error(f"Erro na API ELIQ: O Timeout de 120 segundos foi excedido. A API está muito lenta.")
        return None
    except Exception as e:
        st.error(f"Erro ao processar dados ELIQ: {e}")
        return None
