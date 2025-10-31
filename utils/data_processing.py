import streamlit as st
import pandas as pd
from utils.firebase_config import get_db
from utils.logger import log_audit  # Importa a nova função de log
import httpx # Para chamadas de API
from datetime import datetime
import urllib.parse # Importado para depuração
import time # IMPORTADO PARA O SLEEP
import json # IMPORTADO PARA DEBUG DO ASTO

# --- FUNÇÕES DE LIMPEZA (ETL) ---

def clean_value(value_str):
    """Limpa valores monetários em string (formato BR)."""
    if pd.isna(value_str):
        return 0.0
    if isinstance(value_str, (int, float)):
        return float(value_str)
    
    value_str = str(value_str).strip()
    value_str = value_str.replace("R$", "").replace("%", "")
    value_str = value_str.replace(".", "").replace(",", ".") # Converte 1.000,00 para 1000.00
    try:
        return float(value_str)
    except ValueError:
        return 0.0

def clean_cnpj(cnpj_str):
    """Limpa e padroniza CNPJs."""
    if pd.isna(cnpj_str):
        return None
    
    cnpj_str = str(cnpj_str)
    
    # Remove cotações (ex: "3,96829E+12")
    if 'E' in cnpj_str.upper():
        try:
            cnpj_str = "{:.0f}".format(float(cnpj_str.replace(',', '.')))
        except:
            pass # Deixa seguir para a limpeza padrão

    # Remove caracteres não numéricos
    cleaned_cnpj = "".join(filter(str.isdigit, cnpj_str))
    return cleaned_cnpj.zfill(14) # Garante que tem 14 dígitos


# --- MAPPER DE CARTEIRA (O CORAÇÃO DO SISTEMA) ---

@st.cache_data(ttl=600) # Cache de 10 minutos para o mapa de clientes
def get_client_portfolio_map():
    """
    Busca no Firestore e cria um dicionário (mapa) de:
    { "cnpj_limpo": {"consultant_uid": "...", "manager_uid": "..."} }
    Esta é a função mais importante para performance.
    """
    db = get_db()
    clients_ref = db.collection("clients").stream()
    client_map = {}
    for client in clients_ref:
        data = client.to_dict()
        cnpj = client.id # O ID do documento é o CNPJ limpo
        client_map[cnpj] = {
            "consultant_uid": data.get("consultant_uid"),
            "manager_uid": data.get("manager_uid")
        }
    return client_map

def map_sale_to_consultant(cnpj):
    """Mapeia uma venda (via CNPJ) ao consultor/gestor."""
    client_map = get_client_portfolio_map() # Usa o mapa em cache
    cnpj_limpo = clean_cnpj(cnpj)
    
    if cnpj_limpo in client_map:
        return client_map[cnpj_limpo]["consultant_uid"], client_map[cnpj_limpo]["manager_uid"]
    else:
        return None, None # Venda "Órfã"

# --- FUNÇÕES DE CARGA (POR PRODUTO) ---

def batch_write_to_firestore(records):
    """Escreve os registros em lotes no Firestore."""
    db = get_db()
    batch = db.batch()
    count = 0
    total_written = 0
    total_orphans = 0 # Contagem de órfãs
    
    total_records = len(records)
    progress_bar = st.progress(0, text="Salvando dados no banco... (Isso pode levar vários minutos)")
    
    for i, (doc_id, data) in enumerate(records.items()):
        doc_ref = db.collection("sales_data").document(doc_id)
        batch.set(doc_ref, data) # .set() faz o "upsert" (cria ou sobrescreve)
        count += 1
        
        if data.get("consultant_uid") is None:
            total_orphans += 1
        
        if count == 499: # Limite do batch é 500
            batch.commit()
            total_written += count
            
            # Pausa para evitar Rate Limit (Erro 429)
            time.sleep(1) # Pausa por 1 segundo
            
            batch = db.batch() # Novo batch
            count = 0
            
            # Atualiza a barra de progresso
            progress_percentage = (i + 1) / total_records
            progress_bar.progress(progress_percentage, text=f"Salvando dados... ({total_written} / {total_records} registros)")
            
    if count > 0:
        batch.commit() # Salva o lote final
        total_written += count
        
    progress_bar.progress(1.0, text=f"Concluído! {total_written} registros salvos.")
    
    # Invalida o cache do mapa de clientes
    st.cache_data.clear()

    return total_written, total_orphans # Retorna contagem de órfãs

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
    
    # 1. Filtra apenas pedidos pagos/transferidos
    df_paid = df[df['Status do pedido'].isin(['Transferido', 'Pago e Agendado'])].copy()
    st.write(f"{len(df_paid)} registros de vendas válidas ('Transferido' ou 'Pago e Agendado').")
    
    if df_paid.empty:
        st.warning("Nenhum registro de venda válida encontrado no arquivo.")
        return 0, 0 # Retorna zero para não dar erro na página admin

    records_to_write = {}
    
    for _, row in df_paid.iterrows():
        # 2. Limpeza e ETL
        cnpj = clean_cnpj(row['CNPJ da organização'])
        data_pagamento_str = row['Data do pagamento do pedido']
        
        try:
            data_pagamento = datetime.strptime(data_pagamento_str, "%d/%m/%Y")
        except:
            continue # Pula se a data do pagamento for inválida

        revenue = clean_value(row['Valor total do pedido'])
        
        # 3. Mapeamento
        consultant_uid, manager_uid = map_sale_to_consultant(cnpj)
        
        # 4. Gera ID único (Evita duplicidade)
        # Bionio_NumeroPedido_DataPagamento
        doc_id = f"BIONIO_{row['Número do pedido']}_{data_pagamento_str.replace('/', '-')}"
        
        # 5. Monta o registro unificado
        unified_record = {
            "source": "Bionio",
            "client_cnpj": cnpj,
            "client_name": row['Nome fantasia'],
            "consultant_uid": consultant_uid,
            "manager_uid": manager_uid,
            "date": data_pagamento, # Timestamp
            "revenue_gross": revenue,
            "revenue_net": revenue, # Bionio não tem spread, usamos o valor total
            "product_name": row['Nome do benefício'],
            "status": row['Status do pedido'],
            "payment_type": row['Tipo de pagamento'],
            "raw_id": str(row['Número do pedido']),
        }
        records_to_write[doc_id] = unified_record
        
    # 6. Salva no Firestore
    total_saved, total_orphans = batch_write_to_firestore(records_to_write)
    
    log_audit(
        action="upload_csv",
        details={
            "product": "Bionio",
            "rows_found": len(df),
            "rows_processed": len(df_paid),
            "rows_saved": total_saved,
            "rows_orphaned": total_orphans
        }
    )
    
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
    
    # 1. Filtra apenas transações pagas
    df_paid = df[df['Status'].isin(['Pago', 'Antecipado'])].copy()
    st.write(f"{len(df_paid)} registros de vendas válidas ('Pago' ou 'Antecipado').")

    if df_paid.empty:
        st.warning("Nenhum registro de venda válida encontrado no arquivo.")
        return 0, 0 # Retorna zero para não dar erro na página admin

    records_to_write = {}
    
    for _, row in df_paid.iterrows():
        # 2. Limpeza e ETL
        cnpj = clean_cnpj(row['CNPJ'])
        data_venda_str = row['Venda'] # Ex: 01/09/2025 07:01:16
        
        try:
            data_venda = datetime.strptime(data_venda_str, "%d/%m/%Y %H:%M:%S")
        except:
            continue # Pula se a data for inválida

        revenue_gross = clean_value(row['Bruto'])
        # Métrica de receita: Assumindo que "Spread" é a nossa receita
        revenue_net = clean_value(row['Spread']) 
        
        # 3. Mapeamento
        consultant_uid, manager_uid = map_sale_to_consultant(cnpj)
        
        # 4. Gera ID único
        doc_id = f"ROVEMA_{row['ID Venda']}_{row['ID Parcela']}"
        
        # 5. Monta o registro unificado
        unified_record = {
            "source": "Rovema Pay",
            "client_cnpj": cnpj,
            "client_name": row['EC'],
            "consultant_uid": consultant_uid,
            "manager_uid": manager_uid,
            "date": data_venda, # Timestamp
            "revenue_gross": revenue_gross,
            "revenue_net": revenue_net, # Receita da empresa
            "product_name": row['Tipo'], # Débito / Crédito
            "product_detail": row['Bandeira'], # mastercard, visa
            "status": row['Status'],
            "raw_id": f"{row['ID Venda']}-{row['ID Parcela']}",
        }
        records_to_write[doc_id] = unified_record
        
    # 6. Salva no Firestore
    total_saved, total_orphans = batch_write_to_firestore(records_to_write)
    
    log_audit(
        action="upload_csv",
        details={
            "product": "Rovema Pay",
            "rows_found": len(df),
            "rows_processed": len(df_paid),
            "rows_saved": total_saved,
            "rows_orphaned": total_orphans
        }
    )
    
    return total_saved, total_orphans

async def process_asto_api(start_date, end_date):
    """
    Processa a API ASTO (Logpay) - Manutenção.
    Alterado para usar o endpoint 'FaturaPagamentoFechadaApuracao'
    que o usuário confirmou que funciona.
    """
    
    full_url_for_log = "https://revistacasaejardim.globo.com/arquitetura/noticia/2025/02/como-o-mundo-teria-sido-23-projetos-arquitetonicos-que-nunca-foram-construidos.ghtml"
    
    try:
        creds = st.secrets["api_credentials"]
        
        # 'asto_url' DEVE ser ".../api/Fatura/FaturaPagamentoFechadaApuracao"
        URL_ASTO_BASE = creds["asto_url"] 
        api_user = creds["asto_username"]
        api_pass = creds["asto_password"]
        
        asto_spread_rate = float(creds.get("asto_spread_rate", 0.015)) 
        
    except KeyError as e:
        st.error(f"Secret 'api_credentials.{e.args[0]}' não encontrado. Verifique seus Secrets.")
        st.error("Certifique-se de que 'asto_url', 'asto_username' e 'asto_password' existem.")
        return
    except Exception as e:
        st.error(f"Erro ao ler Secrets da API: {e}")
        return

    # --- CORREÇÃO APLICADA AQUI ---
    # As datas são formatadas e anexadas ao URL
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")
    
    # O URL final é construído com as datas no caminho (path)
    URL_ASTO_FINAL = f"{URL_ASTO_BASE}/{start_str}/{end_str}"
    
    # Parâmetros de query (params) estão vazios
    params = {}
    # -----------------------------
    
    auth = (api_user, api_pass)
    records_to_write = {}
    
    try:
        # Atualiza a variável de log
        full_url_for_log = URL_ASTO_FINAL
        st.info(f"Tentando chamar a API ASTO (Manutenção) no endpoint: {full_url_for_log}")
        
        async with httpx.AsyncClient(auth=auth, timeout=30.0) as client:
            # Chama o URL final e passa os parâmetros (vazios)
            response = await client.get(URL_ASTO_FINAL, params=params)
            response.raise_for_status() 
            data = response.json()

        st.success(f"API ASTO: {len(data)} faturas de manutenção encontradas.")
        if not data:
            st.warning("Nenhum dado retornado pela API ASTO para o período.")
            return 0, 0
        
        # --- BLOCO DE PROCESSAMENTO (ASTO) ---
        try:
            for sale in data:
                # --- VERIFICAÇÃO DE DADOS CRÍTICOS ---
                # Este endpoint (Fatura) não contém o CNPJ do cliente.
                # Ele contém o CNPJ do *Estabelecimento* (ex: 'cnpjEstabelecimento')
                # mas não do cliente que comprou.
                if 'cnpjCliente' not in sale:
                    st.error("Erro Crítico de Dados: A API de Fatura (ASTO) foi chamada com sucesso, mas ela não retorna o 'cnpjCliente' nas transações.")
                    st.error("Sem o CNPJ do Cliente, não é possível atribuir a venda a um consultor.")
                    st.warning("Por favor, solicite à ASTO/Logpay o endpoint de 'Transações Analíticas de Manutenção' que inclua o CNPJ do cliente.")
                    st.info("Amostra do primeiro registro recebido (para depuração):")
                    st.json(data[0])
                    return
                # --- FIM DA VERIFICAÇÃO ---
                
                # Este código (abaixo) provavelmente nunca será executado
                cnpj = clean_cnpj(sale.get('cnpjCliente'))
                if not cnpj: 
                    continue 

                data_venda = datetime.fromisoformat(sale['dataInicioApuracao'])
                revenue_gross = float(sale.get('valorBruto', 0))
                revenue_net = float(sale.get('valorLiquido', 0))
                
                # Se 'valorLiquido' não for a receita, usamos o spread
                if revenue_net == 0:
                    revenue_net = revenue_gross * asto_spread_rate 
                
                consultant_uid, manager_uid = map_sale_to_consultant(cnpj)
                doc_id = f"ASTO_{sale['faturaPagamentoID']}"
                
                unified_record = {
                    "source": "ASTO",
                    "client_cnpj": cnpj,
                    "client_name": "N/A (API de Fatura)",
                    "consultant_uid": consultant_uid,
                    "manager_uid": manager_uid,
                    "date": data_venda,
                    "revenue_gross": revenue_gross,
                    "revenue_net": revenue_net,
                    "product_name": "Manutenção (Fatura)",
                    "product_detail": sale.get('estabelecimentoNomeFantasia', 'N/A'),
                    "volume": 0,
                    "status": "Confirmado",
                    "raw_id": str(sale.get('faturaPagamentoID', 'N/A')),
                }
                records_to_write[doc_id] = unified_record
        
        except KeyError as e:
            st.error(f"Erro ao processar o JSON da ASTO. Chave não encontrada: {e}")
            st.info("Amostra do primeiro registro recebido (para depuração):")
            st.json(data[0] if data else "Nenhum dado recebido.")
            return
        except Exception as e:
            st.error(f"Erro inesperado ao processar os dados da ASTO: {e}")
            st.info("Amostra do primeiro registro recebido (para depuração):")
            st.json(data[0] if data else "Nenhum dado recebido.")
            return
        # --- FIM DO BLOCO ---
            
        total_saved, total_orphans = batch_write_to_firestore(records_to_write)
        
        log_audit(
            action="load_api",
            details={
                "product": "ASTO (Manutenção)",
                "start_date": start_date.strftime("%Y-%m-%d"),
                "end_date": end_date.strftime("%Y-%m-%d"),
                "rows_found": len(data),
                "rows_saved": total_saved,
                "rows_orphaned": total_orphans
            }
        )
        
        return total_saved, total_orphans

    except httpx.HTTPStatusError as e:
        st.error(f"Erro na API ASTO: {e.response.status_code} - {e.response.text}")
        st.error(f"O URL que falhou foi: {full_url_for_log}")
    except Exception as e:
        st.error(f"Erro ao processar dados ASTO: {e}")


async def process_eliq_api(start_date, end_date):
    """
    Processa a API ELIQ (Uzzipay/Sigyo) - ABastecimento.
    Contém as correções de Timeout e Rate Limit.
    """
    try:
        creds = st.secrets["api_credentials"]
        URL_ELIQ = creds["eliq_url"] # Deve ser ".../api/transacoes"
        api_token = creds["eliq_token"]
    except KeyError as e:
        st.error(f"Secret 'api_credentials.{e.args[0]}' não encontrado. Verifique seus Secrets.")
        return
    except Exception as e:
        st.error(f"Erro ao ler Secrets da API: {e}")
        return

    # Formato dos Parâmetros (Corrigido)
    date_range_str = f"{start_date.strftime('%d/%m/%Y')} - {end_date.strftime('%d/%m/%Y')}"
    params = {
        "TransacaoSearch[data_cadastro]": date_range_str
    }
    
    headers = {
        "Authorization": f"Bearer {api_token}"
    }
    
    records_to_write = {}
    
    # Log de Depuração
    full_url_for_log = f"{URL_ELIQ}?{urllib.parse.urlencode(params)}"
    st.info(f"Tentando chamar a API ELIQ (Abastecimento) no endpoint: {full_url_for_log}")

    try:
        # Timeout aumentado para 120 segundos
        async with httpx.AsyncClient(headers=headers, timeout=120.0) as client:
            response = await client.get(URL_ELIQ, params=params)
            response.raise_for_status()
            data = response.json() 

        st.write(f"API ELIQ: {len(data)} transações (abastecimentos) encontradas.")
        if not data:
            st.warning("Nenhum dado retornado pela API ELIQ para o período.")
            return 0, 0

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
            
            # Métrica de Receita (Corrigida)
            revenue_net_raw = sale.get('valor_taxa_cliente', sale.get('desconto', 0))
            revenue_net = abs(clean_value(revenue_net_raw))
            
            produto_info = sale.get('produto', {})
            if not produto_info:
                produto_info = sale.get('informacao', {}).get('produto', {})
            
            # 3. Mapeamento
            consultant_uid, manager_uid = map_sale_to_consultant(cnpj)
            
            # 4. Gera ID único
            doc_id = f"ELIQ_{sale['id']}"
            
            # 5. Monta o registro unificado
            unified_record = {
                "source": "ELIQ",
                "client_cnpj": cnpj,
                "client_name": cliente_info.get('nome', 'N/A'),
                "consultant_uid": consultant_uid,
                "manager_uid": manager_uid,
                "date": data_venda,
                "revenue_gross": revenue_gross,
                "revenue_net": revenue_net, # Receita Corrigida
                "product_name": produto_info.get('nome', 'N/A'),
                "product_detail": produto_info.get('categoria', 'N/A'),
                "volume": clean_value(sale.get('quantidade', 0)),
                "status": sale['status'],
                "raw_id": str(sale.get('id', 'N/A')),
            }
            records_to_write[doc_id] = unified_record
            
        # 6. Salva no Firestore
        # Esta função (batch_write_to_firestore) agora tem o sleep(1)
        total_saved, total_orphans = batch_write_to_firestore(records_to_write)
        
        log_audit(
            action="load_api",
            details={
                "product": "ELIQ (Abastecimento)",
                "start_date": start_date.strftime("%Y-%m-%d"),
                "end_date": end_date.strftime("%Y-%m-%d"),
                "rows_found": len(data),
                "rows_saved": total_saved,
                "rows_orphaned": total_orphans
            }
        )
        
        return total_saved, total_orphans

    except httpx.HTTPStatusError as e:
        st.error(f"Erro na API ELIQ: {e.response.status_code} - {e.response.text}")
        st.error(f"O URL completo que falhou foi: {full_url_for_log}")
    except httpx.TimeoutException:
        st.error(f"Erro na API ELIQ: O Timeout de 120 segundos foi excedido. A API está muito lenta.")
    except Exception as e:
        st.error(f"Erro ao processar dados ELIQ: {e}")
