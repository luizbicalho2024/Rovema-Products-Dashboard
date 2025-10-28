import pandas as pd
import numpy as np
import requests
import streamlit as st
from datetime import datetime
import altair as alt
from fire_admin import log_event, db, server_timestamp
import json 

# --- MOCKS de APIs (Asto e Eliq) ---
# ... (Mantenha as funções fetch_asto_data e fetch_eliq_data IGUAIS) ...

def fetch_asto_data(start_date: str, end_date: str):
    """
    Simula a coleta de dados de Fatura Pagamento Fechada do Asto (API).
    """
    log_event("API_CALL_ASTO", f"Simulação de chamada Asto para {start_date} a {end_date}")
    
    data = [
        {"dataFimApuracao": "2025-10-07T00:00:00", "valorBruto": 2159.81, "valorLiquido": 2084.21},
        {"dataFimApuracao": "2025-10-14T00:00:00", "valorBruto": 15986.99, "valorLiquido": 15427.44},
        {"dataFimApuracao": "2025-10-14T00:00:00", "valorBruto": 683.80, "valorLiquido": 659.86},
        {"dataFimApuracao": "2025-10-21T00:00:00", "valorBruto": 9970.72, "valorLiquido": 9621.74},
        {"dataFimApuracao": "2025-10-28T00:00:00", "valorBruto": 3582.86, "valorLiquido": 3457.45},
    ]
    df = pd.DataFrame(data)
    df['dataFimApuracao'] = pd.to_datetime(df['dataFimApuracao']).dt.normalize()
    df['Receita'] = df['valorBruto'] - df['valorLiquido']
    return df.groupby('dataFimApuracao')[['valorBruto', 'Receita']].sum().reset_index()

def fetch_eliq_data(start_date: str, end_date: str):
    """
    Simula a coleta de dados de Transações do Eliq (API).
    """
    log_event("API_CALL_ELIQ", f"Simulação de chamada Eliq para {start_date} a {end_date}")
    
    data = [
        {"data_cadastro": "2025-09-01 10:00:00", "valor_total": 500.00, "consumo_medio": 7.5, "status": "confirmada"},
        {"data_cadastro": "2025-09-08 15:30:00", "valor_total": 1200.50, "consumo_medio": 6.8, "status": "confirmada"},
        {"data_cadastro": "2025-09-15 11:20:00", "valor_total": 850.25, "consumo_medio": 7.2, "status": "confirmada"},
        {"data_cadastro": "2025-09-22 09:00:00", "valor_total": 1500.00, "consumo_medio": 7.0, "status": "confirmada"},
        {"data_cadastro": "2025-09-29 14:00:00", "valor_total": 900.00, "consumo_medio": 7.3, "status": "confirmada"},
    ]
    df = pd.DataFrame(data)
    df['data_cadastro'] = pd.to_datetime(df['data_cadastro']).dt.normalize()
    return df.groupby('data_cadastro')[['valor_total', 'consumo_medio']].agg({'valor_total': 'sum', 'consumo_medio': 'mean'}).reset_index()

# --- Funções de Processamento de Arquivos ---

def process_uploaded_file(uploaded_file, product_name):
    """
    Processa o arquivo CSV/Excel e o prepara para visualização de preview e salvamento.
    """
    try:
        is_csv = uploaded_file.name.lower().endswith('.csv')
        
        if is_csv:
            uploaded_file.seek(0)
            # Tenta leitura robusta com o separador mais comum para Excel/CSV brasileiro (;)
            try:
                df = pd.read_csv(uploaded_file, decimal=',', sep=';', thousands='.')
            except:
                uploaded_file.seek(0)
                # Fallback para leitura padrão (vírgula ou tab)
                df = pd.read_csv(uploaded_file)
        else: # XLSX
            uploaded_file.seek(0)
            # Lê o Excel
            df = pd.read_excel(uploaded_file)
        
        if product_name == 'Bionio':
            df_processed = process_bionio_data(df.copy())
        elif product_name == 'RovemaPay':
            df_processed = process_rovemapay_data(df.copy())
        else:
            df_processed = df.head(5)
            
        return True, "Processamento bem-sucedido.", df_processed
        
    except Exception as e:
        log_event("FILE_PROCESSING_FAIL", f"Erro no processamento de {product_name}: {e}")
        # Retorna o erro real para o usuário
        return False, f"Erro no processamento do arquivo: {e}", None 

def process_bionio_data(df):
    """Processamento específico para dados do Bionio."""
    df['Valor total do pedido'] = df['Valor total do pedido'].astype(str).str.replace(r'[\sR\$]', '', regex=True).str.replace('.', '', regex=False).str.replace(',', '.', regex=False)
    
    df['Valor total do pedido'] = pd.to_numeric(df['Valor total do pedido'], errors='coerce')
    df['Data da criação do pedido'] = pd.to_datetime(df['Data da criação do pedido'], format='%d/%m/%Y', errors='coerce')
    
    df = df.dropna(subset=['Valor total do pedido', 'Data da criação do pedido'])
    
    df_agg = df.groupby(df['Data da criação do pedido'].dt.to_period('M'))['Valor total do pedido'].sum().reset_index()
    df_agg['Mês'] = df_agg['Data da criação do pedido'].astype(str)
    return df_agg.rename(columns={'Valor total do pedido': 'Valor Total Pedidos'}).drop(columns=['Data da criação do pedido'])

def process_rovemapay_data(df):
    """Processamento específico para dados do Rovema Pay (AGORA MAIS ROBUSTO)."""
    
    # Função auxiliar para limpar e converter valores de string, tratando NaNs
    def clean_value(series_or_key, df_source=df):
        # Usa .get() para defensividade contra colunas inexistentes (embora improvável aqui)
        series = df_source.get(series_or_key) 
        if series is None:
            return pd.Series([np.nan] * len(df_source)) # Retorna nulo se a coluna não existir
            
        if series.dtype == 'object':
            # Usa .fillna('') para evitar erros de string em células vazias
            cleaned = series.fillna('').astype(str).str.replace(r'[\sR\$\%]', '', regex=True).str.replace('.', '', regex=False).str.replace(',', '.', regex=False)
            return cleaned
        return series
        
    # Conversão de colunas de valor
    df['Liquido'] = pd.to_numeric(clean_value('Liquido'), errors='coerce')
    df['Bruto'] = pd.to_numeric(clean_value('Bruto'), errors='coerce')
    df['Taxa Cliente'] = pd.to_numeric(clean_value('Taxa Cliente'), errors='coerce')
    df['Taxa Adquirente'] = pd.to_numeric(clean_value('Taxa Adquirente'), errors='coerce')
    
    # Conversão de Data, tratando NaNs com errors='coerce'
    df['Venda'] = pd.to_datetime(df.get('Venda'), errors='coerce')
    
    # Limpeza: Remove linhas com valores nulos nas colunas críticas
    df = df.dropna(subset=['Bruto', 'Liquido', 'Venda'])
    
    # Cálculo da Receita (MDR + Spread)
    df['Receita'] = df['Bruto'] - df['Liquido']
    
    # Cálculo do Custo_Total_Perc (Defensivo contra divisão por zero)
    df['Custo_Total_Perc'] = np.where(df['Bruto'] != 0, (df['Receita'] / df['Bruto']) * 100, 0)
    
    # Agrupamento para o dashboard
    df_agg = df.groupby([df['Venda'].dt.to_period('M'), 'Status']).agg(
        Liquido=('Liquido', 'sum'),
        Receita=('Receita', 'sum'),
        Taxa_Media=('Custo_Total_Perc', 'mean')
    ).reset_index()
    
    df_agg['Mês'] = df_agg['Venda'].astype(str)
    return df_agg.drop(columns=['Venda'])

# --- FUNÇÃO: BUSCAR DADOS DO FIRESTORE ---
# ... (Mantenha a função get_latest_uploaded_data IGUAL) ...

@st.cache_data(ttl=600) # Cache de 10 minutos para dados do Firestore
def get_latest_uploaded_data(product_name):
    """
    Busca todos os dados da coleção de um produto no Firestore e retorna como DataFrame.
    """
    if 'db' not in st.session_state:
        return pd.DataFrame()
        
    collection_name = f"data_{product_name.lower()}"
    
    try:
        docs = st.session_state['db'].collection(collection_name).limit(1000).stream()
        data_list = []
        for doc in docs:
            data = doc.to_dict()
            data_list.append(data)

        if not data_list:
            return pd.DataFrame()

        df = pd.DataFrame(data_list)
        
        if product_name == 'Bionio' and 'Valor Total Pedidos' in df.columns:
            df['Valor Total Pedidos'] = pd.to_numeric(df['Valor Total Pedidos'], errors='coerce')
            
        elif product_name == 'RovemaPay' and 'Receita' in df.columns:
            df['Receita'] = pd.to_numeric(df['Receita'], errors='coerce')
            df['Liquido'] = pd.to_numeric(df['Liquido'], errors='coerce')
            df['Taxa_Media'] = pd.to_numeric(df['Taxa_Media'], errors='coerce')

        return df.dropna()

    except Exception as e:
        log_event("FIRESTORE_FETCH_FAIL", f"Falha ao buscar dados de {product_name} no Firestore: {e}")
        return pd.DataFrame()
