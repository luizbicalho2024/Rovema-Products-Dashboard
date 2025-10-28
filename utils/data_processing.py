import pandas as pd
import numpy as np
import requests
import streamlit as st
from fire_admin import db
from datetime import datetime

# --- MOCKS de APIs (Asto e Eliq) ---

def fetch_asto_data(start_date: str, end_date: str):
    """
    Simula a coleta de dados de Fatura Pagamento Fechada do Asto (API).
    Em um projeto real, 'requests.get' seria usado aqui.
    """
    st.session_state['db'].collection('logs').add({
        "timestamp": firestore.SERVER_TIMESTAMP,
        "user_email": st.session_state.get('user_email', 'SISTEMA'),
        "action": "API_CALL_ASTO",
        "details": f"Simulação de chamada Asto para {start_date} a {end_date}"
    })
    
    # Mock de dados com base no JSON que você forneceu
    data = [
        {"dataFimApuracao": "2025-10-07T00:00:00", "valorBruto": 2159.81, "valorLiquido": 2084.21},
        {"dataFimApuracao": "2025-10-14T00:00:00", "valorBruto": 15986.99, "valorLiquido": 15427.44},
        {"dataFimApuracao": "2025-10-14T00:00:00", "valorBruto": 683.80, "valorLiquido": 659.86},
        {"dataFimApuracao": "2025-10-21T00:00:00", "valorBruto": 9970.72, "valorLiquido": 9621.74},
        {"dataFimApuracao": "2025-10-21T00:00:00", "valorBruto": 3582.86, "valorLiquido": 3457.45},
    ]
    df = pd.DataFrame(data)
    df['dataFimApuracao'] = pd.to_datetime(df['dataFimApuracao']).dt.normalize()
    df['Receita'] = df['valorBruto'] - df['valorLiquido']
    return df.groupby('dataFimApuracao')[['valorBruto', 'Receita']].sum().reset_index()

def fetch_eliq_data(start_date: str, end_date: str):
    """
    Simula a coleta de dados de Transações do Eliq (API).
    Em um projeto real, 'requests.get' seria usado aqui.
    """
    st.session_state['db'].collection('logs').add({
        "timestamp": firestore.SERVER_TIMESTAMP,
        "user_email": st.session_state.get('user_email', 'SISTEMA'),
        "action": "API_CALL_ELIQ",
        "details": f"Simulação de chamada Eliq para {start_date} a {end_date}"
    })
    
    # Mock de dados de transação com foco em volume e consumo
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

# --- Funções de Processamento de Arquivos Locais (Bionio e Rovema Pay) ---

def process_uploaded_file(uploaded_file, product_name):
    """
    Processa o arquivo CSV/Excel e o prepara para visualização.
    Esta função simula o processamento que ocorreria antes de armazenar no Firestore/Storage
    e o consumo para o Dashboard.
    """
    try:
        # Tenta ler o arquivo como CSV e depois como Excel
        if uploaded_file.name.endswith('.csv'):
            df = pd.read_csv(uploaded_file, decimal=',', sep=',', thousands='.')
        else:
            df = pd.read_excel(uploaded_file)
        
        if product_name == 'Bionio':
            df_processed = process_bionio_data(df)
        elif product_name == 'RovemaPay':
            df_processed = process_rovemapay_data(df)
        else:
            df_processed = df.head() # Retorna só o cabeçalho para outros tipos
            
        return True, "Processamento bem-sucedido.", df_processed
        
    except Exception as e:
        return False, f"Erro no processamento do arquivo: {e}", None

def process_bionio_data(df):
    """Processamento específico para dados do Bionio."""
    # Conversão de colunas de valor e data conforme análise anterior
    df['Valor total do pedido'] = df['Valor total do pedido'].replace('[\s\.,R\$]', '', regex=True).astype(float) / 100
    df['Data da criação do pedido'] = pd.to_datetime(df['Data da criação do pedido'], format='%d/%m/%Y', errors='coerce')
    
    # Agrupamento para o dashboard
    return df.groupby(df['Data da criação do pedido'].dt.to_period('M'))['Valor total do pedido'].sum().reset_index().rename(columns={'Valor total do pedido': 'Valor Total'})

def process_rovemapay_data(df):
    """Processamento específico para dados do Rovema Pay."""
    # Conversão de colunas de valor e taxa conforme análise anterior
    df['Liquido'] = df['Liquido'].replace('[\s\.]', '', regex=True).astype(float) / 100
    df['Bruto'] = df['Bruto'].replace('[\s\.]', '', regex=True).astype(float) / 100
    df['Venda'] = pd.to_datetime(df['Venda'])
    
    # Cálculo da Receita (MDR + Spread) e Agrupamento
    df['Receita'] = df['Bruto'] - df['Liquido']
    
    return df.groupby([df['Venda'].dt.to_period('M'), 'Status'])[['Liquido', 'Receita']].sum().reset_index()
