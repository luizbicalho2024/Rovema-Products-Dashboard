import streamlit as st
import altair as alt
from utils.data_processing import fetch_asto_data, fetch_eliq_data
from datetime import date
from fire_admin import log_event

def dashboard_page():
    if not st.session_state.get('authenticated'):
        st.error("Acesso negado. Por favor, faça login na página principal.")
        return

    st.title("📈 Dashboard Consolidado de Performance")
    log_event("VIEW_DASHBOARD", "Visualizando o dashboard principal.")
    
    st.subheader("Filtros Globais")
    col_d1, col_d2 = st.columns(2)
    with col_d1:
        start_date = st.date_input("Data de Início", date(2025, 9, 1))
    with col_d2:
        end_date = st.date_input("Data Final", date(2025, 10, 31))

    # --- Aba Asto & Eliq (APIs) ---
    st.header("1. Performance Asto e Eliq (Dados de API)")
    
    # Asto: Evolução do Valor Transacionado vs Receita
    asto_df = fetch_asto_data(start_date.isoformat(), end_date.isoformat())
    asto_chart = alt.Chart(asto_df).mark_line(point=True).encode(
        x=alt.X('dataFimApuracao', title='Período de Apuração'),
        y=alt.Y('valorBruto', title='Valor Bruto (R$)'),
        tooltip=['dataFimApuracao', 'valorBruto', 'Receita']
    ).properties(title='ASTO: Evolução do Valor Transacionado (Bruto)').interactive()
    
    asto_revenue_chart = alt.Chart(asto_df).mark_area(opacity=0.4).encode(
        x='dataFimApuracao',
        y=alt.Y('Receita', title='Receita (R$)'),
        tooltip=['dataFimApuracao', 'Receita']
    )

    st.altair_chart(asto_chart + asto_revenue_chart, use_container_width=True)
    st.markdown("---")

    # Eliq: Volume Transacionado e Consumo Médio
    eliq_df = fetch_eliq_data(start_date.isoformat(), end_date.isoformat())
    eliq_chart = alt.Chart(eliq_df).mark_bar().encode(
        x='data_cadastro',
        y=alt.Y('valor_total', title='Volume Transacionado Eliq (R$)'),
        tooltip=['data_cadastro', 'valor_total']
    ).properties(title='ELIQ: Volume Total de Transações por Dia').interactive()
    
    st.altair_chart(eliq_chart, use_container_width=True)
    st.markdown("---")

    # --- Aba Bionio & Rovema Pay (Arquivos Firebase) ---
    st.header("2. Performance Bionio e Rovema Pay (Dados de Arquivo)")
    st.warning("⚠️ **AVISO:** Para visualizar os dados de Bionio e Rovema Pay, acesse a página 'Upload de Dados' e envie os arquivos CSV/Excel.")
    
    # Aqui, a lógica real faria download e processamento dos dados mais recentes do Firebase Storage
    
    st.metric(label="Bionio - Último Valor Total Processado", value="R$ XX.XXX,XX")
    st.metric(label="Rovema Pay - Taxa MDR Média (Último Upload)", value="X.XX%")

dashboard_page()
