import streamlit as st
import altair as alt
from datetime import date, timedelta
import pandas as pd
from fire_admin import log_event
from utils.data_processing import fetch_asto_data, fetch_eliq_data, get_latest_uploaded_data

def dashboard_page():
    if not st.session_state.get('authenticated'):
        st.error("Acesso negado. Por favor, faça login na página principal.")
        return

    st.title("📈 Dashboard Consolidado Multi-Produto")
    log_event("VIEW_DASHBOARD", "Visualizando o dashboard principal.")
    
    # --- FILTROS GLOBAIS ---
    st.subheader("Filtros de Período")
    
    default_end_date = date(2025, 10, 31)
    default_start_date = default_end_date - timedelta(days=90)
    
    col_d1, col_d2 = st.columns(2)
    with col_d1:
        start_date = st.date_input("Data de Início", default_start_date)
    with col_d2:
        end_date = st.date_input("Data Final", default_end_date)
        
    st.markdown("---")

    # --- 1. PERFORMANCE ASTO & ELIQ (Mocks de API) ---
    st.header("1. Performance Asto e Eliq (Dados de API)")
    
    asto_df = fetch_asto_data(start_date.isoformat(), end_date.isoformat())
    eliq_df = fetch_eliq_data(start_date.isoformat(), end_date.isoformat())

    # Indicadores Chave (Métricas)
    col_k1, col_k2, col_k3 = st.columns(3)
    col_k1.metric("Asto: Valor Bruto Total", f"R$ {asto_df['valorBruto'].sum():,.2f}", delta=f"R$ {asto_df['Receita'].sum():,.2f} Receita")
    col_k2.metric("Eliq: Volume Total", f"R$ {eliq_df['valor_total'].sum():,.2f}")
    col_k3.metric("Eliq: Consumo Médio Geral", f"{eliq_df['consumo_medio'].mean():.2f} km/l")
    
    
    # ASTO: Evolução do Valor Transacionado vs Receita (Gráfico principal)
    st.subheader("ASTO: Evolução Valor Transacionado vs Receita")
    
    asto_long = asto_df.melt('dataFimApuracao', var_name='Métrica', value_name='Valor')
    
    asto_chart = alt.Chart(asto_long).mark_line(point=True).encode(
        x=alt.X('dataFimApuracao', title='Período de Apuração (Fim)'),
        y=alt.Y('Valor', title='Valor (R$)'),
        color='Métrica',
        tooltip=['dataFimApuracao', alt.Tooltip('Valor', format='$,.2f'), 'Métrica']
    ).properties(title='Evolução Semanal de Valores do Asto').interactive()

    st.altair_chart(asto_chart, use_container_width=True)
    st.markdown("---")


    # --- 2. PERFORMANCE BIONIO & ROVEMAPAY (Dados de Upload) ---
    st.header("2. Performance Bionio e Rovema Pay (Dados de Banco de Dados)")
    
    # CHAMA A NOVA FUNÇÃO DE BUSCA DO FIRESTORE
    bionio_df_db = get_latest_uploaded_data('Bionio')
    rovemapay_df_db = get_latest_uploaded_data('RovemaPay')

    if bionio_df_db.empty and rovemapay_df_db.empty:
         st.warning("⚠️ **PENDÊNCIA:** Não há dados de Bionio ou Rovema Pay no Banco de Dados. Acesse a página **Upload de Dados** e envie os arquivos CSV/Excel para popular o Dashboard.")
         return
         
    # BIONIO
    st.subheader("BIONIO: Evolução do Valor Total dos Pedidos")
    if not bionio_df_db.empty:
        bionio_chart = alt.Chart(bionio_df_db).mark_bar().encode(
            x=alt.X('Mês:O', title='Mês'),
            y=alt.Y('Valor Total Pedidos', title='Valor Total (R$)'),
            tooltip=['Mês', alt.Tooltip('Valor Total Pedidos', format='$,.2f')]
        ).properties(title='Volume Mensal de Pedidos Bionio').interactive()
        st.altair_chart(bionio_chart, use_container_width=True)
    else:
        st.info("Nenhum dado de Bionio encontrado no Banco de Dados.")
    
    st.markdown("---")
    
    # ROVEMA PAY
    st.subheader("ROVEMA PAY: Receita e Taxa Média por Status")
    if not rovemapay_df_db.empty:
        col_r1, col_r2 = st.columns(2)
        
        # Gráfico de Receita por Status (Pago vs. Antecipado)
        rovema_revenue_chart = alt.Chart(rovemapay_df_db).mark_bar().encode(
            x=alt.X('Mês:O', title='Mês'),
            y=alt.Y('Receita', title='Receita (R$)'),
            color='Status',
            tooltip=['Mês', 'Status', alt.Tooltip('Receita', format='$,.2f')]
        ).properties(title='Receita Total por Mês e Status de Pagamento').interactive()
        col_r1.altair_chart(rovema_revenue_chart, use_container_width=True)
        
        # Indicador de Taxa Média
        avg_taxa = rovemapay_df_db['Taxa_Media'].mean()
        col_r2.metric("Rovema Pay: Taxa Média Geral", f"{avg_taxa:.2f}%", help="Média do Custo Total da Transação sobre o Bruto")
        col_r2.dataframe(rovemapay_df_db[['Mês', 'Status', 'Taxa_Media']].sort_values('Mês', ascending=False), use_container_width=True)
    else:
        st.info("Nenhum dado de Rovema Pay encontrado no Banco de Dados.")


# Garante que a função da página é chamada
if st.session_state.get('authenticated'):
    dashboard_page()
else:
    pass
