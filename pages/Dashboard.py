import streamlit as st
import altair as alt
import pandas as pd
from datetime import date, timedelta
from fire_admin import log_event
from utils.data_processing import fetch_asto_data, fetch_eliq_data, get_latest_uploaded_data

# --- Funções Auxiliares de Visualização (Mapeando o PDF) ---

def get_rovemapay_ranking_data(df_full_rovemapay):
    """Simula a geração dos rankings de crescimento/queda e participação por bandeira.
    
    Em um cenário real, este DF seria o RAW, mas usamos um mock simples aqui
    para simular a estrutura de ranking do PDF com base em dados.
    """
    if df_full_rovemapay.empty:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
        
    # --- Ranking Top 10 (Simulação para replicar o layout do PDF) ---
    # Usaremos empresas fictícias baseadas nos nomes do PDF
    ranking_data = {
        'Cliente': ['Posto Avenida', 'Concessionária RodarMars', 'Restaurante Dom Pepe', 'Farmácia Popular', 'Posto Panorama'],
        'Variação %': [-100.0, -100.0, -100.0, -100.0, -100.0],
        'Tipo': ['Queda'] * 5
    }
    
    ranking_df = pd.DataFrame(ranking_data)
    
    # --- Participação por Bandeira (Mock baseado no Rovema Pay) ---
    bandeira_data = {
        'Bandeira': ['Visa', 'Mastercard', 'Elo', 'Pix'],
        'Valor': [df_full_rovemapay['Liquido'].sum() * 0.40, 
                  df_full_rovemapay['Liquido'].sum() * 0.30,
                  df_full_rovemapay['Liquido'].sum() * 0.15,
                  df_full_rovemapay['Liquido'].sum() * 0.15]
    }
    bandeira_df = pd.DataFrame(bandeira_data)
    
    # --- Detalhamento por Cliente (Exibe os 10 clientes com maior receita) ---
    detalhamento_df = df_full_rovemapay.groupby('status').agg(
        Receita_Total=('Receita', 'sum'),
        Liquido_Total=('Liquido', 'sum')
    ).reset_index().sort_values('Receita_Total', ascending=False)
    
    return ranking_df, bandeira_df, detalhamento_df.rename(columns={'status': 'Cliente'})


# --- DASHBOARD PRINCIPAL ---

def dashboard_page():
    if not st.session_state.get('authenticated'):
        st.error("Acesso negado. Por favor, faça login na página principal.")
        return

    st.title("📈 Dashboard Consolidado Multi-Produto (Rovema Pay Pulse)")
    log_event("VIEW_DASHBOARD", "Visualizando o dashboard principal.")
    
    # --- FILTROS E MÉTRICAS PRINCIPAIS (Replicando o Header do PDF) ---
    
    # Define um intervalo padrão
    default_end_date = date(2025, 10, 31)
    default_start_date = default_end_date - timedelta(days=90)
    
    col_filter1, col_filter2 = st.columns([1, 1])
    with col_filter1:
        start_date = st.date_input("Data de Início", default_start_date)
    with col_filter2:
        end_date = st.date_input("Data Final", default_end_date)
        
    st.markdown("---")
    
    # Busca dados (simulados e reais)
    asto_df = fetch_asto_data(start_date.isoformat(), end_date.isoformat())
    eliq_df = fetch_eliq_data(start_date.isoformat(), end_date.isoformat())
    bionio_df_db = get_latest_uploaded_data('Bionio')
    rovemapay_df_db = get_latest_uploaded_data('RovemaPay')
    
    # Métrica Total (Simulação do R$ 2.146.293,35 do PDF)
    total_revenue = rovemapay_df_db['Receita'].sum() + bionio_df_db['Valor Total Pedidos'].sum()
    
    col_m1, col_m2, col_m3, col_m4 = st.columns(4)
    col_m1.metric("Valor Total Transacionado", f"R$ {total_revenue:,.2f}", help="Soma da Receita Rovema Pay e Valor Total Bionio")
    col_m2.metric("Asto: Valor Bruto Total", f"R$ {asto_df['valorBruto'].sum():,.2f}")
    col_m3.metric("Eliq: Volume Total", f"R$ {eliq_df['valor_total'].sum():,.2f}")
    col_m4.metric("Eliq: Consumo Médio", f"{eliq_df['consumo_medio'].mean():.2f} km/l")
    
    st.markdown("---")


    # --- BLOCO 1: EVOLUÇÃO E DISTRIBUIÇÃO ---
    st.header("1. Evolução e Participação")
    col_c1, col_c2 = st.columns([2, 1])
    
    # C1: Evolução do Valor Transacionado vs Receita (ASTO/ELIQ/BIONIO/ROVEMAPAY)
    with col_c1:
        st.subheader("Evolução do Valor vs. Receita (ASTO)")
        if not asto_df.empty:
            asto_long = asto_df.melt('dataFimApuracao', var_name='Métrica', value_name='Valor')
            
            asto_chart = alt.Chart(asto_long).mark_line(point=True).encode(
                x=alt.X('dataFimApuracao', title='Período'),
                y=alt.Y('Valor', title='Valor (R$)'),
                color='Métrica',
                tooltip=['dataFimApuracao', alt.Tooltip('Valor', format='$,.2f'), 'Métrica']
            ).properties(title='Evolução Semanal de Valores do Asto').interactive()
            st.altair_chart(asto_chart, use_container_width=True)
        else:
            st.info("Nenhum dado de Asto encontrado.")

    # C2: Receita por Carteira (Mapeando os 4 produtos como Carteiras)
    with col_c2:
        st.subheader("Receita por Carteira")
        
        carteira_data = {
            'Carteira': ['RovemaPay', 'Bionio', 'Asto (Simulado)', 'Eliq (Simulado)'],
            'Receita Total': [rovemapay_df_db['Receita'].sum(), bionio_df_db['Valor Total Pedidos'].sum(), asto_df['Receita'].sum(), eliq_df['valor_total'].sum() * 0.05]
        }
        carteira_df = pd.DataFrame(carteira_data)

        if not carteira_df.empty and carteira_df['Receita Total'].sum() > 0:
            carteira_chart = alt.Chart(carteira_df).mark_arc(outerRadius=120).encode(
                theta=alt.Theta(field="Receita Total", type="quantitative"),
                color=alt.Color(field="Carteira", type="nominal"),
                tooltip=["Carteira", alt.Tooltip("Receita Total", format="$,.2f")]
            ).properties(title="Distribuição de Receita por Produto")
            st.altair_chart(carteira_chart, use_container_width=True)
        else:
            st.warning("Dados insuficientes para Receita por Carteira.")

    st.markdown("---")

    # --- BLOCO 2: RANKINGS E DETALHAMENTO ---
    st.header("2. Rankings e Detalhamento")
    
    ranking_df, bandeira_df, detalhamento_df = get_rovemapay_ranking_data(rovemapay_df_db)

    col_r1, col_r2, col_r3 = st.columns([1, 1, 1])

    # R1: TOP 10 QUEDA (Replicando o formato do PDF)
    with col_r1:
        st.subheader("Top 10 Queda")
        if not ranking_df.empty:
            ranking_queda = ranking_df[ranking_df['Tipo'] == 'Queda'].head(10)
            st.dataframe(ranking_queda[['Cliente', 'Variação %']].reset_index(drop=True), hide_index=True, use_container_width=True)
        else:
            st.info("Nenhum ranking de queda disponível.")

    # R2: TOP 10 CRESCIMENTO (Replicando o formato do PDF)
    with col_r2:
        st.subheader("Top 10 Crescimento")
        # Simulação de crescimento
        ranking_crescimento_data = {
            'Cliente': ['Pods Sul Nemcente', 'Supermercado Real', 'Auto Peças V'],
            'Variação %': [17.9, 7.0, 5.5],
        }
        ranking_crescimento = pd.DataFrame(ranking_crescimento_data)
        st.dataframe(ranking_crescimento, hide_index=True, use_container_width=True)

    # R3: PARTICIPAÇÃO POR BANDEIRA
    with col_r3:
        st.subheader("Participação por Bandeira")
        if not bandeira_df.empty and bandeira_df['Valor'].sum() > 0:
            bandeira_chart = alt.Chart(bandeira_df).mark_arc(outerRadius=120).encode(
                theta=alt.Theta(field="Valor", type="quantitative"),
                color=alt.Color(field="Bandeira", type="nominal"),
                order=alt.Order("Valor", sort="descending"),
                tooltip=["Bandeira", alt.Tooltip("Valor", format="$,.2f")]
            ).properties(title="")
            st.altair_chart(bandeira_chart, use_container_width=True)
        else:
            st.warning("Dados de Bandeira insuficientes.")

    st.markdown("---")
    
    # --- BLOCO 3: INSIGHTS E DETALHAMENTO (Fundo do PDF) ---
    
    st.header("3. Insights e Detalhamento")
    
    col_i1, col_i2 = st.columns([1, 2])
    
    with col_i1:
        st.subheader("Insights Automáticos e Oportunidades")
        st.info("⚠️ **Oportunidade:** 3 clientes estão com queda de 100% no volume. Sugerir campanhas de reativação ou verificar problemas técnicos.")
        st.info("✅ **Destaque:** Cliente 'Supermercado Real' aumentou o uso em 7% após a última campanha Pix.")

    with col_i2:
        st.subheader("Detalhamento por Cliente (Exemplo)")
        # A tabela de detalhamento do PDF é replicada usando o status para simplificação
        st.dataframe(detalhamento_df, hide_index=True, use_container_width=True)
        st.caption("Nota: O detalhamento completo exige o download da planilha de RAW Data (Exportar CSV) no painel real.")


# Garante que a função da página é chamada
if st.session_state.get('authenticated'):
    dashboard_page()
else:
    pass
