import streamlit as st
import altair as alt
import pandas as pd
from datetime import date, timedelta
from fire_admin import log_event
from utils.data_processing import fetch_asto_data, fetch_eliq_data, get_latest_uploaded_data

# --- Funções Auxiliares de Visualização (Mapeando o PDF) ---

def get_dashboard_metrics(rovemapay_df, bionio_df, asto_df, eliq_df):
    """Calcula as métricas principais para o header."""
    
    # Receita: Soma da receita Rovema Pay + Valor Bruto Bionio + Receita Asto
    rovema_revenue = rovemapay_df['Receita'].sum() if not rovemapay_df.empty else 0
    bionio_value = bionio_df['Valor Total Pedidos'].sum() if not bionio_df.empty else 0
    asto_revenue = asto_df['Receita'].sum() if not asto_df.empty else 0
    
    total_revenue_sim = rovema_revenue + bionio_value + asto_revenue # Usamos uma métrica simulada consistente
    
    # Margem Média: Custo Total Percentual
    margem_media = rovemapay_df['Taxa_Media'].mean() if not rovemapay_df.empty else 0.0
    
    # O valor transacionado total (R$ 2.146.293,35) e a variação são simulados
    valor_transacionado_sim = 2_146_293.35 
    
    return valor_transacionado_sim, rovema_revenue, margem_media

def get_ranking_data(rovemapay_df):
    """Simula a geração dos rankings de crescimento/queda e participação por bandeira."""
        
    # --- Top 10 Queda (Hardcoded para replicar o layout do PDF) ---
    ranking_queda_data = {
        'Cliente': ['Posto Avenida', 'Concessionária RodarMais', 'Restaurante Dom Pepe', 'Loja Universo Tech', 'Farmácia Popular', 'Posto Panorama', 'Oficina Auto Luz', 'Loja Bella Casa', 'Auto Mecânica Pereira', 'Livraria Estilo'],
        'CNPJ': ['85.789.123/0001-45', '18.456.789/0001-75', '86.456.789/0001-55', '87.987.654/0001-65', '19.567.890/0001-85', '20.678.901/0001-95', '88.234.567/0001-75', '21.789.012/0001-05', '89.567.890/0001-85', '90.678.901/0001-95'],
        'Variação': [-100.0] * 10
    }
    ranking_queda_df = pd.DataFrame(ranking_queda_data)
    
    # --- Detalhamento por Cliente (Hardcoded para replicar o layout do PDF) ---
    detalhamento_data = {
        'CNPJ': ['94.012.345/0001-35', '95.123.456/0001-45', '12.345.678/0001-10', '45.123.678/0001-80', '96.234.567/0001-55', '56.789.123/0001-30', '23.456.789/0001-20', '97.345.678/0001-65', '31.234.567/0001-50', '98.456.789/0001-75'],
        'Cliente': ['Posto Sol Nascente', 'Supermercado Real', 'Auto Peças Silva', 'Concessionária Fenix', 'Papelaria Central', 'Padaria Doce Sabor', 'Supermercado Oliveira', 'Auto Mecânica Lima', 'Posto Vitória', 'Oficina do Tonho'],
        'Receita': [0.0] * 10,
        'Crescimento': [10.4, 21.7, 7.9, -6.6, 17.9, 28.1, 22.7, 18.2, 29.0, 23.8],
        'Nº Vendas': [1] * 10,
        'Bandeira': ['Pix', 'Crédito', 'Crédito', 'Crédito', 'Débito', 'Débito', 'Débito', 'Crédito', 'Crédito', 'Pix']
    }
    detalhamento_df = pd.DataFrame(detalhamento_data)
    
    # --- Participação por Bandeira (Mapeando do Detalhamento) ---
    bandeira_df = detalhamento_df.groupby('Bandeira')['Nº Vendas'].sum().reset_index()
    bandeira_df = bandeira_df.rename(columns={'Nº Vendas': 'Valor'})

    return ranking_queda_df, detalhamento_df, bandeira_df


# --- DASHBOARD PRINCIPAL ---

def dashboard_page():
    if not st.session_state.get('authenticated'):
        st.error("Acesso negado. Por favor, faça login na página principal.")
        return

    st.title("ROVEMA BANK: Dashboard de Transações")
    log_event("VIEW_DASHBOARD", "Visualizando o dashboard principal.")
    
    # --- 1. FILTROS E MÉTRICAS DO HEADER ---
    
    default_end_date = date(2025, 10, 31)
    default_start_date = default_end_date - timedelta(days=90)
    
    col_filter1, col_filter2, col_filter3 = st.columns([1, 1, 1])
    
    with col_filter1:
        st.date_input("Data Início", default_start_date)
    with col_filter2:
        st.date_input("Data Fim", default_end_date)
    with col_filter3:
        st.selectbox("Carteira", ["Todas"], disabled=True)
    
    # Busca dados (simulados e reais)
    asto_df = fetch_asto_data(default_start_date.isoformat(), default_end_date.isoformat())
    eliq_df = fetch_eliq_data(default_start_date.isoformat(), default_end_date.isoformat())
    bionio_df_db = get_latest_uploaded_data('Bionio')
    rovemapay_df_db = get_latest_uploaded_data('RovemaPay')
    
    # Calcula métricas
    valor_transacionado_sim, nossa_receita, margem_media = get_dashboard_metrics(rovemapay_df_db, bionio_df_db, asto_df, eliq_df)
    
    st.markdown("---")
    
    col_m1, col_m2, col_m3, col_m4, col_m5, col_m6 = st.columns(6)
    
    col_m1.metric("Transacionado (Bruto)", f"R$ {valor_transacionado_sim:,.2f}", delta="+142.49% vs. trimestre anterior")
    col_m2.metric("Nossa Receita", f"R$ {nossa_receita:,.2f}")
    col_m3.metric("Margem Média", f"{margem_media:.2f}%")
    col_m4.metric("Clientes Ativos", "99")
    col_m5.metric("Clientes em Queda", "16")
    
    st.markdown("---")


    # --- BLOCO 2: EVOLUÇÃO E PARTICIPAÇÃO (Gráficos) ---
    
    st.header("Evolução do Valor Transacionado vs Receita")
    
    # Gráfico de Evolução (Usando o Rovema Pay como base principal)
    if not rovemapay_df_db.empty:
        # Cria um DataFrame Longo para o gráfico de linha (Receita vs Liquido)
        rovema_long = rovemapay_df_db.melt('Mês', value_vars=['Receita', 'Liquido'], var_name='Métrica', value_name='Valor')
        
        evolucao_chart = alt.Chart(rovema_long).mark_line(point=True).encode(
            x=alt.X('Mês:O', title=''),
            y=alt.Y('Valor', title='Valor (R$)'),
            color='Métrica',
            tooltip=['Mês', alt.Tooltip('Valor', format='$,.2f'), 'Métrica']
        ).properties(title='Evolução da Receita e Volume Rovema Pay').interactive()
        
        st.altair_chart(evolucao_chart, use_container_width=True)
    else:
        st.info("Dados de Rovema Pay insuficientes para o gráfico de evolução.")

    col_g1, col_g2 = st.columns(2)

    # G1: Participação por Bandeira (Baseado no Mock/Detalhamento do PDF)
    ranking_queda_df, detalhamento_df, bandeira_df = get_ranking_data(rovemapay_df_db)
    
    with col_g1:
        st.subheader("Participação por Bandeira")
        if not bandeira_df.empty:
            bandeira_chart = alt.Chart(bandeira_df).mark_arc(outerRadius=120).encode(
                theta=alt.Theta(field="Valor", type="quantitative"),
                color=alt.Color(field="Bandeira", type="nominal"),
                order=alt.Order("Valor", sort="descending"),
                tooltip=["Bandeira", "Valor"]
            ).properties(title="")
            st.altair_chart(bandeira_chart, use_container_width=True)
        else:
            st.warning("Dados de Bandeira insuficientes.")

    # G2: Receita por Carteira (Mapeando os 4 produtos)
    with col_g2:
        st.subheader("Receita por Carteira")
        
        carteira_data = {
            'Carteira': ['RovemaPay', 'Bionio', 'Asto (Simulado)', 'Eliq (Simulado)'],
            'Receita Total': [rovemapay_df_db['Receita'].sum() if not rovemapay_df_db.empty else 0, 
                              bionio_df_db['Valor Total Pedidos'].sum() if not bionio_df_db.empty else 0, 
                              asto_df['Receita'].sum(), 
                              eliq_df['valor_total'].sum() * 0.05]
        }
        carteira_df = pd.DataFrame(carteira_data)
        
        if not carteira_df.empty and carteira_df['Receita Total'].sum() > 0:
            carteira_chart = alt.Chart(carteira_df).mark_bar().encode(
                x=alt.X("Carteira:N", title=""),
                y=alt.Y("Receita Total", title="Receita (R$)"),
                tooltip=["Carteira", alt.Tooltip("Receita Total", format="$,.2f")]
            ).properties(title="").interactive()
            st.altair_chart(carteira_chart, use_container_width=True)
        else:
            st.warning("Dados insuficientes para Receita por Carteira.")


    st.markdown("---")

    # --- BLOCO 3: RANKINGS E DETALHAMENTO ---
    
    col_r1, col_r2 = st.columns(2)

    # R1: TOP 10 QUEDA (Replicando o formato do PDF)
    with col_r1:
        st.subheader("Top 10 Queda")
        st.dataframe(ranking_queda_df[['Cliente', 'CNPJ', 'Variação']].rename(columns={'Variação': 'Variação %'}), hide_index=True, use_container_width=True)

    # R2: TOP 10 CRESCIMENTO (Replicando o formato do PDF)
    with col_r2:
        st.subheader("Top 10 Crescimento")
        # Criamos um ranking de crescimento simulado para preencher o espaço
        ranking_crescimento_data = {
            'Cliente': ['Posto Sol Nascente', 'Supermercado Real', 'Auto Peças Silva', 'Concessionária Fenix'],
            'Variação %': [10.4, 21.7, 7.9, -6.6]
        }
        ranking_crescimento = pd.DataFrame(ranking_crescimento_data)
        st.dataframe(ranking_crescimento, hide_index=True, use_container_width=True)

    st.markdown("---")
    
    # --- BLOCO 4: DETALHAMENTO E INSIGHTS ---
    
    st.header("Detalhamento por Cliente")
    
    col_d1, col_d2 = st.columns([2, 1])
    
    # D1: Tabela de Detalhamento
    with col_d1:
        st.subheader("Clientes e Crescimento")
        st.dataframe(detalhamento_df.rename(columns={'Crescimento': 'Crescimento %'}), hide_index=True, use_container_width=True)
        st.markdown("Mostrando 1 a 10 de 99 clientes | [Anterior] [Próxima]")
        st.button("Exportar CSV")

    # D2: Insights
    with col_d2:
        st.subheader("Insights Automáticos")
        st.success("✅ Destaque do Trimestre: Posto Sol Nascente cresceu 34% com forte aumento em transações Pix.")
        st.info("💡 Oportunidade: 5 clientes estão próximos de atingir novo patamar de faturamento. Considere campanhas de incentivo.")
        st.warning("⚠️ Atenção Necessária: Bar do João apresenta queda de 18%. Recomenda-se contato da equipe comercial.")
        st.markdown("---")
        st.caption("Powered by KR8")


# Garante que a função da página é chamada
if st.session_state.get('authenticated'):
    dashboard_page()
else:
    pass
