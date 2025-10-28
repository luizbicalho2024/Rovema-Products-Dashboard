import streamlit as st
import altair as alt
import pandas as pd
from datetime import date, timedelta
from fire_admin import log_event, get_all_consultores
from utils.data_processing import get_latest_aggregated_data, get_raw_data_from_firestore

# --- FUNÇÕES DE BUSCA DE DADOS (Refatoradas para performance) ---

@st.cache_data(ttl=60, show_spinner="Carregando dados agregados...")
def load_aggregated_data(start_date, end_date, update_counter):
    """Carrega todos os dados AGREGADOS para os gráficos principais."""
    return (
        get_latest_aggregated_data('Asto', start_date, end_date),
        get_latest_aggregated_data('Eliq', start_date, end_date),
        get_latest_aggregated_data('Bionio', start_date, end_date),
        get_latest_aggregated_data('Rovema Pay', start_date, end_date)
    )

@st.cache_data(ttl=60, show_spinner="Calculando métricas de clientes...")
def load_raw_data_for_kpis(start_date, end_date, update_counter):
    """Carrega dados RAW (não agregados) para KPIs dinâmicos como 'Clientes Ativos'."""
    # Por enquanto, focamos nos clientes do Rovema Pay
    df_rovemapay_raw = get_raw_data_from_firestore('Rovema Pay', start_date, end_date)
    
    # Adicione outros produtos se necessário, ex:
    # df_bionio_raw = get_raw_data_from_firestore('Bionio', start_date, end_date)
    
    return df_rovemapay_raw

@st.cache_data(ttl=3600)
def load_consultores_list():
    """Carrega a lista de consultores do Firestore."""
    return get_all_consultores()


# --- DASHBOARD PRINCIPAL ---

def dashboard_page():
    if not st.session_state.get('authenticated'):
        st.error("Acesso negado. Por favor, faça login na página principal.")
        return

    st.title("ROVEMA BANK: Dashboard de Transações")
    st.caption("Filtros Ativos na Barra Lateral")
    log_event("VIEW_DASHBOARD", "Visualizando o dashboard principal.")
    
    # --- 0. FILTROS NA BARRA LATERAL (Sidebar) ---
    
    if 'update_counter' not in st.session_state:
        st.session_state['update_counter'] = 0

    st.sidebar.title("Filtros")
    
    # Filtros de Data
    default_end_date = date(2025, 10, 31)
    default_start_date = default_end_date - timedelta(days=90)
    
    start_date = st.sidebar.date_input("Data início", default_start_date)
    end_date = st.sidebar.date_input("Data fim", default_end_date)
    
    st.sidebar.markdown("---")

    # Filtros de Contexto
    selected_products = st.sidebar.multiselect(
        "Produtos", 
        ["Eliq", "Asto", "Bionio", "Rovema Pay"],
        default=["Eliq", "Asto", "Bionio", "Rovema Pay"],
        help="Filtra dados pelos produtos da empresa."
    )

    selected_payments = st.sidebar.multiselect(
        "Meios de Pagamento", 
        ["Crédito", "Débito", "Pix"],
        default=["Crédito", "Débito", "Pix"],
        help="Filtra transações pelo tipo de meio de pagamento."
    )

    # Filtro de Consultores (DINÂMICO)
    consultores_list = load_consultores_list()
    st.sidebar.selectbox(
        "Consultores", 
        consultores_list,
        help="Filtra dados pela carteira de consultores (requer dados de consultor nos uploads)."
    )
    
    st.sidebar.markdown("---")
    
    if st.sidebar.button("Atualizar"):
        st.session_state['update_counter'] += 1
        st.toast("Dashboard atualizado!")
        st.rerun() 
    
    
    # --- 1. CARREGAMENTO E CÁLCULO DE DADOS ---
    
    # Carrega dados agregados para gráficos
    asto_df_agg, eliq_df_agg, bionio_df_agg, rovemapay_df_agg = load_aggregated_data(
        start_date, end_date, st.session_state['update_counter']
    )
    
    # Carrega dados raw para KPIs
    rovemapay_df_raw = load_raw_data_for_kpis(
        start_date, end_date, st.session_state['update_counter']
    )

    # --- Cálculo Condicional das Métricas ---
    current_rovema_revenue = 0
    current_bionio_value = 0
    current_asto_revenue = 0
    current_eliq_volume = 0
    current_margem_media = 0
    current_valor_transacionado = 0
    total_clientes_ativos = 0
    
    # 1. Rovema Pay (Liquido/Receita/Margem)
    if "Rovema Pay" in selected_products:
        if not rovemapay_df_agg.empty:
            current_rovema_revenue = rovemapay_df_agg['Receita'].sum()
            current_margem_media = rovemapay_df_agg['Taxa_Media'].mean()
            current_valor_transacionado += rovemapay_df_agg['Liquido'].sum()
        if not rovemapay_df_raw.empty:
            # KPI Dinâmico: Clientes Ativos
            # (Assumindo que a coluna 'cliente' ou 'cnpj' existe nos dados raw)
            if 'cnpj' in rovemapay_df_raw.columns:
                total_clientes_ativos += rovemapay_df_raw['cnpj'].nunique()
            elif 'cliente' in rovemapay_df_raw.columns:
                 total_clientes_ativos += rovemapay_df_raw['cliente'].nunique()


    # 2. Bionio (Valor Total Pedidos)
    if "Bionio" in selected_products and not bionio_df_agg.empty:
        current_bionio_value = bionio_df_agg['Valor Total Pedidos'].sum()
        current_valor_transacionado += current_bionio_value

    # 3. Asto (Receita/Volume)
    if "Asto" in selected_products and not asto_df_agg.empty:
        current_asto_revenue = asto_df_agg['Receita'].sum()
        current_valor_transacionado += asto_df_agg['valorBruto'].sum()
        
    # 4. Eliq (Volume)
    if "Eliq" in selected_products and not eliq_df_agg.empty:
        current_eliq_volume = eliq_df_agg['valor_total'].sum()
        current_valor_transacionado += eliq_df_agg['valor_total'].sum()
        
    
    # Métrica do Header
    nossa_receita = current_rovema_revenue + current_asto_revenue
    
    # --- 2. EXIBIÇÃO DAS MÉTRICAS (KPIs) ---
    
    col_m1, col_m2, col_m3, col_m4 = st.columns(4)
    
    col_m1.metric("Transacionado (Bruto)", f"R$ {current_valor_transacionado:,.2f}")
    col_m2.metric("Nossa Receita", f"R$ {nossa_receita:,.2f}")
    col_m3.metric("Margem Média (RovemaPay)", f"{current_margem_media:.2f}%")
    col_m4.metric("Clientes Ativos (RovemaPay)", f"{total_clientes_ativos}")
    
    st.markdown("---")

    # --- 3. ABAS DE VISUALIZAÇÃO (MELHORIA DE UX) ---
    
    tab_evolucao, tab_rankings, tab_insights = st.tabs([
        "📊 Evolução e Participação", 
        "🏆 Rankings de Clientes", 
        "💡 Detalhamento e Insights"
    ])

    # --- ABA 1: Evolução e Participação ---
    with tab_evolucao:
        col_g1, col_g2 = st.columns([2, 1])
        
        # G1: Evolução do Valor Transacionado vs Receita
        with col_g1:
            st.header("Evolução (Receita e Volume)")
            
            if ("Rovema Pay" in selected_products and not rovemapay_df_agg.empty) or \
               ("Asto" in selected_products and not asto_df_agg.empty):
                
                # 1. ROVEMA PAY
                rovema_long = rovemapay_df_agg.melt('Mês', value_vars=['Receita', 'Liquido'], var_name='Métrica', value_name='Valor')
                
                # 2. ASTO
                asto_long = asto_df_agg.melt('Mês', value_vars=['Receita', 'valorBruto'], var_name='Métrica', value_name='Valor')
                asto_long['Métrica'] = asto_long['Métrica'].replace({'valorBruto': 'Volume'})

                final_evolucao_df = pd.concat([rovema_long, asto_long]).groupby(['Mês', 'Métrica'])['Valor'].sum().reset_index()
                
                evolucao_chart = alt.Chart(final_evolucao_df).mark_line(point=True).encode(
                    x=alt.X('Mês:O', title=''),
                    y=alt.Y('Valor', title='Valor (R$)'),
                    color='Métrica',
                    tooltip=['Mês', alt.Tooltip('Valor', format='$,.2f'), 'Métrica']
                ).properties(title='Evolução da Receita e Volume (Asto + RovemaPay)').interactive()
                
                st.altair_chart(evolucao_chart, use_container_width=True)
            else:
                st.info("Selecione 'Rovema Pay' e/ou 'Asto' para ver o gráfico de evolução.")

        # G2: Participação por Bandeira e Receita por Carteira
        with col_g2:
            st.subheader("Receita por Carteira")
            
            carteira_data = {
                'Carteira': [],
                'Receita/Volume': []
            }
            
            if "Rovema Pay" in selected_products:
                carteira_data['Carteira'].append('RovemaPay')
                carteira_data['Receita/Volume'].append(current_rovema_revenue)
            if "Bionio" in selected_products:
                carteira_data['Carteira'].append('Bionio (Volume)')
                carteira_data['Receita/Volume'].append(current_bionio_value)
            if "Asto" in selected_products:
                carteira_data['Carteira'].append('Asto')
                carteira_data['Receita/Volume'].append(current_asto_revenue)
            if "Eliq" in selected_products:
                carteira_data['Carteira'].append('Eliq (Volume)')
                carteira_data['Receita/Volume'].append(current_eliq_volume) 
            
            carteira_df = pd.DataFrame(carteira_data)

            if not carteira_df.empty and carteira_df['Receita/Volume'].sum() > 0:
                carteira_chart = alt.Chart(carteira_df).mark_bar().encode(
                    x=alt.X("Carteira:N", title=""),
                    y=alt.Y("Receita/Volume", title="Valor (R$)"),
                    color=alt.Color("Carteira:N"), 
                    tooltip=["Carteira", alt.Tooltip("Receita/Volume", format="$,.2f")]
                ).properties(title="").interactive()
                st.altair_chart(carteira_chart, use_container_width=True)
            else:
                st.warning("Nenhum produto selecionado para Receita por Carteira.")


    # --- ABA 2: Rankings (DINÂMICOS) ---
    with tab_rankings:
        st.header("Rankings Dinâmicos (Rovema Pay)")
        
        if "Rovema Pay" not in selected_products:
            st.info("Selecione 'Rovema Pay' nos filtros para ver os rankings de clientes.")
        elif rovemapay_df_raw.empty:
            st.warning("Nenhum dado bruto encontrado para 'Rovema Pay' no período selecionado.")
        else:
            col_r1, col_r2 = st.columns(2)
            
            # Coluna de agrupamento (idealmente CNPJ, fallback para Cliente)
            group_col = 'cnpj' if 'cnpj' in rovemapay_df_raw.columns else 'cliente'
            
            if group_col not in rovemapay_df_raw.columns:
                st.error("Dados de RovemaPay não contêm coluna 'cliente' ou 'cnpj' para ranking.")
            else:
                # R1: TOP 10 POR RECEITA
                with col_r1:
                    st.subheader("Top 10 Clientes por Receita")
                    df_top_receita = rovemapay_df_raw.groupby(group_col)['receita'].sum().nlargest(10).reset_index()
                    df_top_receita.columns = ['Cliente/CNPJ', 'Receita Total']
                    st.dataframe(df_top_receita, 
                                 column_config={"Receita Total": st.column_config.NumberColumn(format="R$ %.2f")},
                                 hide_index=True, use_container_width=True)

                # R2: TOP 10 POR VOLUME (LÍQUIDO)
                with col_r2:
                    st.subheader("Top 10 Clientes por Volume Líquido")
                    df_top_liquido = rovemapay_df_raw.groupby(group_col)['liquido'].sum().nlargest(10).reset_index()
                    df_top_liquido.columns = ['Cliente/CNPJ', 'Volume Líquido']
                    st.dataframe(df_top_liquido, 
                                 column_config={"Volume Líquido": st.column_config.NumberColumn(format="R$ %.2f")},
                                 hide_index=True, use_container_width=True)

    # --- ABA 3: Detalhamento e Insights ---
    with tab_insights:
        st.header("Detalhamento por Cliente")
        
        col_d1, col_d2 = st.columns([2, 1])
        
        with col_d1:
            st.subheader("Visão Geral dos Clientes (Rovema Pay)")
            if not rovemapay_df_raw.empty and ('cliente' in rovemapay_df_raw.columns or 'cnpj' in rovemapay_df_raw.columns):
                group_col = 'cnpj' if 'cnpj' in rovemapay_df_raw.columns else 'cliente'
                
                df_detalhe = rovemapay_df_raw.groupby(group_col).agg(
                    Receita_Total=('receita', 'sum'),
                    Volume_Liquido=('liquido', 'sum'),
                    Num_Vendas=('liquido', 'count'),
                    Taxa_Media=('custo_total_perc', 'mean')
                ).reset_index().sort_values(by='Receita_Total', ascending=False)
                
                st.dataframe(df_detalhe.head(20), hide_index=True, use_container_width=True)
                st.caption(f"Mostrando os 20 maiores clientes de {total_clientes_ativos} no período.")
                
                # Botão de Exportar
                @st.cache_data
                def convert_df_to_csv(df):
                    return df.to_csv(index=False, sep=';', decimal=',').encode('utf-8-sig')

                csv = convert_df_to_csv(df_detalhe)
                st.download_button(
                    label="Exportar Detalhamento (CSV)",
                    data=csv,
                    file_name="detalhamento_clientes.csv",
                    mime="text/csv",
                )
                
            else:
                st.info("Sem dados de Rovema Pay para detalhamento.")
                
        with col_d2:
            st.subheader("Insights (Em Desenvolvimento)")
            st.info("💡 A funcionalidade de insights automáticos (crescimento, queda, oportunidades) está em desenvolvimento.")
            st.warning("⚠️ O cálculo de 'crescimento' e 'queda' requer a comparação de dados com o período anterior, o que será implementado na próxima versão.")


# Garante que a função da página é chamada
if st.session_state.get('authenticated'):
    dashboard_page()
else:
    # Se não estiver autenticado, o streamlit_app.py já cuida da página de login
    pass
