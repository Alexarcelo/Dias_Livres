import streamlit as st
import pandas as pd
import mysql.connector
import decimal
from datetime import timedelta

def gerar_df_phoenix(vw_name, base_luck):

    config = {
    'user': 'user_automation_jpa',
    'password': 'luck_jpa_2024',
    'host': 'comeia.cixat7j68g0n.us-east-1.rds.amazonaws.com',
    'database': base_luck
    }
    conexao = mysql.connector.connect(**config)
    cursor = conexao.cursor()

    request_name = f'SELECT * FROM {vw_name}'
        
    cursor.execute(request_name)
    resultado = cursor.fetchall()
    cabecalho = [desc[0] for desc in cursor.description]
    cursor.close()
    conexao.close()
    df = pd.DataFrame(resultado, columns=cabecalho)
    df = df.applymap(lambda x: float(x) if isinstance(x, decimal.Decimal) else x)
    return df

def plotar_filtros_e_filtrar_escalas(row1):

    with row1[0]:

        container_filtros = st.container(border=True)

        container_filtros.subheader('Filtros')

        data_inicial = container_filtros.date_input(
            'Data Inicial', 
            value=None ,
            format='DD/MM/YYYY', 
            key='data_inicial'
        )

        data_final = container_filtros.date_input(
            'Data Final', 
            value=None ,
            format='DD/MM/YYYY', 
            key='data_final'
        )

        if data_inicial and data_final:

            df_escalas = st.session_state.df_escalas[
                (st.session_state.df_escalas['Data_da_Escala'] >= data_inicial) & 
                (st.session_state.df_escalas['Data_da_Escala'] <= data_final)
            ].reset_index(drop=True)

            tipo_de_servico = container_filtros.selectbox(
                'Tipo de Serviço', 
                [
                    'IN', 
                    'OUT', 
                    'TOUR', 
                    'TRANSFER'
                ], 
                index=None,
                key = 'tipo_de_servico'
            )

            if tipo_de_servico:

                df_escalas = df_escalas[
                    df_escalas['Tipo_de_Servico'] == tipo_de_servico
                ].reset_index(drop=True)

            servico_selecionado = container_filtros.multiselect(
                'Serviço', 
                sorted(df_escalas['Servico'].unique()), 
                default=None
            )

            if servico_selecionado:

                df_escalas = df_escalas[
                    df_escalas['Servico'].isin(servico_selecionado)
                ].reset_index(drop=True)

            guia_selecionado = container_filtros.multiselect(
                'Guia', 
                sorted(df_escalas['Guia'].unique()), 
                default=None
            )

            if guia_selecionado:

                df_escalas = df_escalas[
                    df_escalas['Guia'].isin(guia_selecionado)
                ].reset_index(drop=True)

            escala_selecionada = container_filtros.multiselect(
                'Escala', 
                sorted(df_escalas['Escala'].unique()), 
                default=None
            )

            if escala_selecionada:

                df_escalas = df_escalas[
                    df_escalas['Escala'].isin(escala_selecionada)
                ].reset_index(drop=True)

            return df_escalas
        
        else:

            return None

def tratar_df_sales():

    df_sales = st.session_state.df_sales.copy()

    df_sales['Data_Venda'] = pd.to_datetime(df_sales['Data_Venda']).dt.date

    return df_sales

def calcular_performance(df_escalas, df_sales):

    # Inserindo data de último serviço por reserva

    df_escalas = df_escalas.merge(
        st.session_state.df_ultimo_servico, 
        how='left', 
        on='Reserva_Mae'
    )

    # Calculando quantos dias o pax ainda fica no destino

    df_escalas['Estadia'] = (pd.to_datetime(df_escalas['Data_Ultimo_Servico']) - pd.to_datetime(df_escalas['Data_da_Escala'])).dt.days

    # Criando dataframe que preserve o index original

    df_escalas_ref = df_escalas.reset_index()

    # Pegando todas as possibilidades únicas de reserva mãe e data da escala

    df_data_reserva = df_escalas_ref[['index', 'Data_da_Escala', 'Reserva_Mae', 'Data_Ultimo_Servico']].drop_duplicates()

    # Excluindo situações em que o pax não tem período de estadia, ou seja, o último serviço é o próprio serviço da escala

    df_data_reserva = df_data_reserva[df_data_reserva['Data_da_Escala']!=df_data_reserva['Data_Ultimo_Servico']].reset_index(drop=True)

    # Fazendo inner join pra pegar todas as combinações de vendas vs reservas/data da escala

    df_merged = df_sales.merge(df_data_reserva, on='Reserva_Mae', how='inner')

    # Agora filtrando apenas as vendas feitas antes da data da escala e dos serviços que serão executados após a data da escala, mas antes da data do último serviço

    df_merged = df_merged[
        (df_merged['Data_Venda'] < df_merged['Data_da_Escala']) &
        (df_merged['Data_Execucao'] > df_merged['Data_da_Escala']) &
        (df_merged['Data_Execucao'] < df_merged['Data_Ultimo_Servico'])
    ]

    # Contando quantos serviços foram vendidos antes da escala a partir das diferentes datas de execução

    df_contagem = df_merged.groupby('index')['Data_Execucao'].nunique().reset_index()

    # Renomeando coluna e inserindo no dataframe principal

    df_contagem.rename(columns={'Data_Execucao': 'N_Serviços_Vendidos_Antes_da_Escala'}, inplace=True)

    df_escalas.loc[df_contagem['index'], 'N_Serviços_Vendidos_Antes_da_Escala'] = df_contagem['N_Serviços_Vendidos_Antes_da_Escala'].values

    df_escalas['N_Serviços_Vendidos_Antes_da_Escala'] = df_escalas['N_Serviços_Vendidos_Antes_da_Escala'].fillna(0).astype(int)

    # Dias Livres no dia da escala  = Estadia - N_Serviços_Vendidos_Antes_da_Escala - 1 (o -1 entra porque eu não conto o último dia de serviço como possibilidade).
    # Tem que fazer assim porque o último serviço pode não ter sido vendido como PDV

    df_escalas['Dias_Livres_Antes_da_Escala'] = df_escalas['Estadia'] - df_escalas['N_Serviços_Vendidos_Antes_da_Escala'] - 1

    df_escalas.loc[df_escalas['Dias_Livres_Antes_da_Escala'] < 0, 'Dias_Livres_Antes_da_Escala'] = 0

    # Agora seguindo a mesma lógica anterior para calcular as vendas do Guia em D+0 e D+1 pra cada reserva da escala

    # Pegando todas as possibilidades únicas de guia e reserva

    df_guia_reserva = df_escalas_ref[['index', 'Guia', 'Reserva_Mae', 'Data_Ultimo_Servico', 'Data_da_Escala']].drop_duplicates()

    # Excluindo situações em que o pax não tem período de estadia, ou seja, o último serviço é o próprio serviço da escala

    df_guia_reserva = df_guia_reserva[df_guia_reserva['Data_da_Escala']!=df_guia_reserva['Data_Ultimo_Servico']].reset_index(drop=True)

    # Alterando coluna Vendedor pra não gerar novas colunas no merge

    df_sales.rename(columns={'Vendedor': 'Guia'}, inplace=True)

    # Fazendo inner join pra pegar todas as combinações de vendas vs guias/reservas

    df_merged = df_sales.merge(df_guia_reserva, on=['Reserva_Mae', 'Guia'], how='inner')

    # Filtrando apenas as vendas feitas em D+0 e D+1 e que serão executadas após a data da escala, mas antes da data do último serviço

    if st.session_state.tipo_de_servico == 'IN' or st.session_state.base_luck == 'test_phoenix_joao_pessoa':

        df_merged = df_merged[
            (
                (df_merged['Data_Venda'] >= df_merged['Data_da_Escala'])
            ) &
            (df_merged['Data_Execucao'] > df_merged['Data_da_Escala']) &
            (df_merged['Data_Execucao'] <= df_merged['Data_Ultimo_Servico'])
        ]

    if st.session_state.base_luck == 'test_phoenix_natal':

        df_merged = df_merged[
            (
                (df_merged['Data_Venda'] == df_merged['Data_da_Escala']) |
                (df_merged['Data_Venda'] == df_merged['Data_da_Escala'] + timedelta(days=1))
            ) &
            (df_merged['Data_Execucao'] > df_merged['Data_da_Escala']) &
            (df_merged['Data_Execucao'] <= df_merged['Data_Ultimo_Servico'])
        ]

    # Contando quantos serviços foram vendidos em D+0 e D+1 a partir das diferentes datas de execução e retendo a maior data de execução pra poder calcular a performance

    df_contagem = df_merged.groupby('index').agg({'Data_Execucao': ['nunique', 'max']}).reset_index()

    df_contagem.columns = ['index', 'N_Serviços_Vendidos_D0_D1', 'Data_Ultimo_Servico_Vendido_D0_D1']

    df_escalas.loc[df_contagem['index'], 'N_Serviços_Vendidos_D0_D1'] = df_contagem['N_Serviços_Vendidos_D0_D1'].values

    df_escalas.loc[df_contagem['index'], 'Data_Ultimo_Servico_Vendido_D0_D1'] = df_contagem['Data_Ultimo_Servico_Vendido_D0_D1'].values

    df_escalas['N_Serviços_Vendidos_D0_D1'] = df_escalas['N_Serviços_Vendidos_D0_D1'].fillna(0).astype(int)

    # Quando o último serviço for um PDV, aí a performance é calculada em cima da estadia. Quando não for, é calculada em cima dos dias livres

    mask_perf_1 = (
        (pd.notna(df_escalas['Data_Ultimo_Servico_Vendido_D0_D1'])) & 
        (df_escalas['Data_Ultimo_Servico_Vendido_D0_D1'] < df_escalas['Data_Ultimo_Servico'])
    )

    df_escalas.loc[mask_perf_1, 'Dias_Livres_Antes_da_Escala_Correto'] = df_escalas.loc[mask_perf_1, 'Dias_Livres_Antes_da_Escala']

    mask_perf_2 = (
        (pd.notna(df_escalas['Data_Ultimo_Servico_Vendido_D0_D1'])) & 
        (df_escalas['Data_Ultimo_Servico_Vendido_D0_D1'] == df_escalas['Data_Ultimo_Servico'])
    )

    df_escalas.loc[mask_perf_2, 'Dias_Livres_Antes_da_Escala_Correto'] = df_escalas.loc[mask_perf_2, 'Estadia']

    df_escalas['Dias_Livres_Antes_da_Escala_Correto'] = df_escalas['Dias_Livres_Antes_da_Escala_Correto'].fillna(df_escalas['Dias_Livres_Antes_da_Escala'])

    mask_perf_3 = pd.notna(df_escalas['Data_Ultimo_Servico_Vendido_D0_D1'])

    df_escalas.loc[mask_perf_3, 'Performance'] = round(df_escalas.loc[mask_perf_3, 'N_Serviços_Vendidos_D0_D1'] / df_escalas.loc[mask_perf_3, 'Dias_Livres_Antes_da_Escala_Correto'], 2)

    df_escalas['Performance'] = df_escalas['Performance'].fillna(0)

    return df_escalas

def string_voos(x):

    if all(pd.isna(x)):

        return ''
    
    else:

        lista_voos = set(list(x))

        return ' + '.join(lista_voos)

def gerar_df_escalas_agrupadas(df_escalas):

    df_escalas_agrupadas = df_escalas.groupby(
        [
            'Data_da_Escala', 
            'Escala', 
            'Veiculo', 
            'Guia', 
            'Servico',
            'Tipo_de_Servico'
        ]
    ).agg(
        {
            'Reserva_Mae': 'nunique', 
            'Total_Paxs': 'sum',
            'Dias_Livres_Antes_da_Escala_Correto': 'sum', 
            'N_Serviços_Vendidos_D0_D1': 'sum',
            'Voo': string_voos
        }
    ).reset_index()

    df_escalas_agrupadas = df_escalas_agrupadas[df_escalas_agrupadas['Dias_Livres_Antes_da_Escala_Correto'] > 0]

    df_escalas_agrupadas['Performance'] = round(df_escalas_agrupadas['N_Serviços_Vendidos_D0_D1'] / df_escalas_agrupadas['Dias_Livres_Antes_da_Escala_Correto'], 2)

    df_escalas_agrupadas['Performance'] = df_escalas_agrupadas['Performance'].fillna(0)

    df_escalas_agrupadas.rename(
        columns = {
            'Servico': 'Serviço',
            'Veiculo': 'Veículo',
            'Reserva_Mae': 'Total de Reservas',
            'Total_Paxs': 'Total de Paxs',
            'Dias_Livres_Antes_da_Escala_Correto': 'Total de Dias Livres',
            'N_Serviços_Vendidos_D0_D1': 'Total de Dias Vendidos'
        },
        inplace = True
    )

    df_escalas_agrupadas['Performance'] = df_escalas_agrupadas['Performance'].apply(lambda x: f'{x*100:.1f}%')

    return df_escalas_agrupadas

def plotar_resumos(df_escalas_agrupadas, filtrar_resumos):

    def plotar_ranking_performance(df_escalas_agrupadas):

        df_ranking = df_escalas_agrupadas.groupby('Guia', as_index=False).agg(
            {
                'Total de Reservas': 'sum',
                'Total de Paxs': 'sum',
                'Total de Dias Livres': 'sum',
                'Total de Dias Vendidos': 'sum'
            }
        )

        df_ranking['Performance'] = round(df_ranking['Total de Dias Vendidos'] / df_ranking['Total de Dias Livres'], 2)

        df_ranking['Performance'] = df_ranking['Performance'].fillna(0)

        df_ranking = df_ranking.sort_values(by=['Performance', 'Total de Dias Livres'], ascending=False).reset_index(drop=True)

        df_ranking['Performance'] = df_ranking['Performance'].apply(lambda x: f'{x*100:.1f}%')

        df_ranking['Guia'] = df_ranking['Guia'].apply(lambda x: x.title())

        st.title(f'Ranking de Performance')

        st.divider()

        st.dataframe(
            df_ranking,
            hide_index=True,
            use_container_width=True
        )

        st.divider()

    def plotar_resumo_geral(df_escalas_agrupadas, guia):

        df_guia = df_escalas_agrupadas[df_escalas_agrupadas['Guia'] == guia].reset_index(drop=True)

        df_resumo = df_guia.groupby('Guia').agg(
            {
                'Total de Reservas': 'sum',
                'Total de Paxs': 'sum',
                'Total de Dias Livres': 'sum',
                'Total de Dias Vendidos': 'sum'
            }
        ).reset_index()

        df_resumo['Performance'] = round(df_resumo['Total de Dias Vendidos'] / df_resumo['Total de Dias Livres'], 2)

        df_resumo['Performance'] = df_resumo['Performance'].apply(lambda x: f'{x*100:.1f}%')

        st.header('Resumo Geral')

        st.dataframe(
            df_resumo[
                [
                    'Total de Reservas',
                    'Total de Paxs',
                    'Total de Dias Livres',
                    'Total de Dias Vendidos',
                    'Performance'
                ]
            ], 
            hide_index=True, 
            use_container_width=True
        )

        return df_guia
    
    def plotar_resumo_servico(df_guia):

        st.header('Resumo por Serviço')

        for tipo_servico in sorted(df_guia['Serviço'].unique()):

            st.subheader(f"{tipo_servico}")

            df_tipo_servico = df_guia[df_guia['Serviço'] == tipo_servico].groupby('Serviço').agg(
                {
                    'Total de Reservas': 'sum',
                    'Total de Paxs': 'sum',
                    'Total de Dias Livres': 'sum',
                    'Total de Dias Vendidos': 'sum'
                }
            ).reset_index()

            df_tipo_servico['Performance'] = round(df_tipo_servico['Total de Dias Vendidos'] / df_tipo_servico['Total de Dias Livres'], 2)

            df_tipo_servico['Performance'] = df_tipo_servico['Performance'].apply(lambda x: f'{x*100:.1f}%')

            st.dataframe(
                df_tipo_servico[
                    [
                        'Total de Reservas',
                        'Total de Paxs',
                        'Total de Dias Livres',
                        'Total de Dias Vendidos',
                        'Performance'
                    ]
                ], 
                hide_index=True, 
                use_container_width=True
            )

    def plotar_resumo_tipo_servico(df_guia):

        st.header('Resumo por Tipo de Serviço')

        for tipo_servico in sorted(df_guia['Tipo_de_Servico'].unique()):

            st.subheader(f"{tipo_servico}")

            df_tipo_servico = df_guia[df_guia['Tipo_de_Servico'] == tipo_servico].groupby('Tipo_de_Servico').agg(
                {
                    'Total de Reservas': 'sum',
                    'Total de Paxs': 'sum',
                    'Total de Dias Livres': 'sum',
                    'Total de Dias Vendidos': 'sum'
                }
            ).reset_index()

            df_tipo_servico['Performance'] = round(df_tipo_servico['Total de Dias Vendidos'] / df_tipo_servico['Total de Dias Livres'], 2)

            df_tipo_servico['Performance'] = df_tipo_servico['Performance'].apply(lambda x: f'{x*100:.1f}%')

            st.dataframe(
                df_tipo_servico[
                    [
                        'Total de Reservas',
                        'Total de Paxs',
                        'Total de Dias Livres',
                        'Total de Dias Vendidos',
                        'Performance'
                    ]
                ], 
                hide_index=True, 
                use_container_width=True
            )

    def plotar_resumo_dia(df_guia):

        st.header('Resumo por Dia')

        for data_escala in sorted(df_guia['Data_da_Escala'].unique()):

            df_data_escala = df_guia[df_guia['Data_da_Escala'] == data_escala].reset_index(drop=True)

            performance_do_dia = round(df_data_escala['Total de Dias Vendidos'].sum() / df_data_escala['Total de Dias Livres'].sum(), 2)

            st.subheader(f"{data_escala.strftime('%d/%m/%Y')} - {st.session_state.dias_da_semana_ingles_portugues[data_escala.strftime('%A')]} - {performance_do_dia*100:.1f}%")

            st.dataframe(
                df_data_escala[
                    [
                        'Escala', 
                        'Serviço',
                        'Voo',
                        'Veículo',
                        'Total de Reservas',
                        'Total de Paxs',
                        'Total de Dias Livres',
                        'Total de Dias Vendidos',
                        'Performance'
                    ]
                ], hide_index=True, use_container_width=True)

    plotar_ranking_performance(df_escalas_agrupadas)

    for guia in sorted(df_escalas_agrupadas['Guia'].unique()):

        row2 = st.columns(1)

        with row2[0]:

            st.title(f'{guia.title()}')

            st.divider()

        row3 = st.columns(2)

        with row3[0]:

            if not 'Resumo Geral' in filtrar_resumos:

                container_resumo_geral = st.container(border=True)

                with container_resumo_geral:

                    df_guia = plotar_resumo_geral(df_escalas_agrupadas, guia)

            if not 'Resumo por Serviço' in filtrar_resumos:

                container_resumo_servico = st.container(border=True)

                with container_resumo_servico:

                    plotar_resumo_servico(df_guia)

        with row3[1]:

            if not 'Resumo por Tipo de Serviço' in filtrar_resumos:

                container_resumo_tipo_servico = st.container(border=True)

                with container_resumo_tipo_servico:

                    plotar_resumo_tipo_servico(df_guia)

        if not 'Resumo por Dia' in filtrar_resumos:

            container_resumo_dia = st.container(border=True)

            with container_resumo_dia:

                plotar_resumo_dia(df_guia)

            st.divider()

st.set_page_config(layout='wide')

with st.spinner('Puxando dados do Phoenix...'):

    if not 'df_ultimo_servico' in st.session_state:

        st.session_state.df_escalas = gerar_df_phoenix(
            'vw_apr_dias_livres', 
            st.session_state.base_luck
        )

        st.session_state.df_sales = gerar_df_phoenix(
            'vw_sales_apr_dias_livres', 
            st.session_state.base_luck
        )

        st.session_state.df_ultimo_servico = gerar_df_phoenix(
            'vw_data_ultimo_servico', 
            st.session_state.base_luck
        )

        st.session_state.dias_da_semana_ingles_portugues = {
            'Monday': 'Segunda-feira',
            'Tuesday': 'Terça-feira',
            'Wednesday': 'Quarta-feira',
            'Thursday': 'Quinta-feira',
            'Friday': 'Sexta-feira',
            'Saturday': 'Sábado',
            'Sunday': 'Domingo'
        }

st.title('Performance de Dias Livres - Analítico')

st.divider()

row1 = st.columns(2)

df_escalas = plotar_filtros_e_filtrar_escalas(row1)

with row1[1]:

    container_filtro_resumo = st.container(border=True)

    with container_filtro_resumo:

        filtrar_resumos = st.multiselect(
            'Omitir Resumos:', 
            [
                'Resumo Geral', 
                'Resumo por Tipo de Serviço', 
                'Resumo por Serviço', 
                'Resumo por Dia'
            ], 
            default=None
        )

if df_escalas is not None:

    df_sales = tratar_df_sales()

    df_escalas = calcular_performance(df_escalas, df_sales)

    st.divider()

    df_escalas_agrupadas = gerar_df_escalas_agrupadas(df_escalas)

    if st.session_state.base_luck == 'test_phoenix_joao_pessoa':

        dict_replace = {
            'AEROPORTO JOÃO PESSOA / HOTEIS JOÃO PESSOA': 'IN - JPA'
        }

        df_escalas_agrupadas['Serviço'].replace(
            dict_replace, 
            inplace=True
        )

    plotar_resumos(df_escalas_agrupadas, filtrar_resumos)
