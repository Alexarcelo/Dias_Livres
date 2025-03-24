import streamlit as st
import mysql.connector
import decimal
import pandas as pd
from datetime import date, timedelta

def puxar_dados_phoenix():

    def gerar_df_phoenix(base_luck, vw_name):
        
        config = {
            'user': 'user_automation_jpa', 
            'password': 'luck_jpa_2024', 
            'host': 'comeia.cixat7j68g0n.us-east-1.rds.amazonaws.com', 
            'database': base_luck
            }

        conexao = mysql.connector.connect(**config)

        cursor = conexao.cursor()

        request_name = f'''SELECT * FROM {vw_name}'''

        cursor.execute(request_name)

        resultado = cursor.fetchall()
        
        cabecalho = [desc[0] for desc in cursor.description]

        cursor.close()

        conexao.close()

        df = pd.DataFrame(resultado, columns=cabecalho)

        df = df.applymap(lambda x: float(x) if isinstance(x, decimal.Decimal) else x)

        return df

    with st.spinner('Puxando dados do Phoenix...'):

        st.session_state.df_escalas = gerar_df_phoenix(
            st.session_state.base_luck, 
            'vw_in_sem_guia'
        )

        st.session_state.servicos_por_reserva = gerar_df_phoenix(
            st.session_state.base_luck, 
            'vw_servicos_por_reserva'
        )

def botao_atualizar_dados_phoenix(row1):

    with row1[0]:

        atualizar_phoenix = st.button('Atualizar Dados Phoenix')

        if atualizar_phoenix:

            puxar_dados_phoenix()

def colher_periodo_gerar_relatorio(row1):

    with row1[0]:

        container_datas = st.container(border=True)

        container_datas.subheader('Período')

        data_inicial = container_datas.date_input(
            'Data Inicial', 
            value=date.today() +  timedelta(days=1), 
            format='DD/MM/YYYY', 
            key='data_inicial'
        )

        data_final = container_datas.date_input(
            'Data Final', 
            value=date.today() +  timedelta(days=1), 
            format='DD/MM/YYYY', 
            key='data_final'
        )

        omitir_dias_livres_zero = container_datas.checkbox(
            'Omitir Reservas s/ Dias Livres', 
            key='omitir_dias_livres_zero'
        )

    return data_inicial, data_final, omitir_dias_livres_zero

def gerar_df_escalas_filtro_data(data_inicial, data_final):

    # Filtrando período escolhido pelo usuário

    df_escalas_filtro_data = st.session_state.df_escalas[
        (st.session_state.df_escalas['Data_da_Escala'] >= data_inicial) & 
        (st.session_state.df_escalas['Data_da_Escala'] <= data_final)
    ]

    # Se a data do IN for antes de hoje, vai usar a data de hoje como base, se não, usa a data do IN

    df_escalas_filtro_data['Data_IN_Hoje'] = df_escalas_filtro_data['Data_IN'].apply(
        lambda x: date.today() 
        if x < date.today() 
        else x
    )

    # Filtra apenas as reservas que ainda não foram finalizadas

    df_escalas_filtro_data = df_escalas_filtro_data[
        df_escalas_filtro_data['Data_Ultimo_Servico'] > df_escalas_filtro_data['Data_IN_Hoje']
    ]

    # Calcula a estadia de cada reserva

    df_escalas_filtro_data['Estadia'] = (pd.to_datetime(df_escalas_filtro_data['Data_Ultimo_Servico']) - pd.to_datetime(df_escalas_filtro_data['Data_IN_Hoje'])).dt.days

    df_escalas_filtro_data['Estadia'] = df_escalas_filtro_data['Estadia'].apply(
        lambda x: 0 
        if x <= 0
        else x-1
    )

    return df_escalas_filtro_data

def gerar_df_servicos_por_reserva_final():

    # Pegando combinações de reservas, datas IN e datas de último serviço

    df_ref = df_escalas_filtro_data[
        [
            'Reserva_Mae',
            'Data_IN_Hoje',
            'Data_Ultimo_Servico'
        ]
    ]

    # Mesclando com os serviços por reserva (pegando todas as combinações)

    df_merged = df_ref.merge(
        df_servicos_por_reserva[
            [
                'Reserva_Mae', 
                'Data_Execucao'
            ]
        ], 
        on='Reserva_Mae', 
        how='inner'
    )

    # Filtrando apenas os serviços que foram executados entre a data IN e a data do último serviço

    df_merged = df_merged[
        (df_merged['Data_Execucao'] > df_merged['Data_IN_Hoje']) &
        (df_merged['Data_Execucao'] < df_merged['Data_Ultimo_Servico'])
    ]

    # Agrupando por reserva e contando a quantidade de serviços

    df_servicos_por_reserva_final = df_merged.groupby('Reserva_Mae', as_index=False).agg(
        {
            'Data_Execucao': 'count'
        }
    )

    df_servicos_por_reserva_final.columns = ['Reserva_Mae', 'Qtd_Servicos_ate_Ultimo_Servico']

    return df_servicos_por_reserva_final

def plotar_tabela_final(df_escalas_filtro_data):

    df_visualizacao = df_escalas_filtro_data[
        [
            'Reserva_Mae',
            'Cliente',
            'Telefone_Cliente', 
            'Servico', 
            'Estabelecimento_Destino',
            'Voo', 
            'Data_IN_Hoje', 
            'Data_Ultimo_Servico', 
            'Total_Paxs', 
            'Estadia', 
            'Qtd_Servicos_ate_Ultimo_Servico', 
            'Dias_Livres'
        ]
    ]

    df_visualizacao['Telefone_Cliente'] = df_visualizacao['Telefone_Cliente'].fillna('')

    df_visualizacao.rename(
        columns={
            'Reserva_Mae': 'Reserva',
            'Telefone_Cliente': 'Telefone Cliente',
            'Servico': 'Serviço',
            'Estabelecimento_Destino': 'Hotel',
            'Voo': 'Voo IN',
            'Data_IN_Hoje': 'Data IN / Hoje',
            'Data_Ultimo_Servico': 'Data Último Serviço',
            'Total_Paxs': 'Total Paxs',
            'Qtd_Servicos_ate_Ultimo_Servico': 'Qtd. Serviços',
            'Dias_Livres': 'Dias Livres'
        }, 
        inplace=True
    )

    row_height = 35  
    max_height = 1000
    min_height = 200

    num_rows = len(df_visualizacao)
    calculated_height = min(max(row_height * num_rows, min_height), max_height)

    st.dataframe(
        df_visualizacao,
        hide_index=True,
        use_container_width=True,
        height=calculated_height
    )

st.set_page_config(layout='wide')

if not 'servicos_por_reserva' in st.session_state:

    puxar_dados_phoenix()

st.title('Dias Livres - Reservas sem Guia IN')

st.markdown('*cálculo de dias livres nas reservas que tiveram transfer IN no período escolhido, mas chegaram sem guia*')

st.divider()

row1 = st.columns(2)

row2 = st.columns(1)

# Botão pra puxar dados do Phoenix manualmente

botao_atualizar_dados_phoenix(row1)

# Botões de input de datas

data_inicial, data_final, omitir_dias_livres_zero = colher_periodo_gerar_relatorio(row1)

if data_inicial and data_final:

    df_escalas_filtro_data = gerar_df_escalas_filtro_data(data_inicial, data_final)

    # Filtrando apenas reservas que estão no relatório de IN e que tem data de execução posterior a data inicial escolhida

    df_servicos_por_reserva = st.session_state.servicos_por_reserva[
        (st.session_state.servicos_por_reserva['Data_Execucao'] > data_inicial) &
        (st.session_state.servicos_por_reserva['Reserva_Mae'].isin(df_escalas_filtro_data['Reserva_Mae'].unique()))
    ]

    # Criando dataframe com quantidade de serviços por reserva. Apenas serviços que foram executados entre a data IN e a data do último serviço

    df_servicos_por_reserva_final = gerar_df_servicos_por_reserva_final()

    # Adicionando coluna com quantidade de serviços até o último serviço

    df_escalas_filtro_data = df_escalas_filtro_data.merge(
        df_servicos_por_reserva_final, 
        on='Reserva_Mae', 
        how='left'
    )

    df_escalas_filtro_data['Qtd_Servicos_ate_Ultimo_Servico'] = df_escalas_filtro_data['Qtd_Servicos_ate_Ultimo_Servico'].fillna(0)

    # Calculando dias livres

    df_escalas_filtro_data['Dias_Livres'] = df_escalas_filtro_data['Estadia'] - df_escalas_filtro_data['Qtd_Servicos_ate_Ultimo_Servico']

    if omitir_dias_livres_zero:

        df_escalas_filtro_data = df_escalas_filtro_data[
            df_escalas_filtro_data['Dias_Livres'] > 0
        ]

    # Plotando tabela final

    plotar_tabela_final(df_escalas_filtro_data)
