import pandas as pd
import mysql.connector
import decimal
import streamlit as st
from datetime import date, timedelta

def bd_phoenix(vw_name, base_luck):
    
    config = {
    'user': 'user_automation_jpa',
    'password': 'luck_jpa_2024',
    'host': 'comeia.cixat7j68g0n.us-east-1.rds.amazonaws.com',
    'database': base_luck
    }

    conexao = mysql.connector.connect(**config)
    cursor = conexao.cursor()

    request_name = f'SELECT `Reserva`, `Data Execucao`, `Tipo de Servico` FROM {vw_name}'

    cursor.execute(
        request_name
    )

    resultado = cursor.fetchall()

    cabecalho = [desc[0] for desc in cursor.description]

    cursor.close()
    conexao.close()

    df = pd.DataFrame(resultado, columns=cabecalho)
    df = df.applymap(lambda x: float(x) if isinstance(x, decimal.Decimal) else x)
    
    return df

def puxar_dados_phoenix(dict_base_luck, base_luck):

    with st.spinner('Puxando dados do Phoenix...'):

        st.session_state.mapa_router = bd_phoenix('vw_router', dict_base_luck[base_luck][0])

        st.session_state.mapa_router['Reserva Mãe'] = st.session_state.mapa_router['Reserva'].str[:10]

def gerar_df_primeiros_servicos_no_periodo(data_servicos):

    df_primeiro_serviço = st.session_state.mapa_router[~pd.isna(st.session_state.mapa_router['Data Execucao'])].groupby('Reserva Mãe')['Data Execucao'].min().reset_index()

    df_primeiro_serviço_periodo = df_primeiro_serviço[(df_primeiro_serviço['Data Execucao']==data_servicos)].reset_index(drop=True)

    df_primeiro_serviço_periodo = df_primeiro_serviço_periodo.rename(columns={'Data Execucao': 'Data Primeiro Serviço'})

    reservas_periodo = df_primeiro_serviço_periodo['Reserva Mãe'].unique()

    return df_primeiro_serviço_periodo, reservas_periodo

def adicionar_data_de_out(reservas_periodo, df_primeiro_serviço_periodo):

    df_out = st.session_state.mapa_router[(st.session_state.mapa_router['Reserva Mãe'].isin(reservas_periodo)) & (st.session_state.mapa_router['Tipo de Servico']=='OUT')]\
        .groupby(['Reserva Mãe'])['Data Execucao'].min().reset_index()

    df_primeiro_serviço_periodo = pd.merge(df_primeiro_serviço_periodo[['Reserva Mãe', 'Data Primeiro Serviço']], df_out[['Reserva Mãe', 'Data Execucao']], on='Reserva Mãe', how='left')

    df_com_out = df_primeiro_serviço_periodo[~pd.isna(df_primeiro_serviço_periodo['Data Execucao'])].reset_index(drop=True)

    df_com_out = df_com_out.rename(columns={'Data Execucao': 'Data OUT'})

    reservas_periodo_com_out = df_com_out['Reserva Mãe'].unique()

    return df_com_out, reservas_periodo_com_out

def adicionar_data_ultimo_servico(reservas_periodo_com_out, df_com_out):
    
    df_servicos = st.session_state.mapa_router[(st.session_state.mapa_router['Reserva Mãe'].isin(reservas_periodo_com_out)) & 
                                               (st.session_state.mapa_router['Tipo de Servico'].isin(['TOUR', 'TRANSFER', 'IN']))].groupby(['Reserva Mãe'])['Data Execucao'].max().reset_index()
    
    df_com_out = pd.merge(df_com_out, df_servicos, on='Reserva Mãe', how='left')

    df_com_out = df_com_out.rename(columns={'Data Execucao': 'Data Último Serviço'})

    return df_com_out

def plotar_resultado(df_com_out):

    df_reservas_com_problema = df_com_out[df_com_out['Data Último Serviço']>=df_com_out['Data OUT']]

    reservas_com_problema = ', '.join(df_com_out[df_com_out['Data Último Serviço']>=df_com_out['Data OUT']]['Reserva Mãe'].unique())

    df_reservas_com_problema['Data Último Serviço'] = pd.to_datetime(df_reservas_com_problema['Data Último Serviço']).dt.strftime('%d/%m/%Y')

    df_reservas_com_problema['Data OUT'] = pd.to_datetime(df_reservas_com_problema['Data OUT']).dt.strftime('%d/%m/%Y')

    if len(df_reservas_com_problema)>0:

        st.error(f'*As reservas {reservas_com_problema} tem algum serviço com a mesma data do OUT ou com data posterior ao OUT*')

        st.dataframe(df_reservas_com_problema[['Reserva Mãe', 'Data OUT', 'Data Último Serviço']], hide_index=True)

    else:

        st.success(f'*Não existem reservas com serviço no mesmo dia ou após o OUT para a data solicitada*')
        
st.set_page_config(layout='wide')

if not 'base_luck_escolhida' in st.session_state:

    st.session_state.base_luck_escolhida = None

base_luck = st.selectbox('Base Luck', ['Aracajú', 'João Pessoa', 'Maceió', 'Natal', 'Noronha', 'Recife', 'Salvador'], index=None)

dict_base_luck = {'Aracajú': ['test_phoenix_aracaju', 'Conferência de Serviços - Aracajú'], 'João Pessoa': ['test_phoenix_joao_pessoa', 'Conferência de Serviços - João Pessoa'], 
                  'Maceió': ['test_phoenix_maceio', 'Conferência de Serviços - Maceió'], 'Natal': ['test_phoenix_natal', 'Conferência de Serviços - Natal'], 
                  'Noronha': ['test_phoenix_noronha', 'Conferência de Serviços - Noronha'], 'Recife': ['test_phoenix_recife', 'Conferência de Serviços - Recife'], 
                  'Salvador': ['test_phoenix_salvador', 'Conferência de Serviços - Salvador']}

if base_luck:

    if st.session_state.base_luck_escolhida != base_luck:

        puxar_dados_phoenix(dict_base_luck, base_luck)

        st.session_state.base_luck_escolhida = base_luck

    st.title(dict_base_luck[base_luck][1])

    st.divider()

    row0 = st.columns(2)

    with row0[0]:

        atualizar_dados_phoenix = st.button('Atualizar Dados Phoenix')

        data_servicos = st.date_input('Data IN *(ou primeiro serviço)*', value=date.today()+timedelta(days=1),format='DD/MM/YYYY', key='data_servicos')

    if atualizar_dados_phoenix:

        puxar_dados_phoenix(dict_base_luck, base_luck)

    if data_servicos:

        df_primeiro_serviço_periodo, reservas_periodo = gerar_df_primeiros_servicos_no_periodo(data_servicos)

        df_com_out, reservas_periodo_com_out = adicionar_data_de_out(reservas_periodo, df_primeiro_serviço_periodo)

        df_com_out = adicionar_data_ultimo_servico(reservas_periodo_com_out, df_com_out)

        plotar_resultado(df_com_out)    
