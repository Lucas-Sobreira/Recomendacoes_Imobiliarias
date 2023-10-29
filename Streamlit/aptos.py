import pandas as pd 
import streamlit as st

@st.cache_data
def loading_data(path = 'trust.csv'):

    df = (pd
          .read_csv(path)
          .sort_values('id', ascending=True)
          .reset_index().drop(['id', 'index'], axis=1)
          .dropna()
          .rename(columns={'Lat': 'lat', 'Long': 'lon'})
          )

    return df

if __name__ == '__main__':
    # Título e Subtítulo
    st.title('Recomendações de Apartamentos em São Paulo')

    # Lendo o DataFrame    
    df_aptos = loading_data()   

    # Apresentando os Dados  
    with st.expander('Filters'):
        with st.sidebar:
            # Bairro
            st.subheader('Bairro')
            st.text_input(' ', key='bairro')
            bairro = st.session_state.bairro

            if bairro != '': 
                df_aptos = df_aptos.query('Bairro == "{}"'.format(bairro))
            else: 
                df_aptos = df_aptos     

            # Faixa de Valor
            st.subheader('Faixa de valor')
            valor_imovel = st.slider(' ', df_aptos['Valor_RS'].min(), df_aptos['Valor_RS'].max(), int(df_aptos['Valor_RS'].max() / 2))

            # Tamanho dos imóveis        
            st.subheader('Tamanho (m2)')
            tamanho = st.slider(' ', df_aptos['Tamanho_m2'].min(), df_aptos['Tamanho_m2'].max(), int(df_aptos['Tamanho_m2'].max() / 2))

            # Quartos 
            st.subheader('Quantidade de quartos')
            quartos = st.slider(' ', df_aptos['Quartos'].min(), df_aptos['Quartos'].max(), int(df_aptos['Quartos'].max() / 2))

            # Suites 
            st.subheader('Quantidade de suítes')
            suites = st.slider(' ', df_aptos['Suites'].min(), df_aptos['Suites'].max(), int(df_aptos['Suites'].max() / 2))

            # Garagem 
            st.subheader('Quantidade de garagens')
            garagem = st.slider(' ', df_aptos['Garagem'].min(), df_aptos['Garagem'].max(), df_aptos['Garagem'].min())

            # Banheiro 
            st.subheader('Quantidade de banheiros')
            banheiro = st.slider(' ', df_aptos['Banheiros'].min(), df_aptos['Banheiros'].max(), 7)

            # Varanda 
            st.subheader('Quantidade de varandas')
            varanda = st.slider(' ', df_aptos['Varanda'].min(), df_aptos['Varanda'].max(), 2)

            # Mobiliado
            st.subheader('Mobiliado?')
            check = st.checkbox(" ")

            if check: 
                st.text("Apartamento mobiliado")
                mobiliado = 'Sim'
            else: 
                st.text("Apartamento não mobiliado")
                mobiliado = 'Não'

            # Metrô 
            st.subheader('Distância até o metrô')
            metro = st.slider(' ', df_aptos['Metro'].min(), 700, 300)

    # Apresentando os Dados
    if st.checkbox('Show DataFrame and Map'):   

        # Filtro de Dados
        df_aptos = df_aptos.query('Valor_RS <= {} and \
                                  Tamanho_m2 <= {} and \
                                  Quartos <= {} and \
                                  Suites <= {} and \
                                  Garagem <= {} and \
                                  Banheiros <= {} and \
                                  Varanda <= {} and \
                                  Mobiliado == "{}" and \
                                  Metro <= {}'
                                  .format(valor_imovel, tamanho, quartos, suites, garagem, banheiro, varanda, mobiliado, metro))

        # Plotando o Mapa
        st.subheader('Mapa Geolocalização')
        st.map(df_aptos[['lat', 'lon']])

        st.divider()

        # DataFrame
        st.subheader('DataFrame')
        st.dataframe(df_aptos) 