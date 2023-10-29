from pyspark.sql.types import *
import pyspark.sql.functions as fn
from pyspark.sql import SparkSession

# Geolocalização
import requests
from geopy.geocoders import Nominatim

# Tempo de execução 
import time

# Função responsável por coletar as informações de Geolocalização dos Aptos
def lat_long(endereco):
    url = 'https://nominatim.openstreetmap.org/search'
    polygon_geojson = '{"type": "Polygon", "coordinates": [[[-46.40716129144083,-23.69854324841944,0], [-46.38749238945803,-23.65793664277188,0],[-46.35216841140385,-23.6061863663833,0], \
                                                            [-46.3254656926128,-23.52070809238174,0],[-46.36723670560394,-23.4882741441959,0], [-46.3964560698709,-23.42704732285392,0], \
                                                            [-46.4810322050737,-23.4202546515718,0], [-46.57332329583391,-23.46664201988391,0],[-46.67026889598144,-23.51296433526191,0], \
                                                            [-46.81836953868319,-23.50563673390302,0],[-46.8154129968251,-23.52670172370454,0], [-46.8335960455951,-23.63224919274617,0], \
                                                            [-46.82154897217379,-23.75936095180304,0], [-46.71406711016606,-23.82418892073821,0],[-46.55524225662186,-23.78586504747647,0], \
                                                            [-46.40716129144083,-23.69854324841944,0]]]}'
    params = {'q': endereco, 'format': 'json', 'limit': 1, 'polygon_geojson': polygon_geojson}

    try:
        response = requests.get(url, params=params).json()

        if response:
            location = response[0]
            latitude = location['lat']
            longitude = location['lon']
            return f'{latitude},{longitude}'
        else:
            return '0,0'
    except:
        return '-'

# Iniciando a Spark Session
spark = (SparkSession.builder
         .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
         .config("spark.hadoop.fs.s3a.access.key", "aulafia")
         .config("spark.hadoop.fs.s3a.secret.key", "aulafia@123")
         .config("spark.hadoop.fs.s3a.path.style.access", True)
         .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
         .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
         .getOrCreate()
        )

# Lendo os dados da camada Context
df_context = (spark
              .read
              .format("parquet")
              .load("s3a://context/data")
              )

# Verificando se existem dados pré armazenados na camada Trust
try:
    df_trust = (spark
                .read
                .format("parquet")
                .load("s3a://trust/particionado")
                )
except: 
    print('\nA camada trust está vazia\n')

# Limitando a quantidade de linhas para processamento mais rápido
# Finalidade de teste
# df_context = df_context.limit(100)

# Renomeando as colunas
context = (df_context
                .withColumnRenamed('Link_Apto', 'Link_Apto_Context')
                .withColumnRenamed('Endereco', 'Endereco_Context')
                .withColumnRenamed('Valor_RS', 'Valor_RS_Context')
                .withColumnRenamed('Bairro', 'Bairro_Context')
                .withColumnRenamed('Link_Apto', 'Valor_RS_Context')
                .withColumnRenamed('Andares', 'Andares_Context')
                .withColumnRenamed('Tamanho_m2', 'Tamanho_m2_Context')
                .withColumnRenamed('Valor_RS/m2', 'Valor_RS/m2_Context')
                .withColumnRenamed('Quartos', 'Quartos_Context')
                .withColumnRenamed('Suites', 'Suites_Context')
                .withColumnRenamed('Garagem', 'Garagem_Context')
                .withColumnRenamed('Banheiros', 'Banheiros_Context')
                .withColumnRenamed('Varanda', 'Varanda_Context')
                .withColumnRenamed('Mobiliado', 'Mobiliado_Context')
                .withColumnRenamed('Portaria', 'Portaria_Context')
                .withColumnRenamed('Metro', 'Metro_Context')
                .withColumnRenamed('Refdate', 'Refdate_Context')
                )

try: 
    trust = (df_trust
            .withColumnRenamed('Link_Apto', 'Link_Apto_Trust')
            .withColumnRenamed('Endereco', 'Endereco_Trust')
            .withColumnRenamed('Valor_RS', 'Valor_RS_Trust')
            .withColumnRenamed('Bairro', 'Bairro_Trust')
            .withColumnRenamed('Link_Apto', 'Valor_RS_Trust')
            .withColumnRenamed('Andares', 'Andares_Trust')
            .withColumnRenamed('Tamanho_m2', 'Tamanho_m2_Trust')
            .withColumnRenamed('Valor_RS/m2', 'Valor_RS/m2_Trust')
            .withColumnRenamed('Quartos', 'Quartos_Trust')
            .withColumnRenamed('Suites', 'Suites_Trust')
            .withColumnRenamed('Garagem', 'Garagem_Trust')
            .withColumnRenamed('Banheiros', 'Banheiros_Trust')
            .withColumnRenamed('Varanda', 'Varanda_Trust')
            .withColumnRenamed('Mobiliado', 'Mobiliado_Trust')
            .withColumnRenamed('Portaria', 'Portaria_Trust')
            .withColumnRenamed('Metro', 'Metro_Trust')
            .withColumnRenamed('id', 'id_Trust')
            .withColumnRenamed('Refdate', 'Refdate_Trust')
            .withColumnRenamed('Lat', 'Lat_Trust')
            .withColumnRenamed('Long', 'Long_Trust')
            )
    
    # Selecionando os apartamentos que ainda não estão na Trust
    left_join = context.join(trust, trust.Endereco_Trust == context.Endereco_Context, 'left')
    left_join.filter('Lat_Trust is NULL').show(5, truncate=False)

    # Endereços novos para coletar a Geolocalização
    end_uni = left_join.filter('Lat_Trust is NULL').select('Endereco_Context').distinct()
    end_uni = end_uni.withColumnRenamed('Endereco_Context', 'Endereco_Uni')
except: 
    # Coletando os endereços unicos da camada Context
    end_uni = df_context.select('Endereco').distinct()
    end_uni = end_uni.withColumnRenamed('Endereco', 'Endereco_Uni')

end_uni.show(10, truncate=False)
end_uni_pd = end_uni.toPandas()

print('\nDataFrame End_Uni shape: {}'.format(end_uni_pd.shape))

t_antes = time.time()

# Coletando os dados de Geolocalização, utilizando a função "lat_long"
for i in range(end_uni_pd.shape[0]):
    try:
        end = end_uni_pd.loc[i, 'Endereco_Uni']

        # Tratativa para ignorar a parte do Condomínio do Endereço completo
        if end.find('Condomínio') == -1:
            end_uni_pd.loc[i, 'Lat'] = lat_long(end).split(',')[0]
            end_uni_pd.loc[i, 'Long'] = lat_long(end).split(',')[1]
        else: 
            end_uni_pd.loc[i, 'Lat'] = lat_long(" - ".join(z for z in end.split(' - ')[1:])).split(',')[0]
            end_uni_pd.loc[i, 'Long'] = lat_long(" - ".join(z for z in end.split(' - ')[1:])).split(',')[1]
    
    except Exception as e:
        end = end_uni_pd.loc[i, 'Endereco_Uni']
        print('Indice: {}\nEndereço: {}\nMensagem de erro: {}\n\n'.format(i, end, e))

t_depois = time.time()

print("\nO 'for' demorou: {} minutos\n".format((t_depois - t_antes)/60))

# Filtrando os dados 
end_uni_pd = end_uni_pd.query("Lat != '0' and Lat != '-'")

print('Shape depois do filtro: {}'.format(end_uni_pd.shape))

# Transformando os dados, tendo a certeza de que são Float
end_uni_pd['Lat'] = end_uni_pd['Lat'].astype('float64')
end_uni_pd['Long'] = end_uni_pd['Long'].astype('float64')

print('Lat e Long convertidos para Float64')

# Checando se existem dados novos
if end_uni_pd.empty: 
    print('\nSem dados novos\n')
else: 
    # Retransformando para Spark
    end_uni_spark = spark.createDataFrame(end_uni_pd)
    end_uni_spark.show(5, truncate=False)
        
    # Inner Join para juntar as informações 
    df_trust_spark = df_context.join(end_uni_spark, df_context.Endereco == end_uni_spark.Endereco_Uni, 'inner')
    df_trust_spark = (df_trust_spark
                      .drop(fn.col('Endereco_Uni'))
                      .drop(fn.col('id'))
                      .withColumn("id", fn.monotonically_increasing_id()))

    df_trust_spark = (df_trust_spark
                      .select(fn.col('id').cast('long'), fn.col('Refdate').cast('date'), fn.col('Link_Apto').cast('string'), fn.col('Endereco').cast('string'),
                              fn.col('Bairro').cast('string'), fn.col('Valor_RS').cast('integer'), fn.col('Andares').cast('string'), fn.col('Tamanho_m2').cast('integer'), 
                              fn.col('Valor_RS/m2').cast('integer'), fn.col('Quartos').cast('integer'), fn.col('Suites').cast('integer'), fn.col('Garagem').cast('integer'), 
                              fn.col('Banheiros').cast('integer'), fn.col('Varanda').cast('integer'), fn.col('Mobiliado').cast('string'), fn.col('Portaria').cast('string'), 
                              fn.col('Metro').cast('integer'), fn.col('Lat').cast('double'), fn.col('Long').cast('double'))
                              )

    df_trust_spark.show(5, truncate=False)
    df_trust_spark.printSchema()
    df_trust_spark.count()

    # Salvando os dados na camada Trust
    (df_trust_spark
     .write
     .format('parquet')
     .mode('append')
     .partitionBy('Refdate')
     .save('s3a://trust/particionado')
     )
    
    (df_trust_spark
     .write
     .format('parquet')
     .mode('append')
     .save('s3a://trust/unificado')
     )