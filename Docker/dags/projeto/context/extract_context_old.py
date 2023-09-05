from pyspark.sql.types import *
import pyspark.sql.functions as fn
from pyspark.sql import SparkSession
import re

# Ajustando a Coluna Valor -> Retirando o R$ e a separação de .
def valor(str):
    return (str
            .replace("R$ ", "")
            .replace(".", "")
        )

# Função "Explode" na coluna Endereço
def endereco(str):
    padroes = [
        r'Cond[a-z].+SP',
        r'Ru[a-z].+SP',
        r'Av[a-z].+SP',
        r'Praç[a-z].+SP',
        r'Alamed[a-z].+SP',
        r'Travess[a-z].+SP',
        r'Parqu[a-z].+SP',
        r'Viel[a-z].+SP', 
        r'Vil[a-z].+SP', 
        r'Lar[a-z].+SP',
        r'Estr[a-z].+SP', 
        r'Jar[a-z].+SP', 
        r'Viad[a-z].+SP'
    ]

    for padrao in padroes:
        resultado = re.findall(padrao, str.replace('Condomínio à venda', ''))
        if resultado:
            return (resultado[0]
                    .replace("', ' ", "-")
                    .replace('-', ' - ')
                    .replace('" ', '')
                    .replace('"', '')
                    .replace("' ", '')
                    .replace(", ", " - ")
                )

    # Se nenhum padrão foi encontrado, retorna None
    return None

# Bairro
def bairro(str):  
    return re.sub(r'(?<!^)(?=[A-Z])', ' ', str)

## Explode Infos
# Andares
def andares(str):
    # Andares
    regex = r'[0-9]º\s[a-z]+\s[a-z]+\s[a-z]+'
    request = re.findall(regex, str)
    if request:
        return request[0]

    regex = r'Entre\s[0-9]º\sa\s[0-9]º\s\w+'
    request = re.findall(regex, str)
    if request:
        return request[0]

    floor_regex = r'[0-9]º\sandar'
    request = re.findall(floor_regex, str)
    if request:
        return request[0]

    return 'Sem info'

# Tamanho
def tamanho(str):
    regex = r'[0-9]+\sm²'
    request = re.findall(regex, str)
    if request:
        return int(request[0].split(' m²')[0])

    return 0 

#Preco
def preco(str): 
    regex = r'R\$\s[0-9]+\.[0-9]+\s\/m²|R\$\s[0-9]+\s\/m²'
    request = re.findall(regex, str)
    if request:
        return int(request[0]
                .split('R$ ')[1]
                .split(' /m²')[0]
                .replace('.', ''))

    return 0 

# Quartos
# Considerando que Studio tem 1 quarto
def quartos(str):
    regex = r'[0-9]\squarto[s]?'
    request = re.findall(regex, str)
    if request:
        return (request[0]
                .split('qua')[0])

    regex = r'studio'
    request = re.findall(regex, str)
    if request:
        return '1'

    return 'Sem info'

# Suites
def suite(str):
    regex = r'[0-9]\ssuíte'
    request = re.findall(regex, str)
    if request: 
        return int(request[0]
                .split(' su')[0])

    regex = r'sem\ssuíte'
    request = re.findall(regex, str)
    if request: 
        return 0
    
    return 0 

# Garagem
def garagem(str):
    regex = r'Não\s\-\sVag\w+'
    request = re.findall(regex, str)
    if request:
        return 0 
    
    regex = r'[0-9]\svag\w+'
    request = re.findall(regex, str)
    if request: 
        return int(request[0]
                .split(' va')[0])
    
    return 0 
    
# Banheiros
def banheiros(str):
    regex = r'[0-9]\sBan\w+'
    request = re.findall(regex, str)
    if request: 
        return int(request[0]
                .split(' Ba')[0])
    
    return 0 

# Varanda
def varanda(str):
    regex = r'[0-9]\sVar\w+s'
    request = re.findall(regex, str)
    if request:
        return int(request[0]
                .split(' Va')[0])

    regex = r'Var\w+a'
    request = re.findall(regex, str)
    if request:
        return 1
    
    return 0 

# Mobiliado
def mobiliado(str):
    regex = r'Não\s\-\sMob\w+'
    request = re.findall(regex, str)
    if request:
        return 'Não'
    
    regex = r'Mob\w+'
    request = re.findall(regex, str)
    if request:
        return 'Sim'
    
# Portaria
def portaria(str):
    regex = r'Por\w+'
    request = re.findall(regex, str)
    if request: 
        if (request[0] == 'Portaria24h'): 
            return '24h'
        
        elif (request[0] == 'Portariaparcial'): 
            return 'Parcial'
    
    return 'Sem descrição'

# Metro
def metro(str):
    regex = r'Metrô'
    request = re.findall(regex, str)
    if request: 
        regex = r'a\s[0-9]+m'
        request = re.findall(regex, str)
        return int(request[0]
                .replace('a ', '')
                .replace('m', ''))

    else: 
        return 100000000

# UDF's
valor_UDF     = fn.udf(lambda z: valor(z))
endereco_UDF  = fn.udf(lambda z: endereco(z))
bairro_UDF    = fn.udf(lambda z: bairro(z))
andares_UDF   = fn.udf(lambda z: andares(z))
tamanho_UDF   = fn.udf(lambda z: tamanho(z))
preco_UDF     = fn.udf(lambda z: preco(z))
quartos_UDF   = fn.udf(lambda z: quartos(z))
suite_UDF     = fn.udf(lambda z: suite(z))
garagem_UDF   = fn.udf(lambda z: garagem(z))
banheiros_UDF = fn.udf(lambda z: banheiros(z))
varanda_UDF   = fn.udf(lambda z: varanda(z))
mobiliado_UDF = fn.udf(lambda z: mobiliado(z))
portaria_UDF  = fn.udf(lambda z: portaria(z))
metro_UDF     = fn.udf(lambda z: metro(z))

spark = (SparkSession.builder
         .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
         .config("spark.hadoop.fs.s3a.access.key", "aulafia")
         .config("spark.hadoop.fs.s3a.secret.key", "aulafia@123")
         .config("spark.hadoop.fs.s3a.path.style.access", True)
         .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
         .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
         .getOrCreate()
        )

df_raw = (spark
      .read
      .format("parquet")
      .load("s3a://raw/data")
)

df_raw.printSchema()

df_context = (df_raw
              .withColumn('Valor', valor_UDF(fn.col('Valor')))
              .withColumn('Endereco', endereco_UDF(fn.col('Endereco')))
              .withColumn('Bairro', bairro_UDF(fn.col('Bairro')))
              .withColumn('Andares', andares_UDF(fn.col('Informacoes')))
              .withColumn('Tamanho_m2', tamanho_UDF(fn.col('Informacoes')))
              .withColumn('Valor_RS/m2', preco_UDF(fn.col('Informacoes')))
              .withColumn('Quartos', quartos_UDF(fn.col('Informacoes')))
              .withColumn('Suites', suite_UDF(fn.col('Informacoes')))
              .withColumn('Garagem', garagem_UDF(fn.col('Informacoes')))
              .withColumn('Banheiros', banheiros_UDF(fn.col('Informacoes')))
              .withColumn('Varanda', varanda_UDF(fn.col('Informacoes')))
              .withColumn('Mobiliado', mobiliado_UDF(fn.col('Informacoes')))
              .withColumn('Portaria', portaria_UDF(fn.col('Informacoes')))
              .withColumn('Metro', metro_UDF(fn.col('Informacoes')))
             )

df_context = (df_context
              .drop(fn.col('Informacoes'))
              .withColumnRenamed("Valor", "Valor_RS")
             )

df_context = (df_context
              .select('Link_Apto', 'Endereco', 'Bairro', fn.col('Valor_RS').cast('integer'), 'Andares', 
                      fn.col('Tamanho_m2').cast('integer'), fn.col('Valor_RS/m2').cast('integer'), fn.col('Quartos').cast('integer'), 
                      fn.col('Suites').cast('integer'), fn.col('Garagem').cast('integer'), fn.col('Banheiros').cast('integer'), 
                      fn.col('Varanda').cast('integer'), 'Mobiliado', 'Portaria', fn.col('Metro').cast('integer'), 'Refdate'
                     )
             )

# Elimando "sujeira" da Raw
df_context = (df_context.filter(fn.col('Valor_RS').isNotNull()))

# print("Condomínio à venda")
# (df_context
#  .filter(fn.col("Endereco")
#          .like("%à venda%")
#          )
# ).show(10, False)

df_context.show(10, False)

df_context.printSchema()

(df_context
 .write
 .format('parquet')
 .mode('overwrite')
 .partitionBy('Refdate')
 .save('s3a://context/data')
)

