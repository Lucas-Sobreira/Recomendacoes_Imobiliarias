from pyspark.sql.types import *
import pyspark.sql.functions as fn
from pyspark.sql import SparkSession
import re

# Classe responsavel pela extração de features da coluna Informações
class regex_transform:
    def __init__(self, _value): 
        self.value = _value

    # Ajustando a Coluna Valor -> Retirando o R$ e a separação de .
    def valor(self):
        return (self.value
                .replace("R$ ", "")
                .replace(".", "")
            )
    
    # Função "Explode" na coluna Endereço
    def endereco(self):
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
            resultado = re.findall(padrao, self.value.replace('Condomínio à venda', ''))
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
    def bairro(self):
        return re.sub(r'(?<!^)(?=[A-Z])', ' ', self.value)
    
    ## Explode Infos
    # Andares
    def andares(self):
        regex = r'[0-9]º\s[a-z]+\s[a-z]+\s[a-z]+'
        request = re.findall(regex, self.value)
        if request:
            return request[0]

        regex = r'Entre\s[0-9]º\sa\s[0-9]º\s\w+'
        request = re.findall(regex, self.value)
        if request:
            return request[0]

        floor_regex = r'[0-9]º\sandar'
        request = re.findall(floor_regex, self.value)
        if request:
            return request[0]
        
        return 'Sem info'

    # Tamanho
    def tamanho(self):
        regex = r'[0-9]+\sm²'
        request = re.findall(regex, self.value)
        if request:
            return int(request[0].split(' m²')[0])

        return 0 
    
    #Preco
    def preco(self): 
        regex = r'R\$\s[0-9]+\.[0-9]+\s\/m²|R\$\s[0-9]+\s\/m²'
        request = re.findall(regex, self.value)
        if request:
            return int(request[0]
                    .split('R$ ')[1]
                    .split(' /m²')[0]
                    .replace('.', ''))

        return 0 

    # Quartos
    # Considerando que Studio tem 1 quarto
    def quartos(self):
        regex = r'[0-9]\squarto[s]?'
        request = re.findall(regex, self.value)
        if request:
            return (request[0]
                    .split('qua')[0])

        regex = r'studio'
        request = re.findall(regex, self.value)
        if request:
            return '1'

        return 'Sem info'
    
    # Suites
    def suite(self):
        regex = r'[0-9]\ssuíte'
        request = re.findall(regex, self.value)
        if request: 
            return int(request[0]
                    .split(' su')[0])

        regex = r'sem\ssuíte'
        request = re.findall(regex, self.value)
        if request: 
            return 0
        
        return 0 

    # Garagem
    def garagem(self):
        regex = r'Não\s\-\sVag\w+'
        request = re.findall(regex, self.value)
        if request:
            return 0 
        
        regex = r'[0-9]\svag\w+'
        request = re.findall(regex, self.value)
        if request: 
            return int(request[0]
                    .split(' va')[0])
        
        return 0 

    # Banheiros
    def banheiros(self):
        regex = r'[0-9]\sBan\w+'
        request = re.findall(regex, self.value)
        if request: 
            return int(request[0]
                    .split(' Ba')[0])
        
        return 0 
    
    # Varanda
    def varanda(self):
        regex = r'[0-9]\sVar\w+s'
        request = re.findall(regex, self.value)
        if request:
            return int(request[0]
                    .split(' Va')[0])

        regex = r'Var\w+a'
        request = re.findall(regex, self.value)
        if request:
            return 1
        
        return 0 

    # Mobiliado
    def mobiliado(self):
        regex = r'Não\s\-\sMob\w+'
        request = re.findall(regex, self.value)
        if request:
            return 'Não'
        
        regex = r'Mob\w+'
        request = re.findall(regex, self.value)
        if request:
            return 'Sim'
        
    # Portaria
    def portaria(self):
        regex = r'Por\w+'
        request = re.findall(regex, self.value)
        if request: 
            if (request[0] == 'Portaria24h'): 
                return '24h'
            
            elif (request[0] == 'Portariaparcial'): 
                return 'Parcial'
        
        return 'Sem descrição'
    
    # Metro
    def metro(self):
        regex = r'Metrô'
        request = re.findall(regex, self.value)
        if request: 
            regex = r'a\s[0-9]+m'
            request = re.findall(regex, self.value)
            return int(request[0]
                    .replace('a ', '')
                    .replace('m', ''))

        else: 
            return 100000000

# Criando as UFD's
valor_UDF     = fn.udf(lambda z: regex_transform(z).valor())
endereco_UDF  = fn.udf(lambda z: regex_transform(z).endereco())
bairro_UDF    = fn.udf(lambda z: regex_transform(z).bairro())
andares_UDF   = fn.udf(lambda z: regex_transform(str(z)).andares())
tamanho_UDF   = fn.udf(lambda z: regex_transform(str(z)).tamanho())
preco_UDF     = fn.udf(lambda z: regex_transform(str(z)).preco())
quartos_UDF   = fn.udf(lambda z: regex_transform(str(z)).quartos())
suite_UDF     = fn.udf(lambda z: regex_transform(str(z)).suite())
garagem_UDF   = fn.udf(lambda z: regex_transform(str(z)).garagem())
banheiros_UDF = fn.udf(lambda z: regex_transform(str(z)).banheiros())
varanda_UDF   = fn.udf(lambda z: regex_transform(str(z)).varanda())
mobiliado_UDF = fn.udf(lambda z: regex_transform(str(z)).mobiliado())
portaria_UDF  = fn.udf(lambda z: regex_transform(str(z)).portaria())
metro_UDF     = fn.udf(lambda z: regex_transform(str(z)).metro())

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

# Lendo os dados armazenados na camada raw
df_raw = (spark
      .read
      .format("parquet")
      .load("s3a://raw/data")
)

df_raw.printSchema()

# Fazendo o enriquecimento dos dados utilizando as UDF's criadas
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

# Elimando "sujeira" da Raw
df_context = (df_context.filter(fn.col('Valor_RS').isNotNull()))

print('Quantidade de linhas antes do "drop_duplicates": {}'.format(df_context.count()))

df_context = (df_context
              .na.drop() # Dropando os valores nulos
              .dropDuplicates(["Link_Apto", "Endereco", "Valor_RS"]) # Dropando apartamentos que mantiveram o valor igual ao longo do tempo  
              .orderBy("Refdate", ascending=False)    
              .withColumn("id", fn.monotonically_increasing_id()) # Criando a coluna ID com o autoincremento
              )

df_context.printSchema()

print('Quantidade de linhas antes do "drop_duplicates": {}'.format(df_context.count()))

print('Quantidade de linhas depois do "drop_duplicates": {}'.format(df_context.count()))

df_context = (df_context
              .select('id', 'Refdate', 'Link_Apto', 'Endereco', 'Bairro', fn.col('Valor_RS').cast('integer'), 'Andares', 
                      fn.col('Tamanho_m2').cast('integer'), fn.col('Valor_RS/m2').cast('integer'), fn.col('Quartos').cast('integer'), 
                      fn.col('Suites').cast('integer'), fn.col('Garagem').cast('integer'), fn.col('Banheiros').cast('integer'), 
                      fn.col('Varanda').cast('integer'), 'Mobiliado', 'Portaria', fn.col('Metro').cast('integer')
                     )
             )

df_context.show(10, False)

df_context.printSchema()

# Armazenando os dados na camada Context
(df_context
 .write
 .format('parquet')
 .mode('overwrite')
 .partitionBy('Refdate')
 .save('s3a://context/data')
)