# Recomendacoes_Imobiliarias

### Projeto de Engenharia de Dados, utilizando Webscraping para a Ingestão de dados. 

## Objetivo do projeto

<p align="justify">
O objetivo desse trabalho é promover uma <b>arquitetura escalável</b> na área imobiliária das regiões de São Paulo, para que seja de fácil acesso consultar os imóveis disponíveis para venda e recomendá-los, de acordo com o <b>perfil</b> e preferência do cliente, utilizando uma plataforma de <b>BI (Business Intelligence)</b>. 
</p>

## Contextualização do problema 

<p align="justify">
As imobiliárias geralmente apresentam um <b>enorme portfólio de imóveis</b> para vender e alugar, muitas vezes deixando os <b>clientes confusos</b> com tantas informações. 
<br>
<br>
Tendo isso em vista, a imobiliária solicitou para a área de dados auxiliar com esse <b>business case</b>, afim de deixar a reunião com o cliente mais direcionada, <b>facilitando</b> assim a <b>decisão do cliente</b> na hora da escolha de seu futuro imóvel. 
<br>
<br>
O funcionário poderá guiar o cliente, de acordo com os gostos e preferências, faixa de preço e localização do imóvel.        
</p>

## Arquitetura da Solução e Detalhamento

<div align="center">
<img src="https://github.com/Lucas-Sobreira/Recomendacoes_Imobiliarias/blob/main/imgs/arquitetura.png" width="650" height="350"/>
</div>

<br>

1) <b><ins>Ingestão</ins></b>: Foi desenvolvido um job em Python para coletar as informações necessárias do site da Loft; 
2) <b><ins>Orquestração e Processamento</ins></b>: Foi utilizada a linguagem Spark para processamento dos dados, realizando o orquestramento dos jobs via Airflow e Jupyter Notebook como ambiente de desenvolvimento de código; 
3) <b><ins>API Geolocalização</ins></b>: Foi utilizada a biblioteca “geopy” e geocoder “Nominatim” para coleta dos dados de geolocalização (latitude e longitude);
4) <b><ins>Data Lake</ins></b>: Na camada Landing foi realizada a ingestão dos dados vindos do Web Scraping. A camada Raw espelha os dados da cama Landing e as duas camadas posteriores (Context e Trust) armazenam os dados enriquecidos; 
5) <b><ins>Exploração e Visualização</ins></b>: Parte responsável por disponibilizar os dados e gerar os relatórios dos imóveis à venda na cidade de São Paulo, a fim de auxiliar na melhor escolha possível para compra do imóvel;

### <ins>Dicionário de Dados</ins>

<div align="center">
<img src="https://github.com/Lucas-Sobreira/Recomendacoes_Imobiliarias/blob/main/imgs/dicionario_dados.png" width="800" height="350"/>
</div>

### <ins>Relatório Final</ins>

<div align="center">
<img src="https://github.com/Lucas-Sobreira/Recomendacoes_Imobiliarias/blob/main/imgs/relatorio.png" width="900" height="450"/>
</div>
