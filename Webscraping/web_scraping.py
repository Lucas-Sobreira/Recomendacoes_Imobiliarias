#Drivers para abrir o navegador
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options

#Localizar dentro do html
from selenium.webdriver.common.by import By

# BS4 
from bs4 import BeautifulSoup

# Import Regex 
import re

# from csv import writer
import csv

# Pegar a data que está rodando
from datetime import date

# Biblioteca para sobreescrever o DataFrame do Webscraping
import pandas as pd

# Função para Checar se tem ou não a Feature no Apto
def check_feature(feature):
    try:
        check = feature.find('svg', class_= re.compile(r"jss73 [A-Za-z0-9]+", re.IGNORECASE))
    except:
        check = None
    
    if check is not None:
        return 'Não - ' + feature.text
    else: 
        return feature.text
    
# HTML da página    
def page_driver(driver, url):
    driver.get(url)
    page_content = driver.page_source
    html_page = BeautifulSoup(page_content, 'html.parser')
    return html_page 

def webscraping(today, path):

    # Testando os drivers para rodar o Selenium 
    service = Service()
    options = webdriver.ChromeOptions()
    options.add_experimental_option('excludeSwitches', ['enable-logging'])

    driver = webdriver.Chrome(service= service, options=options)

    driver.implicitly_wait(30)

    # Link padrão para pesquisa
    default_link = 'https://loft.com.br'

    # Pegando informações da página principal
    url = default_link + '/venda/imoveis/sp/sao-paulo?pagina={}'
    url_page1 = url.format(1)

    # Padrão para pegar o HTML
    # HTML Página 01
    site = page_driver(driver, url_page1)

    # Quantidade de Páginas que tem no site
    total_pages = site.find('a', class_='MuiTypography-root MuiLink-root jss346 MuiLink-underlineNone jss347 jss362 jss349 jss364 jss339 MuiTypography-colorPrimary').text
    
    # Salvando o DataFrame no CSV
    with open(path + 'webscraping{}.csv'.format(today), 'a', encoding= 'utf-8', newline='') as csvfile:
        
        # Habilitando a escrita e salvando o Cabeçalho
        writer = csv.writer(csvfile)
        writer.writerow(['Link_Apto', 'Endereco', 'Bairro', 'Valor', 'Informacoes'])    

        # Varrendo todas as páginas do Site  
        for n_page in range(1, int(total_pages)+1): 
            print('Página {} de {}'.format(n_page, total_pages))
        
            # HTML p/ página 
            url_page = url.format(n_page)
            page = page_driver(driver, url_page)
            
            # Pegando todos os links da página atual 
            links = page.find_all('a', class_='MuiButtonBase-root MuiCardActionArea-root jss263')
            if links == []: 
                print('Não foi possível encontrar o link do imóvel\nProvavelmente a "class_" foi alterada\n')

            # Laço para todos os aptos da página atual
            for link in links:
                features = []
                infos = []
    
                # Link do Apto
                try:                
                    apto_link = link['href']
                    infos.append(default_link + apto_link)
                    print('Link do apto atual: {}'.format(default_link + apto_link))
                except Exception as e:
                    print('Mensagem de erro: {}'.format(e))

                try: 
                    # Entrando em cada página dos aptos existentes na página atual  
                    pagina_apto = page_driver(driver, url= default_link + apto_link) 
                    
                    # Endereço
                    try:                                          
                        endereco = pagina_apto.find('div', class_='MuiGrid-root MuiGrid-item MuiGrid-grid-xs-12 MuiGrid-grid-md-9').text.replace('•', ',').replace('  ', ' ').replace(' ,', ',').split(',')
                        infos.append(endereco)
                    except Exception as e:
                        # print('Mensagem de erro: {}'.format(e))
                        print('Erro na coleta do endereço\nlink:{}\nMensagem de erro: {}'.format((default_link + apto_link), e))

                    # Bairro do Imóvel
                    try:
                        bairro = pagina_apto.find('div', class_='MuiGrid-root MuiGrid-item MuiGrid-grid-xs-12 MuiGrid-grid-md-9').text.replace('•', ',').replace('  ', ' ').replace(' ,', ',').split(',')[-2].replace(' ', '')
                        infos.append(bairro)
                    except Exception as e:
                        print('Erro na coleta do bairro\nlink:{}\nMensagem de erro: {}'.format((default_link + apto_link), e))

                    # Valor do Imóvel
                    classes_interesse = ['c-gPjxah c-gPjxah-cmVlgk-align-left c-gPjxah-iPJLV-css Copan_Typography c-PJLV c-PJLV-cUuldJ-textStyle-h3 c-PJLV-bxuTfx-cv',
                                         'MuiTypography-root jss200 jss177 jss185 MuiTypography-body1',
                                         'MuiTypography-root jss189 jss166 jss174 MuiTypography-body1'
                                         ]
                    
                    # Função para encontrar o valor com base nas classes de interesse
                    def encontrar_valor(pagina_apto, classes_interesse):
                        for classe in classes_interesse:
                            valor = pagina_apto.find('p', class_=classe)
                            if valor:
                                return valor.text
                        return None
                    
                    # Tentativa de encontrar o valor
                    valor = encontrar_valor(pagina_apto, classes_interesse)

                    if valor:
                        infos.append(valor)
                    else:
                        print('Erro na coleta do valor\nlink:{}\nMensagem de erro: Nenhuma classe de interesse encontrada'.format((default_link + apto_link)))
                        # infos.append("Erro na coleta")

                    # Pegando todas as Infos do Imóvel
                    grid_features = pagina_apto.find('div', class_='MuiGrid-root MuiGrid-container')

                    for feature in grid_features:
                        # Verificando se tem ou não a feature no apto   
                        features.append(check_feature(feature))

                    # Salvando tudo no .CSV    
                    infos.append(features)
                    writer.writerow(infos)                     
                    
                except: 
                    pass
            
    csvfile.close()

    driver.quit()

def input_date(today, path):
    df = pd.read_csv(path + 'webscraping{}.csv'.format(today))
    df = df.query("Link_Apto != 'Link_Apto'") # Caso seja necessário rodar mais de uma vez o mesmo código, ele limpa o cabeçalho a onde deveria ser o dado. 
    df.insert(0, 'Refdate', today.replace('-', '/')) # Historização dos dados
    df.to_csv(path + 'webscraping{}.csv'.format(today), index=False, encoding= 'utf-8')

if __name__== "__main__":
    today = date.today().strftime('%d-%m-%Y')
    print('Data: {}'.format(today.replace('-', '/')))

    # Pasta para Salvar o .CSV
    path = '../Docker/webscraping_data/'

    # Realizando o Web Scraping
    webscraping(today, path)

    # Inserindo a data no DF
    input_date(today, path)
