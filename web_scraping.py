#Drivers para abrir o navegador
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

#Localizar dentro do html
from selenium.webdriver.common.by import By

# BS4 
from bs4 import BeautifulSoup

# Import Regex 
import re

# from csv import writer
import csv


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

# Webscraping
def webscraping():
    # Pasta para Salvar o .CSV
    path = 'C:/Users/lucas/Desktop/WebScraping/arquivo_teste/'

    # Caminho do Driver
    executable_path = 'S:/Management and Trading/Back Office Operacoes/Passivo/Shareholders_WebFilling/Drivers/Chromedrivers/'

    # Testando os drivers para rodar o Selenium 
    options = webdriver.ChromeOptions()
    options.add_experimental_option('excludeSwitches', ['enable-logging'])

    try: 
        driver = webdriver.Chrome(executable_path= executable_path + 'chromedriver1.10', options=options)
    except: 
        driver = webdriver.Chrome(executable_path= executable_path + 'chromedriver1.11', options=options)

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
    total_pages = site.find('a', class_='MuiTypography-root MuiLink-root jss333 MuiLink-underlineNone jss334 jss349 jss336 jss351 jss326 MuiTypography-colorPrimary').text

    # Salvando o DataFrame no CSV
    with open(path + 'webscraping01.csv', 'a', encoding= 'utf-8', newline='') as csvfile:
        
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
            links = page.find_all('a', class_='MuiButtonBase-root MuiCardActionArea-root jss262')

            # Laço para todos os aptos da página atual
            for link in links:
                features = []
                infos = []
    
                # Link do Apto
                try:                
                    apto_link = link['href']
                    infos.append(default_link + apto_link)
                    # print('Link do apto atual: {}'.format(default_link + apto_link))
                except Exception as e:
                    print('Mensagem de erro: {}'.format(e))

                try: 
                    # Entrando em cada página dos aptos existentes na página atual  
                    pagina_apto = page_driver(driver, url= default_link + apto_link)     

                    # Ver se o Imóvel está disponível no site
                    try: 
                        disp = pagina_apto.find('h2', class_='MuiTypography-root jss138 jss115 jss123 MuiTypography-body1').text
                        print('Imóvel Indisponivel:    {}'.format(apto_link))                 

                    except:
                        # Endereço do Imóvel
                        try:                
                            endereco = pagina_apto.find('div', class_='MuiGrid-root MuiGrid-item MuiGrid-grid-xs-12 MuiGrid-grid-md-9').text.replace('•', ',').replace('  ', ' ').replace(' ,', ',').split(',')
                            infos.append(endereco)
                        except Exception as e:
                            print('Mensagem de erro: {}'.format(e))

                        # Bairro do Imóvel
                        try:
                            bairro = pagina_apto.find('div', class_='MuiGrid-root MuiGrid-item MuiGrid-grid-xs-12 MuiGrid-grid-md-9').text.replace('•', ',').replace('  ', ' ').replace(' ,', ',').split(',')[-2].replace(' ', '')
                            infos.append(bairro)
                        except Exception as e:
                            print('Mensagem de erro: {}'.format(e))

                        # Valor do Imóvel
                        try:
                            valor = pagina_apto.find('p', class_='MuiTypography-root jss197 jss174 jss182 MuiTypography-body1').text
                            infos.append(valor)
                        except:
                            valor = pagina_apto.find('p', class_='MuiTypography-root jss201 jss178 jss186 MuiTypography-body1').text
                            infos.append(valor)
                        else: 
                            pass

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

if __name__== "__main__":
     webscraping()
