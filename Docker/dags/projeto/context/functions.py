import re

def functions():
    # Ajustando a Coluna Valor -> Retirando o R$ e a separação de .
    def valor(value):
        return (str
                .replace("R$ ", "")
                .replace(".", "")
            )

    # Função "Explode" na coluna Endereço
    def endereco(value):
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
    def bairro(value):  
        return re.sub(r'(?<!^)(?=[A-Z])', ' ', str)

    ## Explode Infos
    # Andares
    def andares(value):
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
    def tamanho(value):
        regex = r'[0-9]+\sm²'
        request = re.findall(regex, str)
        if request:
            return int(request[0].split(' m²')[0])

        return 0 

    #Preco
    def preco(value): 
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
    def quartos(value):
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
    def suite(value):
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
    def garagem(value):
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
    def banheiros(value):
        regex = r'[0-9]\sBan\w+'
        request = re.findall(regex, str)
        if request: 
            return int(request[0]
                    .split(' Ba')[0])
        
        return 0 

    # Varanda
    def varanda(value):
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
    def mobiliado(value):
        regex = r'Não\s\-\sMob\w+'
        request = re.findall(regex, str)
        if request:
            return 'Não'
        
        regex = r'Mob\w+'
        request = re.findall(regex, str)
        if request:
            return 'Sim'
        
    # Portaria
    def portaria(value):
        regex = r'Por\w+'
        request = re.findall(regex, str)
        if request: 
            if (request[0] == 'Portaria24h'): 
                return '24h'
            
            elif (request[0] == 'Portariaparcial'): 
                return 'Parcial'
        
        return 'Sem descrição'

    # Metro
    def metro(value):
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
        
