# %%
import pandas as pd
import string
import requests
from datetime import datetime, timedelta
from sqlalchemy import create_engine

# Json com a lista das palavras chave, e as categorias informadas no documento da Pfizer
lista_de_palavras = {'palavras_chave':
                     ['COVID AND (vacina OR antiviral)',
                         'COVID-19 AND (vacina OR antiviral)',
                         'Coronavírus AND (vacina OR antiviral)',
                         'antiviral AND (covid OR covid-19)',
                         'nirmatrelvir',
                         'ritonavir',
                         'Paxlovid',
                         'COVID-19 AND tratamento',
                         'COVID AND tratamento',    
                         '"vacina bivalente" AND (covid OR covid-19)',
                         'covid AND teste',
                         'Covid-19 AND teste',
                         '"campanha de vacinação" AND (covid OR covid-19)',
                         'vacinação AND (covid OR covid-19)',
                         'Comirnaty',
                         '"vacina atenuada" AND (covid OR covid-19)',
                         '"vacina inativada" AND (covid OR covid-19)',
                         '"Sintomas Coronavírus"',
                         '"Sitomas Covid-19"',
                         '"Sintomas Covid"',
                         '"Ministério da saúde" AND (covid OR covid-19)',
                         'SBIm AND (covid OR covid-19)',
                         '"Jornada de imunização" AND (covid OR covid-19)',
                         '"eficácia vacina" AND (covid OR covid-19)',
                         '"segurança vacina" AND (covid OR covid-19)',
                         '"vacina mRNA" AND(Comirnaty OR RNA mensageiro)',
                         'Coronavírus AND (covid OR covid-19)',
                         'hospitalização AND (covid OR covid-19)',
                         'imunização AND (covid OR covid-19)',
                         'pandemia AND (covid OR covid-19)',
                         'sequelas AND (covid OR covid-19)',
                         '"Covid longa" AND (covid OR covid-19)',
                         'imunossuprimido AND (covid OR covid-19)',
                         'imunocomprometido AND (covid OR covid-19)',
                         'diagnóstico AND (covid OR covid-19)'],
                     'categorias': {
                         'Cardiologia':
                         ['AVC',
                             'Derrame',
                             'Arritmia Cardíaca',
                             'Fibrilação Atrial',
                             'Trombose',
                             'TVP',
                             'TEV',
                             'Embolia Pulmonar',
                             'Insuficiência Cardíaca',
                             'Pressão Alta',
                             'Hipertensão'
                          ],
                        #  'Hematologia':
                        #  ['Leucemia',
                        #      'Leucemia Aguda',
                        #      'Mielóide',
                        #      'Linfóide',
                        #      'Mieloma Múltiplo',
                        #      'Leucemia Linfoblastica Aguda',
                        #      'Mieloide',
                        #      'Leucemia linfoblástica aguda',
                        #      'Leucemia Mieloide Aguda',
                        #      'Hemofilia',
                        #   ],
                        #  'SNC': ['Depressão',
                        #          'Ansiedade',
                        #          'Antidepressivos',
                        #          'Sindrome do pânico',
                        #          'TDM',
                        #          'Transtorno Depressivo Maior',
                        #          'Depressivo',
                        #          'Ansiosos',
                        #          'Saúde Mental',
                        #          'Doença Mental',
                        #          'Transtorno Mental',
                        #          'Tristeza',
                        #          'Suicídio',
                        #          'Obesidade',
                        #          'Burnout',
                        #          'TAG',
                        #          ],
                        #  'Oncologia': [
                        #      'Câncer',
                        #      'Carcinoma',
                        #      'Neoplasia',
                        #      'Tumor',
                        #      'Imunoterapia',
                        #      'Quimioterapia',
                        #      'Hormonioterapia',
                        #      'Terapia Alvo',
                        #      'Transplante',
                        #      'Medula Óssea',
                        #      'Quinase',
                        #      'Sobrevida',
                        #      'Monoterapia',
                        #      'Câncer de Mama',
                        #      'Câncer de Rim',
                        #      'Câncer de Bexiga',
                        #      'Neoplasia renal',
                        #      'Neoplasia do trato gastrointestinal',
                        #      'Tumores neuroendocrinos de pâncreas',
                        #      'Câncer de Pulmão',
                        #      'Neoplasia pulmonar',
                        #      'Câncer de Pele',
                        #      'Melanoma',
                        #      'Colorretal',
                        #      'Pierre Fabre',
                        #      'Carcinoma de pele'],
                        #  'Reumatologia': [
                        #      'Anticorpo Antidroga',
                        #      'Anticorpo Antidroga Neutralizante',
                        #      'Anticorpo Monoclonal',
                        #      'Artrite',
                        #      'Artrite Idiopática Juvenil',
                        #      'Artrite Idiopática Juvenil Poliarticular',
                        #      'Artrite Psoriásica',
                        #      'Artrite Psoriática',
                        #      'Artrite relacionada a Entesite',
                        #      'Artrite Reumatoide',
                        #      'Biológicos',
                        #      'Congresso Brasileiro de Reumatologia',
                        #      'Doenças imunomediadas',
                        #      'Doenças Inflamatórias',
                        #      'Dor articular',
                        #      'Espondilite Anquilosante',
                        #      'Espondiloartrite Axial não Radiográfica',
                        #      'Fator de Necrose tumoral',
                        #      'Herpes Zoster',
                        #      'Lombalgia',
                        #      'Necrose tumoral',
                        #      'Sociedade Brasileira de Reumatologia',
                        #      'Terapia biológica',
                        #      'Trombose',
                        #      'Tuberculose',

                        # ]

                     }}


# função de busca na API
def apisearch(query: string):
    # Definir os parâmetros de busca
    search_params = {        
        "q": query,
        "lang": "pt",
        "sort_by": "relevancy",
        "page_size": 100
    }

    # Obter as notícias nos últimos 24 horas
    end_date = datetime.utcnow()
    start_date = end_date - timedelta(days=1)
    search_params["from"] = start_date.strftime("%Y-%m-%d")
    search_params["to"] = end_date.strftime("%Y-%m-%d")

    # Adiciona a chave de API da Newscatcher
    # api_key = "BbJk1vWYhB6jK6BJAoR8NFXL-acL8pqF0jdIQbbxI9A>"
    headers = {"x-api-key": "BbJk1vWYhB6jK6BJAoR8NFXL-acL8pqF0jdIQbbxI9A"}

    # Faz a requisição para a  API
    url = "https://api.newscatcherapi.com/v2/search"
    response = requests.get(url, headers=headers, params=search_params)

    return response

#Salva o dataframe na base de dados
def saveInDatabase(dataframe):
    from sqlalchemy import create_engine

    host1 = 'pfizerdb.cqxct8aqwlso.us-east-2.rds.amazonaws.com'
    port1 = '3306'
    user1 = 'pfizer'
    passw1 = 'P!zer2597!'
    database1 = 'pfizerdb'

    mydb1 = create_engine('mysql+pymysql://' + user1 + ':' + passw1 +
                          '@' + host1 + ':' + str(port1) + '/' + database1, echo=False)
    dataframe.to_sql(name='news_covid', con=mydb1,
                     if_exists='append', index=False)


# %%
#Ao final do processo esse dataframe estará com todos dos dados das pesquisas concatenados
df_final = pd.DataFrame()

#Neste bloco, para cada palavra-chave do json, é feito um loop nas categorias
#             e nas áreas de interesse
for i in lista_de_palavras['palavras_chave']:
    print('*** Palavra-chave:'+ i)
    for c in lista_de_palavras['categorias']:
        print('** Categoria: '+ c)
        for k in lista_de_palavras['categorias'][c]:
            str_de_busca = i+' ' + ' "' + k + '"'
            print(str_de_busca) 
            response = apisearch(str_de_busca)
            
            try:
             articles = response.json()["articles"]
             df = pd.DataFrame(articles, columns=[
                "_id", "title", "published_date", "link"])
             df['category'] = 'COVID'
             df['associated'] = c
             df['keyword'] = k

        # Imprimir o dataframe
             df_final = pd.concat([df_final, df], ignore_index=True)
            except:
             continue
#refatora o json, agrupando as notícias duplicadas e adicionando os campos 'keyword' em um único campo, separados por vírgula
df_refatorado = df_final.groupby(["_id", "title", "published_date", "link", "category", "associated"])['keyword'].agg(
    lambda x: ', '.join(x)).reset_index()

#renomeia as colunas
df_refatorado = df_refatorado.rename(
         columns={'published_date': 'published', '_id': 'id', 'keyword':'kw','category':'categoria'})
#df_refatorado.to_excel('teste.xlsx')

#saveInDatabase(df_refatorado)

