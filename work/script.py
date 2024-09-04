# %%
### Contabilizar os acessos para definir o valor de cada medalha para o Relatório
### BIBLIOTECAS
# Importa bibliotecas
from pymongo import MongoClient
import sqlalchemy
import pandas  as pd
from pandas import json_normalize
import json
import numpy as np
import datetime as dt
from datetime import datetime, timedelta, date, timezone
from time import sleep
import sys
import os
import warnings
import matplotlib.pyplot as plt
import seaborn as sns
#from funcoes import calcular_estatistica_descritiva, identificar_outliers, remover_outliers, somar_acessos, definir_grupos

### DIRETÓRIOS
# Definição de diretórios
diretorio_principal = os.getcwd()
diretorio_script = os.path.join(diretorio_principal, "Relatório medalhas")

### COLETA DOS DADOS
# Cria conexão com Banco de dados DBeaver: Plataforma e Portal
print('Conectando aos bancos de dados')
try:
    engine_postgres = sqlalchemy.create_engine("postgresql://postgres:37973a49-fbf4-410f-afc6-54fb8de7cfc8@192.168.100.14/plataforma_doutorie")
    engine_mariadb = sqlalchemy.create_engine("mysql+pymysql://marombe:z8eWgN68tmmvx976UEv@192.168.200.15:3306/portal")
    log = 'MariaDB e Postgres funcionando normalmente'
    print(log)
    sleep(3)
except:
    try:
        engine_mariadb = sqlalchemy.create_engine("mysql://marombe:z8eWgN68tmmvx976UEv@192.168.200.15:3306/portal")
        log = 'Apenas MariaDB em funcionamento! Contatar a equipe de software para backup do Postgres'
        print(log)
        sleep(5)
    except:
        log = 'MariaDB e Postgres offline! Contatar a equipe de software'
        print(log)
        sleep(10)
        sys.exit

# Cria tabela com informações de clientes pagantes - Banco de Dados Maria: Portal
print('Coletando informações clientes pagantes')
query = "SELECT id as u_id, name, cnpj, group_id, status FROM subscriber WHERE account_type in (3, 5) AND status = 9"
u_id_df = pd.read_sql(query, engine_mariadb)
u_id_df.replace('', np.nan, inplace=True)
u_id_df = u_id_df.dropna()
# Transformar id dos clientes em lista
u_id_list = u_id_df['u_id'].tolist()
# Selecionando clientes antigos pagantes
print('Criando tabela com clientes antigos pagantes')
query = """
SELECT
    subs.id "u_id",
    subs.name "name",
    subs.account_type "account_type",
    subs.status "status"
FROM
    subscriber subs
WHERE
    subs.status IN (9)
AND
    subs.account_type in (3,5)
AND
    subs.id BETWEEN 1 AND 10000;
"""
# Transformar query em DataFrame
df_clientes_antigos = pd.read_sql(query, engine_mariadb)
# Transformar id dos clientes em lista
u_id_antigos = df_clientes_antigos['u_id'].tolist()
# Selecionando clientes meio antigos pagantes
print('Criando tabela com clientes meio antigos pagantes')
query = """
SELECT
    subs.id "u_id",
    subs.name "name",
    subs.account_type "account_type",
    subs.status "status"
FROM
    subscriber subs
WHERE
    subs.status IN (9)
AND
    subs.account_type in (3,5)
AND
    subs.id BETWEEN 10000 AND 20000;
"""
# Transformar query em DataFrame
df_clientes_meio_antigos = pd.read_sql(query, engine_mariadb)
# Transformar id dos clientes em lista
u_id_meio_antigos = df_clientes_meio_antigos['u_id'].tolist()
# Selecionando clientes meio novos pagantes
print('Criando tabela com clientes meio novos pagantes')
query = """
SELECT
    subs.id "u_id",
    subs.name "name",
    subs.account_type "account_type",
    subs.status "status"
FROM
    subscriber subs
WHERE
    subs.status IN (9)
AND
    subs.account_type in (3,5)
AND
    subs.id BETWEEN 20000 AND 50000;
"""
# Transformar query em DataFrame
df_clientes_meio_novos = pd.read_sql(query, engine_mariadb)
# Transformar id dos clientes em lista
u_id_meio_novos = df_clientes_meio_novos['u_id'].tolist()
# Selecionando clientes novos pagantes
print('Criando tabela com clientes novos pagantes')
query = """
SELECT
    subs.id "u_id",
    subs.name "name",
    subs.account_type "account_type",
    subs.status "status"
FROM
    subscriber subs
WHERE
    subs.status IN (9)
AND
    subs.account_type in (3,5)
AND
    subs.id >= 50000;
"""
# Transformar query em DataFrame
df_clientes_novos = pd.read_sql(query, engine_mariadb)
# Extraindo os u_id
u_id_novos = df_clientes_novos['u_id'].tolist()

# Solicitação para adicionar data inicial e data final
count = 0
while count == 0:
    try:
        data_inicial_str = input("Entre com a data inicial (dd/mm/aaaa): ")
        data_final_str = input("Entre com a data final (dd/mm/aaaa): ")
        # Converter strings de data para datetime
        data_inicial = datetime.strptime(data_inicial_str, "%d/%m/%Y")
        data_final = datetime.strptime(data_final_str, "%d/%m/%Y")
        # Converter para o formato UTC se necessário
        data_inicial_utc = data_inicial.replace(tzinfo=timezone.utc)
        data_final_utc = data_final.replace(tzinfo=timezone.utc)
        count = 1
    except ValueError:
        print("Erro inesperado. Por favor, tente novamente.")

# Função de busca a acessos no Banco de Dados Mongo
def busca_acessos(codigo):
    print('MongoDB funcionando totalmente!')
    client = MongoClient('mongodb://root:37973a49-fbf4-410f-afc6-54fb8de7cfc8@192.168.100.14:27017/')
    filter = {
        'cod': int(codigo),
        'u_id': {'$in': u_id_list},
        'h_utc': {'$gte': data_inicial_utc, '$lte': data_final_utc}
    }
    result = client['api']['logs'].find(filter=filter)
    return result

## FUNÇÕES
# Calcula a Média, Desvio padrão e Coeficiente de variação
def calcular_estatistica_descritiva(df, coluna):
    """
    Esta função calcula a média, desvio padrão e coeficiente de variação dos acessos.
    Args:
    df: o Dataframe original com os valores que precisam ser calculados.
    coluna: a coluna que deve ser usada para fazer o cálculo.
    Return:
    Variáveis com os valores de cada medida calculada.
    """
    media = np.mean(df[coluna])
    desvio_padrao = np.std(df[coluna])
    coeficiente_variacao = np.std(df[coluna])/np.mean(df[coluna])
    return media, desvio_padrao, coeficiente_variacao

# Z score (identifica os outliers)
def identificar_outliers(df, coluna):
    """
    Esta função identifica o índice dos outliers.
    Args:
    df: o DataFrame original onde os outliers serão removidos.
    coluna: coluna onde estão os outliers.
    Return:
    Lista com os índices dos outliers.
    """
    outliers = (df[coluna] - np.mean(df[coluna]))/np.std(df[coluna])
    indice_outliers = np.where((outliers >= 3)|(outliers <= - 3))
    return indice_outliers

# Função para remover outliers com base em u_id
def remover_outliers(df1, df2, index_outliers):
    """
    Esta função identifica o u_id a partir da lista criada na função de identificar outliers e
    remove os outliers a partir do u_id.
    Args:
    df1: DataFrame que contém os acessos e a coluna h_tz.
    df2: DataFrame que contém o índice dos outliers.
    index_outliers: Lista com o índice dos outliers.
    Return:
    DataFrame sem os outliers.
    """
    u_id_outliers = df2.iloc[index_outliers]['u_id'].values
    df1 = df1[~df1['u_id'].isin(u_id_outliers)]
    return df1

# Função para somar acessos dos períodos delimitados
def somar_acessos(df, colunas_para_somar):
    """
    Esta função soma os períodos delimitados para o projeto: 60 dias, 90 dias, 120 dias e 180 dias.
    Args:
    df: o DataFame original onde consta as colunas a serem somadas.
    colunas_para_somar: consta as colunas como devem ser somadas.
    Return:
    DataFrame com as colunas originais e as colunas com as somas dos períodos estabelecidos.
    """
    for col_nova, *colunas in colunas_para_somar:
        df[col_nova] = df[colunas].sum(axis=1)
    return df
colunas_para_somar = [
    ('60_dias', '2024-01', '2024-02'),
    ('90_dias', '2024-01', '2024-02', '2024-03'),
    ('120_dias', '2024-01', '2024-02', '2024-03', '2024-04'),
    ('180_dias', '2024-01', '2024-02', '2024-03', '2024-04', '2024-05', '2024-06')
]

# Função que adiciona uma coluna para diferenciar grupos
def definir_grupos(df):
    """
    Essa função adiciona uma coluna ao DataFrame para diferenciar os grupos.
    Args:
    df: DataFrame onde será aplicada a função.
    Return:
    DataFrame original com a nova coluna 'grupo'
    """
    df['grupo'] = np.where(df['u_id'].isin(df_clientes_novos['u_id']), 'A',
                      np.where(df['u_id'].isin(df_clientes_meio_novos['u_id']), 'B',
                      np.where(df['u_id'].isin(df_clientes_meio_antigos['u_id']), 'C',
                      np.where(df['u_id'].isin(df_clientes_antigos['u_id']), 'D', 'None'))))
    return df

# %%
def main(codigo, categoria):
# Função de busca a acessos no Banco de Dados Mongo
    def busca_acessos(codigo):
        client = MongoClient('mongodb://root:37973a49-fbf4-410f-afc6-54fb8de7cfc8@192.168.100.14:27017/')
        print(client, 'MongoDB funcionando totalmente!')
        filter = {
            'cod': int(codigo),
            'u_id': {'$in': u_id_list},
            'h_utz': {'$gte': '2024-01-201', '$lte': '2024-07-01'}
        }
        result = client['api']['logs'].find(filter=filter)
        return result
    
    # Coleta de livros acessados na Plataforma no Banco de Dados Mongo COD: 1007
    print('Coletando dados')
    try:
        df_teste = busca_acessos(codigo)
        log = 'MongoDB funcionando normalmente.'
    except:
        log = 'MongoDB offline! Verificar'
        print(log)
        sleep(3)
    df_teste = pd.DataFrame(df_teste)
    if len(df_teste)==0:
        print('Nenhum dado encontrado no período definido! fechando programa.')
        sleep(3)
        sys.exit

    ### TRATAMENDO DOS DADOS
    # Selecionar colunas desejadas
    df_acessos = df_acessos[['h_tz','u_id']]
    # Converte coluna 'h_tz' em datetime
    df_acessos['h_tz'] = pd.to_datetime(df_acessos['h_tz'])
    # print(f"Tratamento dos dados feito")

    ### FILTROS
    # Filtrar clientes pagantes
    # Remove colaborador que está como cliente pagante
    df_acessos = df_acessos[df_acessos['u_id'] != 47337]
    df_acessos = df_acessos.reset_index(drop = True)

    ### REMOÇÃO DE OUTLIERS
    # Converte a coluna 'h_tz' em datetime
    df_acessos['h_tz'] = df_acessos['h_tz'].dt.strftime('%m/%Y')
    # Agrupa por 'u_id' e conta o número de ocorrências para identificar outliers
    df_mes_u_id = df_acessos.groupby('u_id').size().reset_index(name='acessos')
    # Aplica a função para calcular a Média, Desvio padrão e Coeficiente de variação
    media_1, desvio_padrao_1, coeficiente_variacao_1 = calcular_estatistica_descritiva(df_mes_u_id, 'acessos')
    print(f"Média: {media_1}")
    print(f"Desvio padrão: {desvio_padrao_1}")
    print(f"Coeficiente de variação: {coeficiente_variacao_1}")
    # Aplica a função Z score (identifica os outliers)
    index_outliers = identificar_outliers(df_mes_u_id, 'acessos')
    # Aplica a função para remover outliers do DataFrame original
    df_sem_outliers = remover_outliers(df_acessos, df_mes_u_id, index_outliers)
    print(f"Outliers removidos")
    # Agrupa por 'u_id' e conta o número de ocorrências para VERIFICAR SE OS OUTLIERS FORAM REMOVIDOS
    df_graf_sem_outliers = df_sem_outliers.groupby('u_id').size().reset_index(name='acessos_180_dias')
    # Verificar média após a remoção dos outliers
    df_graf_sem_outliers.describe()
    # Total de clientes após a remoção dos outliers
    print(f"""Total de clientes únicos após a remoção dos outliers: {df_sem_outliers['u_id'].nunique()}
    clientes novos (Grupo A): {df_sem_outliers[df_sem_outliers['u_id'].isin(df_clientes_novos['u_id'])]['u_id'].nunique()}
    clientes meio novos (Grupo B): {df_sem_outliers[df_sem_outliers['u_id'].isin(df_clientes_meio_novos['u_id'])]['u_id'].nunique()}
    clientes meio antigos (Grupo C): {df_sem_outliers[df_sem_outliers['u_id'].isin(df_clientes_meio_antigos['u_id'])]['u_id'].nunique()}
    clientes antigos (Grupo D): {df_sem_outliers[df_sem_outliers['u_id'].isin(df_clientes_antigos['u_id'])]['u_id'].nunique()}
    """)

    ### CALCULANDO ACESSOS
    # Altera o formato da coluna com data e agrupa por data e u_id
    df_soma_acessos = df_sem_outliers.groupby(['h_tz', 'u_id']).size().reset_index(name='acessos')
    # Cria tabela dinâmica para alterar colunas e índice
    df_pivot = df_soma_acessos.pivot(index='u_id', columns='h_tz', values='acessos')
    df_pivot.columns = df_pivot.columns.astype(str).str.strip()
    df_pivot = df_pivot.reset_index()
    df_pivot.index.name = None
    df_pivot.columns.name = None
    df_pivot
    df_pivot = df_pivot.fillna(0)
    df_pivot = df_pivot.astype(int)
    # Aplica a função de somar acessos
    df_somar_periodos = somar_acessos(df_pivot, colunas_para_somar)
    # Manter colunas desejadas
    df_somar_periodos = df_somar_periodos[['u_id','2024-01','60_dias','90_dias','120_dias','180_dias']]
    df_somar_periodos = df_somar_periodos.rename(columns={'2024-01':'30_dias'})
    print(f"Acessos somados")

    ### FILTROS
    # Aplica a função que adiciona uma coluna para diferenciar grupos
    df_somar_periodos = definir_grupos(df_somar_periodos)
    ### AMOSTRAGEM PROPORCIONAL
    # Identifica e ordena clientes novos que mais acessam
    df_clientes_novos_filtrados = df_pivot[df_pivot['u_id'].isin(u_id_novos)]
    lista_top_u_id_novos = df_clientes_novos_filtrados['u_id']
    # Identifica e ordena clientes meio novos que mais acessam
    df_clientes_meio_novos_filtrados = df_pivot[df_pivot['u_id'].isin(u_id_meio_novos)]
    lista_top_u_id_meio_novos = df_clientes_meio_novos_filtrados['u_id']
    # Identifica e ordena clientes meio antigos que mais acessam
    df_clientes_meio_antigos_filtrados = df_pivot[df_pivot['u_id'].isin(u_id_meio_antigos)]
    lista_top_u_id_meio_antigos = df_clientes_meio_antigos_filtrados['u_id']
    # Identifica e ordena clientes antigos que mais acessam
    df_clientes_antigos_filtrados = df_pivot[df_pivot['u_id'].isin(u_id_antigos)]
    lista_top_u_id_antigos = df_clientes_antigos_filtrados['u_id']
    # Cria um DataFrame para cada grupo filtrado
    df_antigos = df_somar_periodos[df_somar_periodos['u_id'].isin(lista_top_u_id_antigos)]
    df_meio_antigos = df_somar_periodos[df_somar_periodos['u_id'].isin(lista_top_u_id_meio_antigos)]
    df_meio_novos = df_somar_periodos[df_somar_periodos['u_id'].isin(lista_top_u_id_meio_novos)]
    df_novos = df_somar_periodos[df_somar_periodos['u_id'].isin(lista_top_u_id_novos)]
    print(f"Filtro dos grupos aplicado")

    # Concatena os Dataframes de cada grupo
    df_concatenado = pd.concat([df_novos, df_meio_novos, df_meio_antigos, df_antigos], axis=0)
    df_concatenado = df_concatenado.drop(columns=('u_id'))
    df_concatenado = df_concatenado.reset_index(drop=True)
    df_concatenado = df_concatenado.sort_values(by='180_dias', ascending=False).head(20)
    print(f"DataFrame finalizado")

    # Calcula a média por grupo
    media_1 = df_concatenado.loc[df_concatenado['grupo'].str.contains('A'), 'valor'].mean()
    media_2 = df_concatenado.loc[df_concatenado['grupo'].str.contains('B'), 'valor'].mean()
    media_3 = df_concatenado.loc[df_concatenado['grupo'].str.contains('C'), 'valor'].mean()
    media_4 = df_concatenado.loc[df_concatenado['grupo'].str.contains('D'), 'valor'].mean()

    # Aplica a função para converter DataFrame em arquivo excel
    df_concatenado.to_excel(f'{categoria}.xlsx', index=False)

if __name__ == "__main__":
    codigos = {
        1001: "chassi_buscado",
        1007: "livro_acessado",
        1008: "busca_no_veiculo",
        1009: "busca_no_livro",
        1041: "liberacao_acesso_temporario",
        1061: "chat_solicitado",
        1085: "transcricao_audio"
    }

    for categoria, codigo in codigos.items():
        main(codigo, categoria)