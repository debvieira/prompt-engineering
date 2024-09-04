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