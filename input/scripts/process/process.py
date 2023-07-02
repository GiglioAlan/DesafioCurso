import argparse
import json
from typing import Dict, Tuple, Any
from pyspark.sql import SparkSession, DataFrame, HiveContext
from pyspark.sql.types import StructType, StructField, DoubleType, IntegerType, StringType
from pyspark.sql.functions import *
from pyspark.sql import functions as f
import importlib
import pandas
import os
import re


spark = SparkSession.builder.master("local[*]")\
    .enableHiveSupport()\
    .getOrCreate()

# Criando dataframes diretamente do Hive
df_clientes = spark.sql("select * from desafio_curso.tbl_clientes")
df_base_divisao = spark.sql("select * from desafio_curso.tbl_divisao")
df_base_endereco = spark.sql("select * from desafio_curso.tbl_endereco")
df_base_regiao = spark.sql("select * from desafio_curso.tbl_regiao")
df_base_vendas = spark.sql("select * from desafio_curso.tbl_vendas")

#Função pra validar se existe campo vazio na tabela, retorna um booleano
def tratar_campos_vazios(texto):
    return texto.isspace() or texto == ""

tratar_campos_vazios_udf = udf(tratar_campos_vazios, StringType())


# Substituindo valores vazios na tbl_clientes
df_base_clientes = df_base_clientes.withColumn("lineofbusiness", when(tratar_campos_vazios_udf('lineofbusiness') == True, "Não informado").otherwise(df_base_clientes.lineofbusiness))
#Criando chaveamento para criação da dimensão clientes
df_base_clientes = df_base_clientes.withColumn("DW_CLIENTES", sha2(concat_ws("",df_base_clientes.customer, df_base_clientes.customerkey), 256))

#Substituindo valores vazios na tbl_endereco
df_base_endereco = df_base_endereco.withColumn("city", when(tratar_campos_vazios_udf('city') == True, "Não informado").otherwise(df_base_endereco.city))
df_base_endereco = df_base_endereco.withColumn("customeraddress2", when(tratar_campos_vazios_udf('customeraddress2') == True, "Não informado").otherwise(df_base_endereco.customeraddress2))
df_base_endereco = df_base_endereco.withColumn("customeraddress3", when(tratar_campos_vazios_udf('customeraddress3') == True, "Não informado").otherwise(df_base_endereco.customeraddress3))
df_base_endereco = df_base_endereco.withColumn("customeraddress4", when(tratar_campos_vazios_udf('customeraddress4') == True, "Não informado").otherwise(df_base_endereco.customeraddress4))
df_base_endereco = df_base_endereco.withColumn("state", when(tratar_campos_vazios_udf('state') == True, "Não informado").otherwise(df_base_endereco.state))
df_base_endereco = df_base_endereco.withColumn("zipcode", when(tratar_campos_vazios_udf('zipcode') == True, "Não informado").otherwise(df_base_endereco.zipcode))
#Criando chaveamento para criação da dimensão localidade
df_base_endereco = df_base_endereco.withColumn("DW_LOCALIDADE", sha2(concat_ws("", df_base_endereco.city, df_base_endereco.state, df_base_endereco.country), 256))


#exclusão de linhas na tbl_vendas, haviam 253 linhas totalmente em branco, então preencher com 0 (zero) no caso 
#de decimais ou inteiros e "Não informado" em caso de string achei irrelevante.
df_base_vendas = df_base_vendas.dropna()

#Substituindo valores vazios na tbl_vendas
df_base_vendas = df_base_vendas.withColumn("discountamount", when(tratar_campos_vazios_udf('discountamount') == True, "0").otherwise(df_base_vendas.discountamount))
df_base_vendas = df_base_vendas.withColumn("itemclass", when(tratar_campos_vazios_udf('itemclass') == True, "Não informado").otherwise(df_base_vendas.itemclass))
df_base_vendas = df_base_vendas.withColumn("itemnumber", when(tratar_campos_vazios_udf('itemnumber') == True, "Não informado").otherwise(df_base_vendas.itemnumber))
df_base_vendas = df_base_vendas.withColumn("salesprice", when(tratar_campos_vazios_udf('salesprice') == True, "0").otherwise(df_base_vendas.salesprice))
#Criando chaveamento para criação da dimensão tempo
df_base_vendas = df_base_vendas.withColumn("DW_TEMPO", sha2(concat_ws("", df_base_vendas.invoicedate, df_base_vendas.dia, df_base_vendas.mes, df_base_vendas.ano, df_base_vendas.trimestre), 256))


#Tratar campo invoiceDate para data ao invés de String
df_base_vendas = df_base_vendas.withColumn("invoicedate", to_date("invoicedate", "dd/MM/yyyy"))

#Tratando data individualmente
df_base_vendas = (df_base_vendas
                  .withColumn('ano', year(df_base_vendas.invoicedate))
                  .withColumn('mes', month(df_base_vendas.invoicedate))
                  .withColumn('dia', dayofmonth(df_base_vendas.invoicedate))
                  .withColumn('trimestre', quarter(df_base_vendas.invoicedate))
                 )

  
#Tirando as duplicadas para criação da DIMENSÃO_LOCALIDADE
dim_localidade = df_base_endereco.dropDuplicates(["DW_LOCALIDADE","city","state","country"])
#criando a dimensão localidade
dim_localidade = dim_localidade.select(["DW_LOCALIDADE","city","state","country"])


#Tirando as duplicadas para criação da DIMENSÃO_TEMPO
dim_tempo = df_base_vendas.dropDuplicates(["DW_TEMPO", "invoicedate", "dia", "mes", "ano", "trimestre"])
#criando a dimensão tempo
dim_tempo = dim_tempo.select("DW_TEMPO", "invoicedate", "dia", "mes", "ano", "trimestre")


#Tirando as duplicadas para criação da DIMENSÃO_CLIENTES
dim_clientes = df_base_clientes.dropDuplicates(["DW_CLIENTES", "customer", "customerkey"])
#criando a dimensão cliente
dim_clientes = dim_clientes.select("DW_CLIENTES", "customer", "customerkey")



# criando o fato
ft_vendas = []



# função para salvar os dados
def salvar_df(df, file):
    output = "/input/desafio_hive/gold/" + file
    erase = "hdfs dfs -rm " + output + "/*"
    rename = "hdfs dfs -get /datalake/gold/"+file+"/part-* /input/desafio_hive/gold/"+file+".csv"
    print(rename)
    
    
    df.coalesce(1).write\
        .format("csv")\
        .option("header", True)\
        .option("delimiter", ";")\
        .mode("overwrite")\
        .save("/datalake/gold/"+file+"/")

    os.system(erase)
    os.system(rename)

salvar_df(dim_clientes, 'dimclientes')