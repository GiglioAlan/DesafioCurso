#!/bin/bash

# Criação das pastas

DADOS=("CLIENTES" "DIVISAO" "ENDERECO" "REGIAO" "VENDAS")

for table in "${DADOS[@]}"
do
	echo "$table"
    cd ../../raw/
    hdfs dfs -mkdir /datalake/raw/$table
    hdfs dfs -chmod 777 /datalake/raw/$table
    hdfs dfs -copyFromLocal $table.csv /datalake/raw/$table
    beeline -u jdbc:hive2://localhost:10000 -f ../../scripts/hql/create_table_$table.hql 
done
