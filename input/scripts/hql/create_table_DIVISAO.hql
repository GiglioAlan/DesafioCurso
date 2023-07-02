CREATE EXTERNAL TABLE IF NOT EXISTS DESAFIO_CURSO_STG.TBL_DIVISAO_STG ( 
        Division string,
        DivisionName string
    )
COMMENT 'Tabela externa de DIVISAO'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ';'
STORED AS TEXTFILE
location '/datalake/raw/DIVISAO/'
TBLPROPERTIES ("skip.header.line.count"="1");

CREATE TABLE IF NOT EXISTS DESAFIO_CURSO.TBL_DIVISAO ( 
        DivisionName string
    )
PARTITIONED BY (Division string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
TBLPROPERTIES ('orc.compress'='SNAPPY');

SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nostrict;

INSERT OVERWRITE TABLE
    DESAFIO_CURSO.TBL_DIVISAO
PARTITION(Division)
SELECT
    DivisionName string,
    Division string
FROM DESAFIO_CURSO_STG.TBL_DIVISAO_STG
;