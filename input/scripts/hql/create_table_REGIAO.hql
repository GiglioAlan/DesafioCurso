CREATE EXTERNAL TABLE IF NOT EXISTS DESAFIO_CURSO_STG.TBL_REGIAO_STG ( 
        RegionCode string,
        RegionName string
    )
COMMENT 'Tabela externa de REGIAO'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ';'
STORED AS TEXTFILE
location '/datalake/raw/REGIAO/'
TBLPROPERTIES ("skip.header.line.count"="1");

CREATE TABLE IF NOT EXISTS DESAFIO_CURSO.TBL_REGIAO ( 
        RegionName string
    )
PARTITIONED BY (RegionCode string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
TBLPROPERTIES ('orc.compress'='SNAPPY');

SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nostrict;

INSERT OVERWRITE TABLE
    DESAFIO_CURSO.TBL_REGIAO
PARTITION(RegionCode)
SELECT
        RegionName string,
        RegionCode string
FROM DESAFIO_CURSO_STG.TBL_REGIAO_STG
;