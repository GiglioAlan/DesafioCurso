CREATE EXTERNAL TABLE IF NOT EXISTS DESAFIO_CURSO_STG.TBL_ENDERECO_STG ( 
        AddressNumber string,
        City string,
        Country string,
        CustomerAddress1 string,
        CustomerAddress2 string,
        CustomerAddress3 string,
        CustomerAddress4 string,
        State string,
        ZipCode string
    )
COMMENT 'Tabela externa de ENDERECO'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ';'
STORED AS TEXTFILE
location '/datalake/raw/ENDERECO/'
TBLPROPERTIES ("skip.header.line.count"="1");

CREATE TABLE IF NOT EXISTS DESAFIO_CURSO.TBL_ENDERECO ( 
        AddressNumber string,
        City string,
        CustomerAddress1 string,
        CustomerAddress2 string,
        CustomerAddress3 string,
        CustomerAddress4 string,
        State string,
        ZipCode string
    )
PARTITIONED BY (Country string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
TBLPROPERTIES ('orc.compress'='SNAPPY');

SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nostrict;

INSERT OVERWRITE TABLE
    DESAFIO_CURSO.TBL_ENDERECO
PARTITION(Country)
SELECT
        AddressNumber string,
        City string,
        CustomerAddress1 string,
        CustomerAddress2 string,
        CustomerAddress3 string,
        CustomerAddress4 string,
        State string,
        ZipCode string,
        Country string
FROM DESAFIO_CURSO_STG.TBL_ENDERECO_STG
;