CREATE EXTERNAL TABLE IF NOT EXISTS DESAFIO_CURSO_STG.TBL_CLIENTES_STG ( 
        AddressNumber string,
        BusinessFamily string,
        BusinessUnit string,
        Customer string,
        CustomerKey string,
        CustomerType string,
        Division string,
        LineOfBusiness string,
        Phone string,
        RegionCode string,
        RegionalSalesMgr string,
        SearchType string
    )
COMMENT 'Tabela externa de CLIENTES'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ';'
STORED AS TEXTFILE
location '/datalake/raw/CLIENTES/'
TBLPROPERTIES ("skip.header.line.count"="1");

CREATE TABLE IF NOT EXISTS DESAFIO_CURSO.TBL_CLIENTES ( 
        AddressNumber string,
        BusinessUnit string,
        Customer string,
        CustomerKey string,
        CustomerType string,
        Division string,
        LineOfBusiness string,
        Phone string,
        RegionCode string,
        RegionalSalesMgr string,
        SearchType string
    )
PARTITIONED BY (BusinessFamily string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
TBLPROPERTIES ('orc.compress'='SNAPPY');

SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nostrict;

INSERT OVERWRITE TABLE
    DESAFIO_CURSO.TBL_CLIENTES
PARTITION(BusinessFamily)
SELECT
    AddressNumber string,
    BusinessUnit string,
    Customer string,
    CustomerKey string,
    CustomerType string,
    Division string,
    LineOfBusiness string,
    Phone string,
    RegionCode string,
    RegionalSalesMgr string,
    SearchType string,
    BusinessFamily string
FROM DESAFIO_CURSO_STG.TBL_CLIENTES_STG
;