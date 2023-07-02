CREATE EXTERNAL TABLE IF NOT EXISTS DESAFIO_CURSO_STG.TBL_VENDAS_STG ( 
        ActualDeliveryDate string,
        CustomerKey string,
        DateKey string,
        DiscountAmount string,
        InvoiceDate string,
        InvoiceNumber string,
        ItemClass string,
        ItemNumber string,
        Item string,
        LineNumber string,
        ListPrice string,
        OrderNumber string,
        PromisedDeliveryDate string,
        SalesAmount string,
        SalesAmountBasedOnListPrice string,
        SalesCostAmount string,
        SalesMarginAmount string,
        SalesPrice string,
        SalesQuantity string,
        SalesRep string,
        UM string
    )
COMMENT 'Tabela externa de VENDAS'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ';'
STORED AS TEXTFILE
location '/datalake/raw/VENDAS/'
TBLPROPERTIES ("skip.header.line.count"="1");

CREATE TABLE IF NOT EXISTS DESAFIO_CURSO.TBL_VENDAS ( 
        ActualDeliveryDate string,
        CustomerKey string,
        DateKey string,
        DiscountAmount string,
        InvoiceDate string,
        InvoiceNumber string,
        ItemClass string,
        ItemNumber string,
        Item string,
        LineNumber string,
        ListPrice string,
        OrderNumber string,
        PromisedDeliveryDate string,
        SalesAmount string,
        SalesAmountBasedOnListPrice string,
        SalesCostAmount string,
        SalesMarginAmount string,
        SalesPrice string,
        SalesQuantity string,
        SalesRep string        
    )
PARTITIONED BY (UM string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
TBLPROPERTIES ('orc.compress'='SNAPPY');

SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nostrict;

INSERT OVERWRITE TABLE
    DESAFIO_CURSO.TBL_VENDAS
PARTITION(UM)
SELECT
    ActualDeliveryDate string,
    CustomerKey string,
    DateKey string,
    DiscountAmount string,
    InvoiceDate string,
    InvoiceNumber string,
    ItemClass string,
    ItemNumber string,
    Item string,
    LineNumber string,
    ListPrice string,
    OrderNumber string,
    PromisedDeliveryDate string,
    SalesAmount string,
    SalesAmountBasedOnListPrice string,
    SalesCostAmount string,
    SalesMarginAmount string,
    SalesPrice string,
    SalesQuantity string,
    SalesRep string,
    UM string    
FROM DESAFIO_CURSO_STG.TBL_VENDAS_STG
;