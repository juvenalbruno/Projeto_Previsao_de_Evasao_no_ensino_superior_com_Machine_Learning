
### CODIGO_DE_IDENTIFICACAO_DA_UFBA 
### VARIAVEL CO_IES
### 578

library(sparklyr);
library(dplyr);

IES_db = 'C:/Users/bruno/Desktop/projeto/Dados_Ensino_Superior/Microdados_Educação_Superior_2019/dados/SUP_IES_2019.csv';

IES_dados = spark_read_csv(sc, name = 'ies_2019', header = TRUE, path = IES_db, delimiter = '|', memory = FALSE);

tabela2 <- dados2 %>%
           group_by(SG_IES) %>%
           filter(SG_IES == 'UFBA') %>%
           sdf_register('tabela')

tabela2