#install.packages("dplyr")
#install.packages("sparklyr")
#spark_install("2.3")

library(sparklyr)
library(dplyr)
library(readxl)

sc <- spark_connect(master = 'local', version = '2.3')

name = 'microdados_alunos_2019'

path_db = 'C:/Users/bruno/Desktop/projeto/Dados_Ensino_Superior/Microdados_Educação_Superior_2019/dados/SUP_ALUNO_2019.csv'

dados = spark_read_csv(sc, name = name, header = TRUE, path = path_db, delimiter = '|', memory = FALSE)

tabela <- dados %>%
          group_by(CO_IES) %>%
          filter(CO_IES == 578) %>%
          sdf_register('tabela')

tabela
