library(sparklyr)
library(dplyr)
library(readxl)

#Variavel -> CO_CURSO

sc <- spark_connect(master = 'local', version = '2.3')

name1 = 'Cursos_2019'
Curso = 13277

path_Aluno19 = 'C:/Users/bruno/Desktop/projeto/Dados/ALUNO_2019.csv'

dados1 = spark_read_csv(sc, name1 = name, header = TRUE, path = path_Aluno19, delimiter = '|', memory = FALSE);

tabelaCurso_2019 <- dados1 %>% group_by(CO_IES, CO_CURSO) %>% filter(CO_CURSO = 578, CO_CURSO == Curso) %>% sdf_register('tabelaCurso_2019')

count(tabelaCurso_2019)

write.csv(tabela_2019, 'Cursos_2019.csv')
