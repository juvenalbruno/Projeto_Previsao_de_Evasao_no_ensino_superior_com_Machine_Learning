#install.packages("dplyr")
#install.packages("sparklyr")
#spark_install("2.3")

library(sparklyr)
library(dplyr)
library(readxl)

sc <- spark_connect(master = 'local', version = '2.3')

name1 = 'alunos_2019'
name2 = 'alunos_2019'
name3 = 'alunos_2019'
name4 = 'alunos_2019'
name5 = 'alunos_2019'
name6 = 'alunos_2019'
name7 = 'alunos_2019'
name8 = 'alunos_2019'
name9 = 'alunos_2019'
name10 = 'alunos_2019'
name11 = 'alunos_2019'

path_Aluno19 = 'C:/Users/bruno/Desktop/projeto/Dados/ALUNO_2019.csv'
path_Aluno18 = 'C:/Users/bruno/Desktop/projeto/Dados/ALUNO_2019.csv'
path_Aluno17 = 'C:/Users/bruno/Desktop/projeto/Dados/ALUNO_2019.csv'
path_Aluno16 = 'C:/Users/bruno/Desktop/projeto/Dados/ALUNO_2019.csv'
path_Aluno15 = 'C:/Users/bruno/Desktop/projeto/Dados/ALUNO_2019.csv'
path_Aluno14 = 'C:/Users/bruno/Desktop/projeto/Dados/ALUNO_2019.csv'
path_Aluno13 = 'C:/Users/bruno/Desktop/projeto/Dados/ALUNO_2019.csv'
path_Aluno12 = 'C:/Users/bruno/Desktop/projeto/Dados/ALUNO_2019.csv'
path_Aluno11 = 'C:/Users/bruno/Desktop/projeto/Dados/ALUNO_2019.csv'
path_Aluno10 = 'C:/Users/bruno/Desktop/projeto/Dados/ALUNO_2019.csv'
path_Aluno09 = 'C:/Users/bruno/Desktop/projeto/Dados/ALUNO_2019.csv'

dados1 = spark_read_csv(sc, name1 = name, header = TRUE, path = path_Aluno19, delimiter = '|', memory = FALSE);
dados2 = spark_read_csv(sc, name2 = name, header = TRUE, path = path_Aluno18, delimiter = '|', memory = FALSE);
dados3 = spark_read_csv(sc, name3 = name, header = TRUE, path = path_Aluno17, delimiter = '|', memory = FALSE);
dados4 = spark_read_csv(sc, name4 = name, header = TRUE, path = path_Aluno16, delimiter = '|', memory = FALSE);
dados5 = spark_read_csv(sc, name5 = name, header = TRUE, path = path_Aluno15, delimiter = '|', memory = FALSE);
dados6 = spark_read_csv(sc, name6 = name, header = TRUE, path = path_Aluno14, delimiter = '|', memory = FALSE);
dados7 = spark_read_csv(sc, name7 = name, header = TRUE, path = path_Aluno13, delimiter = '|', memory = FALSE);
dados8 = spark_read_csv(sc, name8 = name, header = TRUE, path = path_Aluno12, delimiter = '|', memory = FALSE);
dados9 = spark_read_csv(sc, name9 = name, header = TRUE, path = path_Aluno11, delimiter = '|', memory = FALSE);
dados10 = spark_read_csv(sc, name10 = name, header = TRUE, path = path_Aluno10, delimiter = '|', memory = FALSE);
dados11 = spark_read_csv(sc, name11 = name, header = TRUE, path = path_Aluno09, delimiter = '|', memory = FALSE);

tabela_2019 <- dados %>% group_by(CO_IES) %>% filter(CO_IES == 578) %>% sdf_register('tabela_2019')
tabela_2018 <- dados %>% group_by(CO_IES) %>% filter(CO_IES == 578) %>% sdf_register('tabela_2018')
tabela_2017 <- dados %>% group_by(CO_IES) %>% filter(CO_IES == 578) %>% sdf_register('tabela_2017')
tabela_2016 <- dados %>% group_by(CO_IES) %>% filter(CO_IES == 578) %>% sdf_register('tabela_2016')
tabela_2015 <- dados %>% group_by(CO_IES) %>% filter(CO_IES == 578) %>% sdf_register('tabela_2015')
tabela_2014 <- dados %>% group_by(CO_IES) %>% filter(CO_IES == 578) %>% sdf_register('tabela_2014')
tabela_2013 <- dados %>% group_by(CO_IES) %>% filter(CO_IES == 578) %>% sdf_register('tabela_2013')
tabela_2012 <- dados %>% group_by(CO_IES) %>% filter(CO_IES == 578) %>% sdf_register('tabela_2012')
tabela_2011 <- dados %>% group_by(CO_IES) %>% filter(CO_IES == 578) %>% sdf_register('tabela_2011')
tabela_2010 <- dados %>% group_by(CO_IES) %>% filter(CO_IES == 578) %>% sdf_register('tabela_2010')
tabela_2009 <- dados %>% group_by(CO_IES) %>% filter(CO_IES == 578) %>% sdf_register('tabela_2009')

write.csv(tabela_2019, 'Alunos_2019.csv')
write.csv(tabela_2018, 'Alunos_2018.csv')
write.csv(tabela_2017, 'Alunos_2017.csv')
write.csv(tabela_2016, 'Alunos_2016.csv')
write.csv(tabela_2015, 'Alunos_2015.csv')
write.csv(tabela_2014, 'Alunos_2014.csv')
write.csv(tabela_2013, 'Alunos_2013.csv')
write.csv(tabela_2012, 'Alunos_2012.csv')
write.csv(tabela_2011, 'Alunos_2011.csv')
write.csv(tabela_2010, 'Alunos_2010.csv')
write.csv(tabela_2009, 'Alunos_2009.csv')
