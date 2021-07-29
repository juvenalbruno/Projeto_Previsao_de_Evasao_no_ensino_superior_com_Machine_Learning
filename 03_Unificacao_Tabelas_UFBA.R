name1 = 'alunos_2019'
name2 = 'alunos_2018'
name3 = 'alunos_2017'
name4 = 'alunos_2016'
name5 = 'alunos_2015'
name6 = 'alunos_2014'
name7 = 'alunos_2013'
name8 = 'alunos_2012'
name9 = 'alunos_2011'
name10 = 'alunos_2010'

path_Aluno19 = 'C:/Users/bruno/Desktop/projeto/Tabelas_Alunos_UFBA_Anos/Alunos_2019.csv'
path_Aluno18 = 'C:/Users/bruno/Desktop/projeto/Tabelas_Alunos_UFBA_Anos/Alunos_2018.csv'
path_Aluno17 = 'C:/Users/bruno/Desktop/projeto/Tabelas_Alunos_UFBA_Anos/Alunos_2017.csv'
path_Aluno16 = 'C:/Users/bruno/Desktop/projeto/Tabelas_Alunos_UFBA_Anos/Alunos_2016.csv'
path_Aluno15 = 'C:/Users/bruno/Desktop/projeto/Tabelas_Alunos_UFBA_Anos/Alunos_2015.csv'
path_Aluno14 = 'C:/Users/bruno/Desktop/projeto/Tabelas_Alunos_UFBA_Anos/Alunos_2014.csv'
path_Aluno13 = 'C:/Users/bruno/Desktop/projeto/Tabelas_Alunos_UFBA_Anos/Alunos_2013.csv'
path_Aluno12 = 'C:/Users/bruno/Desktop/projeto/Tabelas_Alunos_UFBA_Anos/Alunos_2012.csv'
path_Aluno11 = 'C:/Users/bruno/Desktop/projeto/Tabelas_Alunos_UFBA_Anos/Alunos_2011.csv'
path_Aluno10 = 'C:/Users/bruno/Desktop/projeto/Tabelas_Alunos_UFBA_Anos/Alunos_2010.csv'

dados19 = spark_read_csv(sc, name1 = name, header = TRUE, path = path_Aluno19, delimiter = '|', memory = FALSE);
dados18 = spark_read_csv(sc, name2 = name, header = TRUE, path = path_Aluno18, delimiter = '|', memory = FALSE);
dados17 = spark_read_csv(sc, name3 = name, header = TRUE, path = path_Aluno17, delimiter = '|', memory = FALSE);
dados16 = spark_read_csv(sc, name4 = name, header = TRUE, path = path_Aluno16, delimiter = '|', memory = FALSE);
dados15 = spark_read_csv(sc, name5 = name, header = TRUE, path = path_Aluno15, delimiter = '|', memory = FALSE);
dados14 = spark_read_csv(sc, name6 = name, header = TRUE, path = path_Aluno14, delimiter = '|', memory = FALSE);
dados13 = spark_read_csv(sc, name7 = name, header = TRUE, path = path_Aluno13, delimiter = '|', memory = FALSE);
dados12 = spark_read_csv(sc, name8 = name, header = TRUE, path = path_Aluno12, delimiter = '|', memory = FALSE);
dados11 = spark_read_csv(sc, name9 = name, header = TRUE, path = path_Aluno11, delimiter = '|', memory = FALSE);
dados10 = spark_read_csv(sc, name10 = name, header = TRUE, path = path_Aluno10, delimiter = '|', memory = FALSE);

tabela_2019 <- dados19
tabela_2018 <- dados18
tabela_2017 <- dados17
tabela_2016 <- dados16
tabela_2015 <- dados15
tabela_2014 <- dados14
tabela_2013 <- dados13
tabela_2012 <- dados12
tabela_2011 <- dados11
tabela_2010 <- dados10

tabela_ufba_unificada = sdf_bind_rows( dados19, dados18, dados17, dados16, dados15, dados14, dados13, dados12, dados11, dados10)
write.csv(tabela_ufba_unificada, 'Tabela_Alunos_UFBA_Unificada.csv')

tabela_19_18 = sdf_bind_rows( dados19, dados18)
tabela_19_18
tabela_19_a_17 = sdf_bind_rows( tabela_19_18, dados17)
tabela_19_a_17
tabela_19_a_16 = sdf_bind_rows( tabela_19_a_17, dados16)
tabela_19_a_16
tabela_19_a_15 = sdf_bind_rows( tabela_19_a_16, dados15)
tabela_19_a_15
tabela_19_a_14 = sdf_bind_rows( tabela_19_a_15, dados14)
tabela_19_a_14
tabela_19_a_13 = sdf_bind_rows( tabela_19_a_14, dados13)
tabela_19_a_13
tabela_19_a_12 = sdf_bind_rows( tabela_19_a_13, dados12)
tabela_19_a_12
tabela_19_a_11 = sdf_bind_rows( tabela_19_a_12, dados11)
tabela_19_a_11
tabela_19_a_10 = sdf_bind_rows( tabela_19_a_11, dados10)
tabela_19_a_10


write.csv(tabela_19_a_10, 'Tabela_Alunos_UFBA_Unificada.csv')

