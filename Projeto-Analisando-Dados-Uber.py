#!/usr/bin/env python
# coding: utf-8

# ### *********** Atenção: *********** 
# Utilizado Java JDK 11 e Apache Spark 2.4.2

# # Mini-Projeto 1 - Analisando Dados do Uber com Spark 

# Dataset: https://github.com/fivethirtyeight/uber-tlc-foil-response

# Esse conjunto de dados contém dados de mais de 4,5 milhões de captações Uber na cidade de Nova York de abril a setembro de 2014 e 14,3 milhões de captações Uber de janeiro a junho de 2015. Dados em nível de viagem sobre 10 outras empresas de veículos de aluguel (FHV) bem como dados agregados para 329 empresas de FHV, também estão incluídos. Todos os arquivos foram recebidos em 3 de agosto, 15 de setembro e 22 de setembro de 2015.

# 1- Quantos são e quais são as bases de carros do Uber (onde os carros ficam esperando passageiros)?
# 
# 2- Qual o total de veículos que passaram pela base B02617?
# 
# 3- Qual o total de corridas por base? Apresente de forma decrescente.



from pandas import read_csv


# Criando um objeto Pandas
uberFile = read_csv("data/uber.csv")



type(uberFile)


# Visualizando as primeiras linhas
uberFile.head(10)


# Tranformando o dataframe (Pandas) em um Dataframe (Spark)
uberDF = sqlContext.createDataFrame(uberFile)


type(uberDF)


# Criando o RDD a partir do arquivo csv
uberRDD = sc.textFile("data/uber.csv")


type(uberRDD)


# Total de registros
uberRDD.count()


# Primeiro registro
uberRDD.first()


# Dividindo o arquivo em colunas, separadas pelo caracter ",""
UberRDDLinhas = uberRDD.map(lambda x: x.split(","))
UberRDDLinhas.collect()


type(UberRDDLinhas)


# Número de bases de carros do Uber
UberRDDLinhas.map(lambda x: x[0]).distinct().count() -1


# Bases de carros do Uber
UberRDDLinhas.map(lambda x: x[0]).distinct().collect()


# Total de veículos que passaram pela base B02617
UberRDDLinhas.filter( lambda x: "B02617" in x).count()


# Gravando os dados dos veículos da base B02617 em um novo RDD
tB02617 = UberRDDLinhas.filter( lambda x: "B02617" in x)



# Total de dias em que o número de corridas foi superior a 16.000
tB02617.filter(lambda x: int(x[3]) > 16000).count()


# Dias em que o total de corridas foi superior a 16.000
tB02617.filter(lambda x: int(x[3]) > 16000).collect()


# Criando um novo RDD
uberRDD2 = sc.textFile("data/uber.csv").filter(lambda line: "base" not in line).map(lambda line:line.split(","))


# Aplicando redução para calcular o total por base
uberRDD2.map(lambda kp: (kp[0], int(kp[3])) ).reduceByKey(lambda k,v: k + v).collect()


# Aplicando redução para calcular o total por base, em ordem decrescente
uberRDD2.map(lambda kp: (kp[0], int(kp[3])) ).reduceByKey(lambda k,v: k + v).takeOrdered(10, key = lambda x: -x[1])


# # Fim
