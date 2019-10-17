#!/usr/bin/env python
# coding: utf-8



# ### *********** Atenção: *********** 
# Utilize Java JDK 1.8 ou 11 e Apache Spark 2.4.2

# ****** Caso receba mensagem de erro "name 'sc' is not defined", interrompa o pyspark e apague o diretório metastore_db no mesmo diretório onde está este Jupyter notebook ******

# ## <font color='blue'>Spark MLLib - Sistema de Recomendação</font>

# <strong> Descrição </strong>
# <ul style="list-style-type:square">
#   <li>Também chamado de filtros colaborativos.</li>
#   <li>Analisa dados passados para compreender comportamentos de pessoas/entidades.</li>
#   <li>A recomendação é feita por similaridade de comportamento.</li>
#   <li>Recomendação baseada em usuários ou items.</li>
#   <li>Algoritmos de Recomendação esperam receber os dados em um formato específico: [user_ID, item_ID, score].</li>
#   <li>Score, também chamado rating, indica a preferência de um usuário sobre um item. Podem ser valores booleanos, ratings ou mesmo volume de vendas.</li>
# </ul>

# In[ ]:


# Imports
from pyspark.ml.recommendation import ALS


# In[ ]:


# Spark Session - usada quando se trabalha com Dataframes no Spark
spSession = SparkSession.builder.master("local").appName("DSA-SparkMLLib").getOrCreate()


# In[ ]:


# Carrega os dados no formato ALS (user, item, rating: nível de propenção do usuário ao fazer novas compras.)
ratingsRDD = sc.textFile("data/user-item.txt")
ratingsRDD.collect()


# In[ ]:


# Convertendo as strings
ratingsRDD2 = ratingsRDD.map(lambda l: l.split(',')).map(lambda l:(int(l[0]), int(l[1]), float(l[2])))


# In[ ]:


# Criando um Dataframe
ratingsDF = spSession.createDataFrame(ratingsRDD2, ["user", "item", "rating"])


# In[ ]:


ratingsDF.show()


# In[ ]:


# Construindo o modelo
# ALS = Alternating Least Squares --> Algoritmo para sistema de recomendação, que otimiza a loss function 
# e funciona muito bem em ambientes paralelizados
als = ALS(rank = 10, maxIter = 5)
modelo = als.fit(ratingsDF)


# In[ ]:


# Visualizando o Affinity Score
modelo.userFactors.orderBy("id").collect()


# In[ ]:


# Criando um dataset de teste com usuários e items para rating
testeDF = spSession.createDataFrame([(1001, 9003),(1001,9004),(1001,9005)], ["user", "item"])


# In[ ]:


# Previsões  
# Quanto maior o Affinity Score, maior a probabilidade do usuário aceitar uma recomendação (prediction positivo)
previsoes = (modelo.transform(testeDF).collect())
previsoes


# # Fim


