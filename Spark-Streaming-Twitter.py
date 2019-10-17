#!/usr/bin/env python
# coding: utf-8



# ### *********** Atenção: *********** 
# Utilize Java JDK 1.8 ou 11 e Apache Spark 2.4.2



# Acesse http://localhost:4040 sempre que quiser acompanhar a execução dos jobs

# ## Spark Streaming - Twitter

# In[5]:


# Pode ser necessário instalar esses pacotes
get_ipython().system('pip install requests requests_oauthlib')
#!pip install twython
#!pip install nltk


# In[14]:


# Módulos usados
from pyspark.streaming import StreamingContext
from pyspark import SparkContext
from requests_oauthlib import OAuth1Session #autenticação twitter
from operator import add
import requests_oauthlib
from time import gmtime, strftime
import requests
import time
import string
import ast
import json


# In[3]:


# Pacote NLTK
import nltk
from nltk.classify import NaiveBayesClassifier
from nltk.sentiment import SentimentAnalyzer
from nltk.corpus import subjectivity
from nltk.corpus import stopwords
from nltk.sentiment.util import *


# In[4]:


# Frequência de update
INTERVALO_BATCH = 5


# In[5]:


# Criando o StreamingContext
ssc = StreamingContext(sc, INTERVALO_BATCH)


# ## Treinando o Classificador de Análise de Sentimento

# Uma parte essencial da criação de um algoritmo de análise de sentimento (ou qualquer algoritmo de mineração de dados) é ter um conjunto de dados abrangente ou "Corpus" para o aprendizado, bem como um conjunto de dados de teste para garantir que a precisão do seu algoritmo atende aos padrões que você espera. Isso também permitirá que você ajuste o seu algoritmo a fim de deduzir melhores (ou mais precisas) características de linguagem natural que você poderia extrair do texto e que vão contribuir para a classificação de sentimento, em vez de usar uma abordagem genérica. Tomaremos como base o dataset de treino fornecido pela Universidade de Michigan, para competições do Kaggle --> https://inclass.kaggle.com/c/si650winter11.

# Esse dataset contém 1,578,627 tweets classificados e cada linha é marcada como: 
# 
# ### 1 para o sentimento positivo 
# ### 0 para o sentimento negativo 

# In[6]:


# Lendo o arquivo texto e criando um RDD em memória com Spark
arquivo = sc.textFile("D:...FCD/Mod_2_Python_Spark/Cap10/dataset_analise_sentimento.csv")


# In[7]:


# Removendo o cabeçalho
header = arquivo.take(1)[0]
dataset = arquivo.filter(lambda line: line != header)


# In[8]:


type(dataset)


# In[9]:


# Essa função separa as colunas em cada linha, cria uma tupla e remove a pontuação.
def get_row(line):
  row = line.split(',')
  sentimento = row[1]
  tweet = row[3].strip()
  translator = str.maketrans({key: None for key in string.punctuation})
  tweet = tweet.translate(translator)
  tweet = tweet.split(' ')
  tweet_lower = []
  for word in tweet:
    tweet_lower.append(word.lower())
  return (tweet_lower, sentimento)


# In[10]:


# Aplica a função a cada linha do dataset
dataset_treino = dataset.map(lambda line: get_row(line))


# In[11]:


# Cria um objeto SentimentAnalyzer 
sentiment_analyzer = SentimentAnalyzer()


# In[12]:


# Certifique-se de ter espaço em disco - Aproximadamente 5GB
# https://raw.githubusercontent.com/nltk/nltk_data/gh-pages/index.xml
# nltk.download()
nltk.download("stopwords")


# In[ ]:


from IPython.display import Image
Image(url = 'ntlkdata.png')


# In[13]:


# Obtém a lista de stopwords em Inglês 
stopwords_all = []
for word in stopwords.words('english'):
  stopwords_all.append(word)
  stopwords_all.append(word + '_NEG')


# In[14]:


# Obtém 10.000 tweets do dataset de treino e retorna todas as palavras que não são stopwords
dataset_treino_amostra = dataset_treino.take(10000)


# In[15]:


all_words_neg = sentiment_analyzer.all_words([mark_negation(doc) for doc in dataset_treino_amostra])
all_words_neg_nostops = [x for x in all_words_neg if x not in stopwords_all]


# In[16]:


# Cria um unigram (n-grama: sequência de palavras) e extrai as features / tri-grama wordtrueback deep learning
unigram_feats = sentiment_analyzer.unigram_word_feats(all_words_neg_nostops, top_n = 200)
sentiment_analyzer.add_feat_extractor(extract_unigram_feats, unigrams = unigram_feats)
training_set = sentiment_analyzer.apply_features(dataset_treino_amostra)


# In[17]:


type(training_set)


# In[18]:


print(training_set)


# In[19]:


# Treinar o modelo
trainer = NaiveBayesClassifier.train
classifier = sentiment_analyzer.train(trainer, training_set)


# In[20]:


# Testa o classificador em algumas sentenças
test_sentence1 = [(['this', 'program', 'is', 'bad'], '')]
test_sentence2 = [(['tough', 'day', 'at', 'work', 'today'], '')]
test_sentence3 = [(['good', 'wonderful', 'amazing', 'awesome'], '')]
test_set = sentiment_analyzer.apply_features(test_sentence1)
test_set2 = sentiment_analyzer.apply_features(test_sentence2)
test_set3 = sentiment_analyzer.apply_features(test_sentence3)


# In[1]:


# Autenticação do Twitter 
consumer_key = "xxxxxxxxxx"
consumer_secret = "xxxxxxxxxxxxx"
access_token = "xxxxxxxxxxxxxx"
access_token_secret = "xxxxxxxxxxxxxx"


# In[2]:


# Especifica a URL termo de busca
search_term = 'Trump'
sample_url = 'https://stream.twitter.com/1.1/statuses/sample.json'
filter_url = 'https://stream.twitter.com/1.1/statuses/filter.json?track='+search_term


# In[16]:


# Criando o objeto de atutenticação para o Twitter
auth = requests_oauthlib.OAuth1(consumer_key, consumer_secret, access_token, access_token_secret)


# In[17]:


# Configurando o Stream
rdd = ssc.sparkContext.parallelize([0])
stream = ssc.queueStream([], default = rdd)


# In[ ]:


type(stream)


# In[ ]:


# Total de tweets por update
NUM_TWEETS = 500  


# In[ ]:


# Essa função conecta ao Twitter e retorna um número específico de Tweets (NUM_TWEETS)
def tfunc(t, rdd):
  return rdd.flatMap(lambda x: stream_twitter_data())

def stream_twitter_data():
  response = requests.get(filter_url, auth = auth, stream = True)
  print(filter_url, response)
  count = 0
  for line in response.iter_lines():
    try:
      if count > NUM_TWEETS:
        break
      post = json.loads(line.decode('utf-8'))
      contents = [post['text']]
      count += 1
      yield str(contents)
    except:
      result = False


# In[ ]:


stream = stream.transform(tfunc)


# In[ ]:


coord_stream = stream.map(lambda line: ast.literal_eval(line))


# In[ ]:


# Essa função classifica os tweets, aplicando as features do modelo criado anteriormente
def classifica_tweet(tweet):
  sentence = [(tweet, '')]
  test_set = sentiment_analyzer.apply_features(sentence)
  print(tweet, classifier.classify(test_set[0][0]))
  return(tweet, classifier.classify(test_set[0][0]))


# In[ ]:


# Essa função retorna o texto do Twitter
def get_tweet_text(rdd):
  for line in rdd:
    tweet = line.strip()
    translator = str.maketrans({key: None for key in string.punctuation})
    tweet = tweet.translate(translator)
    tweet = tweet.split(' ')
    tweet_lower = []
    for word in tweet:
      tweet_lower.append(word.lower())
    return(classifica_tweet(tweet_lower))


# In[ ]:


# Cria uma lista vazia para os resultados
resultados = []


# In[ ]:


# Essa função salva o resultado dos batches de Tweets junto com o timestamp
def output_rdd(rdd):
  global resultados
  pairs = rdd.map(lambda x: (get_tweet_text(x)[1],1))
  counts = pairs.reduceByKey(add)
  output = []
  for count in counts.collect():
    output.append(count)
  result = [time.strftime("%I:%M:%S"), output]
  resultados.append(result)
  print(result)


# In[ ]:


# A função foreachRDD() aplica uma função a cada RDD to streaming de dados
coord_stream.foreachRDD(lambda t, rdd: output_rdd(rdd))


# In[ ]:


# Start streaming
ssc.start()
# ssc.awaitTermination()


# In[ ]:


cont = True
while cont:
  if len(resultados) > 5:
    cont = False


# In[ ]:


# Grava os resultados
rdd_save = '...BigDataAnalytics-Python-Spark2.0/Cap10/r'+time.strftime("%I%M%S")
resultados_rdd = sc.parallelize(resultados)
resultados_rdd.saveAsTextFile(rdd_save)


# In[ ]:


# Visualiza os resultados
resultados_rdd.collect()


# In[ ]:


# Finaliza o streaming
ssc.stop()



