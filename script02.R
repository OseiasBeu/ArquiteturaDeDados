#Script de instalação de manipulação spark pela linguagem R
options(repos=structure(c(
  FHI="https://folkehelseinstituttet.github.io/drat/",
  CRAN="https://cran.rstudio.com"
)))
#Instalando uma biblioteca para analisede texto no R
install.packages('tidytext')

#Carregando as bibliotecas 

library(sparklyr)
library(titanic)
library(pryr)
library(dplyr)
library(tidytext)


# Fazendo a conexão com o spark 
spark_conn <- sparklyr::spark_connect(master = "local",
                                      config=list(spark.sql.warehouse.dir=Sys.getenv("SPARK_HOME")))
#Link dos livros:
#https://www.gutenberg.org/files/74/74-0.txt 
#https://www.gutenberg.org/files/1661/1661-0.txt

#Fazendo a leitura de texto:
doyle <- spark_read_text(spark_conn, "doyle", "C:\\Users\\Oseia\\OneDrive\\Documentos\\MBA\\Doyle.txt")
twain <- spark_read_text(spark_conn, "twain", "C:\\Users\\Oseia\\OneDrive\\Documentos\\MBA\\Twain.txt")


#Criando coluna author:
all_words <- doyle %>%
  mutate(author = "doyle") %>%
  sdf_bind_rows({ # Coloca um livro em baixo do outro
    twain %>%
      mutate(author = "twain")}) %>%
  filter(nchar(line)> 0) # Removendo linahs com carácter 0

# Removendo caracteres indesejados
all_words <- all_words %>%
  mutate(line = regexp_replace(line, "[_\"\'():;,.!?\\-]", " ")) 


#listagem de palavras:
all_words <- all_words %>%
  ft_tokenizer(input_col = "line",
               output_col = "word_list")


head(all_words,4)
print(all_words)

#Pivotamos, ou seja, transformamos cada palavra em uma linha
all_words <- all_words %>%
  mutate(word = explode(word_list))%>%
  select(word,author) %>%
  filter(nchar(word) > 2 )

print(all_words, n=20)

#Separamos todas as palavras de todas as linhas de dois livros,
#removendo alguns casos extremos.


#Computando toda a tabela
all_words <- all_words %>%
  compute("all_words")

print(all_words,n=40)


# ANALISE  ----------------------------------------------------------------

#Agrupando quantidade de palavras por author
word_count <- all_words %>%
  group_by(author,word) %>% #função de agrupamento
  tally() %>% #função que efetua a contagem por agrupamento
  arrange(desc(n)) #função de ordenação

print(word_count, n= 10)


#Em odem crescente
word_count <- all_words %>%
  group_by(author,word) %>%
  tally() %>% 
  arrange(n)

#Geralmente, nós removemos as palavras que aparecem apenas uma vez
print(word_count, n= 10)


# Conversão para o tipo de dados do R (saindo do spark)
# Tf–idf -- o peso, a importância de uma sentença em um livro está totalmetne 
#ligada a quantidade de vezes que a mesma se repete no livro
#https://pt.wikipedia.org/wiki/Tf%E2%80%93idf
word_count <- as_tibble(word_count)
print(word_count,n = 20)

#Aplicando a função tf-idf -- relaciona a contagem de palavras,
# com a frequências das palavras e as importâncias no documento

word_tfidf <- word_count %>%
  bind_tf_idf(word,author,n)

#Encontrando o protagonista dos livro:
# É só descobrir quem é o maior tf_idf dos livros
word_tfidf %>%
  arrange(desc(tf_idf))


#COntando os primieros 20 tf-idf de cada autor
twain20 <- word_tfidf %>%
  arrange(desc(tf_idf)) %>%
  filter(author == "twain")


doyle20 <- word_tfidf %>%
  arrange(desc(tf_idf)) %>%
  filter(author == "doyle")

print(twain20, n =20)
print(doyle20, n =20)


#Plotanto um gráfico com as palavras de maior mportância:
#plot
word_tfidf_filter <-  word_tfidf %>%
  filter(tf_idf > 0.0002 & tf_idf < 0.0008)
barplot(word_tfidf_filter$tf_idf,names.arg = word_tfidf_filter$word)


#Tema para o exercício final:
#Quais as palavras que usaria para difernecias Machado de Assis de Eça de Queirós?
