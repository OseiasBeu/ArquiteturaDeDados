library(sparklyr)
library(dplyr)
library(pryr)
library(tidytext)




spark_conn <- spark_connect(master = "local",
                            config=list(spark.sql.warehouse.dir=Sys.getenv("SPARK_HOME")))

#https://www.gutenberg.org/files/1661/1661-0.txt
#https://www.gutenberg.org/files/74/74-0.txt


twain <- spark_read_text(spark_conn, "twain", "C:\\Users\\fekop\\Documents\\aulas\\arquiteturadedados\\aula3\\twain.txt")
doyle <- spark_read_text(spark_conn, "doyle", "C:\\Users\\fekop\\Documents\\aulas\\arquiteturadedados\\aula3\\doyle.txt")
print(doyle,n=20)

#criando coluna autor
doyle %>%
  mutate(author = "doyle")

#juntando tudo
all_words <- doyle %>%
  mutate(author = "doyle") %>% #criando coluna com autor doyle
  sdf_bind_rows({ #colocando um livro em baixo do outro
    twain %>%
      mutate(author = "twain")}) %>% #criando coluna com autor twain
  filter(nchar(line) > 0) #removendo linhas com 0 caracteres

#removendo caracteres pouco relevantes
all_words <- all_words %>%
  mutate(line = regexp_replace(line, "[_\"\'():;,.!?\\-]", " ")) 

print(all_words)


#listagem de palavras

all_words <- all_words %>%
  ft_tokenizer(input_col = "line",
               output_col = "word_list")

head(all_words, 4)


all_words <- all_words %>%
  mutate(word = explode(word_list)) %>%
  select(word, author) %>%
  filter(nchar(word) > 2)

print(all_words,n=20)

#Separamos todas palavras de todas as linhas de dois livros,
#removendo algunsc asos extremos.


all_words <- all_words %>%
  compute("all_words")

print(all_words,n=40)


# ANÁLISE -----------------------------------------------------------------

#contagens de palavras
word_count <- all_words %>%
  group_by(author, word) %>%
  tally() %>%
  arrange(desc(n)) 

word_count
print(word_count,n=40)


#saindo do spark
word_count <- as_tibble(word_count)

#tf-idf (pesos para balancear as palavras com frequencias diferentes)
word_tfidf <- word_count %>%
  bind_tf_idf(word,author,n)

word_tfidf %>%
  arrange(desc(tf_idf))

#plot
word_tfidf_filter <-  word_tfidf %>%
  filter(tf_idf > 0.0002 & tf_idf < 0.0008)
barplot(word_tfidf_filter$tf_idf,names.arg = word_tfidf_filter$word,horiz = T)






