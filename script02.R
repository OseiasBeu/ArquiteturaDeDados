#Script de instalação de manipulação spark pela linguagem R
options(repos=structure(c(
  FHI="https://folkehelseinstituttet.github.io/drat/",
  CRAN="https://cran.rstudio.com"
)))

#Carregando as bibliotecas 

library(sparklyr)
library(titanic)
library(pryr)
library(dplyr)


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

