#Script de instalação de manipulação spark pela linguagem R
options(repos=structure(c(
  FHI="https://folkehelseinstituttet.github.io/drat/",
  CRAN="https://cran.rstudio.com"
)))

# Carregando Bibliotecas --------------------------------------------------
library(sparklyr)
library(pryr)
library(tidytext)
library(dplyr)

# Conexão com o Spark -----------------------------------------------------
spark_conn <- sparklyr::spark_connect(master = "local",
                                      config=list(spark.sql.warehouse.dir=Sys.getenv("SPARK_HOME")))



# Fazendo leitura dos livros ----------------------------------------------
queiros_book1 <- spark_read_text(spark_conn, "ARelíquia_Queiros", "C:\\Users\\Oseia\\OneDrive\\Documentos\\MBA\\ArquiteturaDeDados\\ExeFInal\\ARelíquia_Queiros.txt")
queiros_book2 <- spark_read_text(spark_conn, "OPrimoBazilio", "C:\\Users\\Oseia\\OneDrive\\Documentos\\MBA\\ArquiteturaDeDados\\ExeFInal\\OPrimoBazilio_Queiro.txt")

machado_book1 <- spark_read_text(spark_conn, "DomCasmurro", "C:\\Users\\Oseia\\OneDrive\\Documentos\\MBA\\ArquiteturaDeDados\\ExeFInal\\DomCasmurro_Machado.txt")
machado_book2 <- spark_read_text(spark_conn, "QuincasBorba", "C:\\Users\\Oseia\\OneDrive\\Documentos\\MBA\\ArquiteturaDeDados\\ExeFInal\\QuincasBorba_Machado.txt")

# Concatenando Livros -----------------------------------------------------

#Criando coluna author, concatenando os livros do Eça de Queiros 
#e Removendo linhas com carácter 0 
#Criando as colunas autor e livro

queiros_books <- queiros_book1 %>%
  mutate(author = "Eca_Queiros", book = "AReliquia_Queiros") %>%
  sdf_bind_rows({
    queiros_book2 %>%
      mutate(author = "Eca_Queiros",book = "OPrimoBazilio")}) %>%
  filter(nchar(line)> 0) 


#Criando coluna author e concatenando os livros do Machado de Assis
#e Removendo linhas com carácter 0
#Criando as colunas autor e livro

machado_books <- machado_book1 %>%
  mutate(author = "Machado_Assis", book = "DomCasmurro") %>%
  sdf_bind_rows({ 
    machado_book2 %>%
      mutate(author = "Machado_Assis", book = "QuincasBorba")}) %>%
  filter(nchar(line)> 0)

print(queiros_books, n = 10)
print(machado_books, n = 10)


#1) concatene os arquivos um em baixo do outro -------------------------

#concatenando os quatro livros:
all_books <- machado_books %>%
  sdf_bind_rows({
    queiros_books
  })

print(all_books, n = 10)

#Testando filtro com coluna autor e livro
all_books %>% 
  filter(author == 'Eca_Queiros', book == "OPrimoBazilio")


# Removendo caracteres indesejados ----------------------------------------
all_books <- all_books %>%
  mutate(line = regexp_replace(line, "[_\"\'():;,.!?\\-]", " "))



# Fazendo listagem de palavras --------------------------------------------
all_books <- all_books %>%
  ft_tokenizer(input_col = "line",
               output_col = "word_list")


print(all_books, n =10)


# Transformando palavras em linhas ----------------------------------------
#removendo alguns casos extremos.

all_books <- all_books %>%
  mutate(word = explode(word_list))%>%
  select(word,author,book) %>%
  filter(nchar(word) > 2 )

print(all_books, n=20)



# Computando a tabela -----------------------------------------------------
all_books <- all_books %>%
  compute("all_words")

print(all_books,n=2)

all_books %>%
  filter(author == "Machado_Assis", book == "QuincasBorba")


#2) conte as frequencias das palavras por livro / autor; ---------------
#2.1 Autor
#2.2 Livro
#2.3 Livro e Autor

# Agrupando a quantidade de palavras por Autor:
word_count_author <- all_books %>%
  group_by(author,word) %>% 
  tally() %>% 
  arrange(desc(n))

print(word_count_author, n= 10)

#fazendo ordernação: crescente
word_count <- all_books %>%
  group_by(author,word) %>%
  tally() %>% 
  arrange(n)
print(word_count_author, n= 10)


# Agrupando a quantidade de palavras por Livro:
word_count_book <- all_books %>%
  group_by(book,word) %>% 
  tally() %>% 
  arrange(desc(n))

print(word_count_book, n= 10)

#fazendo ordernação: crescente
word_count_book <- all_books %>%
  group_by(book,word) %>%
  tally() %>% 
  arrange(n)
print(word_count_book, n= 10)

# Agrupando a quantidade de palavras por Livro e Autor:
word_count_book_author <- all_books %>%
  group_by(book,author,word) %>% 
  tally() %>% 
  arrange(desc(n))

print(word_count_book_author, n= 10)

#fazendo ordernação: crescente
word_count_book_author <- all_books %>%
  group_by(book,author,word) %>%
  tally() %>% 
  arrange(n)
print(word_count_book_author, n= 10)


#VISUALIZAÇÕES FINAIS:
print(word_count_author, n = 1)
print(word_count_book, n = 1)
print(word_count_book_author, n = 1)
#A palavra que mais se repere em todos os livros é 'que'


# 3) verifique que as maiores frequÊncias são referentes a termos comuns na língua --------
#Termos mais frequêntes:
#Por autor:
print(word_count_author, n = 10)

#Por Livro:
print(word_count_book, n = 10)

#Por livro e autor:
print(word_count_book_author, n = 10)

#Com essas bisualizações podemos afirmar que as cinco palavras que mais se repetem são:
#"que", "não", "uma", "para" e "mas"


# 4) verifique quais são os termos menos frequêntes: ----------------------
#Termos menos frequêntes:
#Por autor:
#Machado de Assis
word_count_author %>%
  filter(author == "Machado_Assis",n < 2)
#Eça de Queiros
word_count_author %>%
  filter(author == "Eca_Queiros",n < 2)

# Saindo do SPARK ---------------------------------------------------------
#VISUALIZAÇÕES FINAIS:
print(word_count_author, n = 1)
print(word_count_book, n = 1)
print(word_count_book_author, n = 1)

# Conversão para o tipo de dados do R (saindo do spark)

#Tabela: word_count_author
word_count_author <- as_tibble(word_count_author)
print(word_count_author,n = 20)

#Tabela: word_count_book
word_count_book <- as_tibble(word_count_book)
print(word_count_book,n = 20)

#Tabela: word_count_book_author
word_count_book_author <- as_tibble(word_count_book_author)
print(word_count_book_author,n = 20)


# 5)calcule o peso tf-idf de importância de palavras ----------------------

#Por Autor:
word_tfidf_author <- word_count_author %>%
  bind_tf_idf(word,author,n)

word_tfidf_author %>%
  arrange(desc(tf_idf))

#Por Livro:
word_tfidf_book <- word_count_book %>%
  bind_tf_idf(word,book,n)

word_tfidf_book %>%
  arrange(desc(tf_idf))


# 6) identifique 1 dezena de palavras mais importantes por autor. ---------


#Por Autor
word_tfidf_filter <-  word_tfidf_author %>%
  filter(tf_idf > 0.0002 & tf_idf < 0.0008)
barplot(word_tfidf_filter$tf_idf,names.arg = word_tfidf_filter$word)

#Por Livro
word_tfidf_filter <-  word_tfidf_book %>%
  filter(tf_idf > 0.0002 & tf_idf < 0.0008)
barplot(word_tfidf_filter$tf_idf,names.arg = word_tfidf_filter$word)





