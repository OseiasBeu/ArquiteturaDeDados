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







