#Script de instalação de manipulação spark pela linguagem R

options(repos=structure(c(
  FHI="https://folkehelseinstituttet.github.io/drat/",
  CRAN="https://cran.rstudio.com"
)))

install.packages("magrittr")
library(magrittr)
# INSTALAÇÃO DE BIBLIOTECAS -----------------------------------------------

install.packages('sparklyr')
install.packages('dplyr')
install.packages('titanic')
install.packages('pryr')

# CARREGAR BIBLIOTECAS ----------------------------------------------------
library(dplyr)
library(sparklyr)
library(titanic)
library(pryr)


# INSTALAR O SPARK --------------------------------------------------------

spark_install(version = "1.6.2")

Sys.setenv(SPARK_HOME= 'C:/Users/Oseia/AppData/Local/spark/spark-1.6.2-bin-hadoop2.6')



# CONECTAR AO SPARK -------------------------------------------------------
#spark_conn <- spark_connect(master = "local",
 #                           config=list(spark.sql.warehouse.dir=Sys.getenv("SPARK_HOME")))


#sparklyr::spark_connect()
spark_conn <- sparklyr::spark_connect(master = "local",
                                     config=list(spark.sql.warehouse.dir=Sys.getenv("SPARK_HOME")))

install.packages("titanic")
library(titanic)

# LENDO ARQUIVO -----------------------------------------------------------

#Função para ler arquivos csv do spark
?spark_read_csv()


View(Titanic)
View(titanic_test)

titanic_tbl <- sparklyr::copy_to(spark_conn, titanic_test, overwrite="TRUE")

src_tbls(spark_conn)


titanic_tbl <- tbl(spark_conn, "titanic_test")
dim(titanic_tbl)
object.size(titanic_tbl)


# EXPLORAR ----------------------------------------------------------------

#iMPRIMINDO TODAS AS COLUNAS E TRÊS LINHAS
print(titanic_tbl, n=3, width=Inf)


#Estrutura de colunas 
str(titanic_tbl)
#glimpse(titanic_tbl)



# MANIPULAÇÃO DE DADOS ----------------------------------------------------
#Vamos manipular a tabela titanic_tbl
titanic_tbl %>%
  #selecionar colunas
 select(Name,Sex,Age, Embarked, Pclass)  %>%
  #filtrar mulheres
  filter(Sex =="female") %>%
  #Filtrar mais velhos
  filter(Age > 20) %>%
  #Filtar cidade de embarque
  filter(Embarked !=  "S") %>%
  #filtrar Pclass
  filter(Pclass == 2)


titanic_tbl %>%
  select(Name,Sex,Age, Embarked, Pclass)  %>%
  #Ordenação de colunas
  arrange(Name,desc(Age),Sex)


#MUTATE = Criar Colunas 
titanic_tbl %>%
  select(Name,Sex,Ticket,Fare) %>%
  #Criando uma nova coluna 
  mutate(
    fare_multiplo = Fare * 4
  )
  

titanic_tbl %>%
  select(Name,Sex,Ticket,SibSp, Parch) %>%
  #Criando uma nova coluna 
  mutate(
    SOMA_SIBSPPARCH = SibSp + Parch
  )

#SUMARIZAR OU AGREGAR
titanic_tbl %>%
  select(Name, Sex, Ticket, Fare) %>%
  mutate(
    fare_multiplo = Fare * 4
  ) %>%
  summarise(media_fare_multiplo = mean(fare_multiplo))


#outras funções de sumarização mean, max, min, mdian, var


#Desconectar
spark_disconnect(spark_conn)



















