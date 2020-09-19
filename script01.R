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

library(sparklyr)
library(titanic)
library(pryr)
library(dplyr)


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



# EXERCICIO ---------------------------------------------------------------

#Compute o valor médio de Fare para cada Pclass

titanic_tbl %>%
  select(Name, Pclass, Fare) %>%
  #agrupando 
  group_by(Pclass) %>%
  summarize(Fare_medio = mean(Fare))




titanic_tbl %>%
  select(Name, Pclass, Fare) %>%
  #agrupando 
  group_by(Pclass) %>%
  summarize(Fare_medio = mean(Fare))



titanic_tbl %>%
  #select(Name, Pclass, Fare, Embarked) %>%
  #agrupando 
  group_by(Pclass,Embarked) %>%
  summarize(Fare_medio = mean(Fare)) %>%
  arrange(Pclass)


#EXERCICIO 01
#calculem a média de Fare apenas para os grupos Pclass 1 ou 2 é bem próximo do que já fizemos, mas há um remoção de linhas com Pclass = 3
titanic_tbl %>%
  select(Name, Pclass, Fare) %>%
  filter(Pclass != 3) %>%
  #agrupando 
  group_by(Pclass) %>%
  summarize(Fare_medio = mean(Fare))

#EXERCICIO 02
#Construa uma nova coluna usando mutate, onde o valor é "Adulto" caso a idade seja maior que 20 e  
#"Jovem" caso a idade seja menor ou igual a 20
#Contar todas a quantidade de jovens e adultos 
#Remover idades com o valor 'N#A'

titanic_tbl %>%
  select(Name,Sex, Age) %>%
  #Criando uma nova coluna 
  mutate(
    maioridade =  ifelse(Age > 20,"Adulto","Jovem")
  )



titanic_tbl %>%
  select(Name,Sex, Age) %>%
  #Criando uma nova coluna 
  mutate(
    maioridade = if (Age <= 20) {"Jovem"} else {"Adulto"}
  ) %>%
  group_by(maioridade) %>%
  count(maioridade) %>%
  filter(maioridade != 'N#A')



#EXERCICIO 03
#Qual o ticket médio para cada faixa etaria?
#0-10
#10-20
#20-30
#...
#60-70
#70-80

  titanic_tbl %>%
    select(Name, Age, Fare) %>%
    mutate(
      maioridade = case_when(
        Age >= 0 & Age <= 10 ~ 'A: 0-10',
        Age >=11 & Age <= 20 ~ 'B: 10-20',
        Age >=21 & Age <= 30 ~ 'C: 20-30',
        Age >=31 & Age <= 40 ~ 'D: 30-40',
        Age >=41 & Age <= 50 ~ 'E: 40-50',
        Age >=51 & Age <= 60 ~ 'F: 50-60',
        Age >=61 & Age <= 70 ~ 'G: 60-70',
        Age >=71 & Age <= 80 ~ 'H: 70-80',
        Age >=81 & Age <= 90 ~ 'I: 80-90',
        Age >=91 & Age <= 100 ~'J: 90-100',
        Age >=101 & Age<= 110 ~'K: 100-110',
        Age >=111 & Age<= 120 ~'L: 100-110',
        TRUE ~ "N#A"
      )
  ) %>%
    group_by(maioridade) %>%
    summarise( Fare_medio = mean(Fare)) %>%
    arrange(maioridade)
  
  
#Age[0:10]
    
    
    
    mtcars %>% 
  mutate(
    category = case_when(
    cyl == 4 & disp < median(disp),
    cyl == 8 & disp > median(disp),
    TRUE ~ "other"
  )
  )



print(titanic_tbl)










#Desconectar
#spark_disconnect(spark_conn)



















