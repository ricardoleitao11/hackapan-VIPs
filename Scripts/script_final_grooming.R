rm(list=ls())
gc()


# Chamada Bibliotecas -----------------------------------------------------

library(data.table)
library(dplyr)
library(reshape)
library(lubridate)
library(plyr)
library(stringr)
library(ggplot2)

# Tratamento Base Transacoes ----------------------------------------------


base_transacoes <- fread("C:\\Bases - Hackapan\\BASE_TRANSACOES_HACKAPAN.txt", dec=",")

base_transacoes$DATA_HORA2 <- as.POSIXct(base_transacoes$DATA_HORA, format = "%Y-%m-%d %H:%M:%S", tz = "UTC")

base_transacoes$DATA_HORA <- as.Date(base_transacoes$DATA_HORA)

base_transacoes <- arrange(base_transacoes, DATA_HORA)


nightday <- function(datetime) {
  paste(
    c("Night", "Morning", "Afternoon", "Evening", "Night")[
      cut(as.numeric(format(datetime, "%H%M")), c(0, 530, 1100, 1700 ,2000, 2359))
      ]
  )
}

base_transacoes$periodo_dia_transacao <- nightday(base_transacoes$DATA_HORA2)


base_transacoes$NUMERO_PARCELAS[is.na(base_transacoes$NUMERO_PARCELAS)] <- 0

sapply(base_transacoes, function(x) sum(is.na(x)))

#Agregando informacoes de transacao para unidade de analise individuo
base_transacoes_individuo <- base_transacoes %>% 
  dplyr::group_by(ID) %>%
  dplyr::summarise(
    DATA_TRANSACAO_MAIS_ANTIGA=data.table::first(DATA_HORA),
    DATA_TRANSACAO_MAIS_RECENTE = data.table::last(DATA_HORA),
    
    numero_transacoes_tarde_noite = sum(!is.na(periodo_dia_transacao[periodo_dia_transacao=="Night"])) ,
    numero_transacoes_manha = sum(!is.na(periodo_dia_transacao[periodo_dia_transacao=="Morning"])) ,
    numero_transacoes_tarde = sum(!is.na(periodo_dia_transacao[periodo_dia_transacao=="Afternoon"])) ,
    numero_transacoes_noite_cedo = sum(!is.na(periodo_dia_transacao[periodo_dia_transacao=="Evening"])),
    
    prop_transacoes_tarde_noite = sum(!is.na(periodo_dia_transacao[periodo_dia_transacao=="Night"])) / sum(!is.na(periodo_dia_transacao)) ,
    prop_transacoes_manha = sum(!is.na(periodo_dia_transacao[periodo_dia_transacao=="Morning"])) / sum(!is.na(periodo_dia_transacao)) ,
    prop_transacoes_tarde = sum(!is.na(periodo_dia_transacao[periodo_dia_transacao=="Afternoon"])) / sum(!is.na(periodo_dia_transacao)) ,
    prop_transacoes_noite_cedo = sum(!is.na(periodo_dia_transacao[periodo_dia_transacao=="Evening"])) / sum(!is.na(periodo_dia_transacao)) ,
    
    total_transacoes_6m = sum(!is.na(DATA_HORA[month( data.table::last(DATA_HORA)) - month(DATA_HORA) < 7 ])) ,
    media_transacoes_dia_ultimos_6m = sum(!is.na(DATA_HORA[month( data.table::last(DATA_HORA)) - month(DATA_HORA) < 7 ])) / 180  ,
    total_transacoes_ultimos_3d = sum(!is.na(DATA_HORA[day(data.table::last(DATA_HORA)) - day(DATA_HORA) < 4 ])) ,
    media_transacoes_dia_ultimos_3d = sum(!is.na(DATA_HORA[day(data.table::last(DATA_HORA)) - day(DATA_HORA) < 4 ])) / 3,
    
    media_parcelas_por_transacao_6m = mean(NUMERO_PARCELAS[(month(data.table::last(DATA_HORA)) - month(DATA_HORA) < 7)]),
    media_parcelas_por_transacao_3d = mean(NUMERO_PARCELAS[(day(data.table::last(DATA_HORA)) - day(DATA_HORA) < 4)]),
    
    bandeira = data.table::first(BANDEIRA),
    
    valor_max_transacao_6m = max(VALOR_TRANSACAO[(month(data.table::last(DATA_HORA)) - month(DATA_HORA) < 7)]),
    valor_max_transacao_3d = max(VALOR_TRANSACAO[(day(data.table::last(DATA_HORA)) - day(DATA_HORA) < 4)]),
    
    valor_med_transacao_6m = mean(VALOR_TRANSACAO[(month(data.table::last(DATA_HORA)) - month(DATA_HORA) < 7)]),
    valor_med_transacao_3d = mean(VALOR_TRANSACAO[(day(data.table::last(DATA_HORA)) - day(DATA_HORA) < 4)]),
    valor_med_diario_6m = sum(VALOR_TRANSACAO[(month(data.table::last(DATA_HORA)) - month(DATA_HORA) < 7)]) / 180,
    valor_med_diario_3d = sum(VALOR_TRANSACAO[(day(data.table::last(DATA_HORA)) - day(DATA_HORA) < 4)]) / 3,
    
    valor_mediano_transacao_6m = median(VALOR_TRANSACAO[(month(data.table::last(DATA_HORA)) - month(DATA_HORA) < 7)]),
    valor_mediano_transacao_3d = median(VALOR_TRANSACAO[(day(data.table::last(DATA_HORA)) - day(DATA_HORA) < 4)]),
    
    valor_total_6m = sum(VALOR_TRANSACAO[(month(data.table::last(DATA_HORA)) - month(DATA_HORA) < 7)]),
    valor_total_3d = sum(VALOR_TRANSACAO[(day(data.table::last(DATA_HORA)) - day(DATA_HORA) < 4)]),
    
    limite_disponivel_med_diario_6m = sum(LIMITE_DISPONIVEL_APOS_TRANSACAO[(month(data.table::last(DATA_HORA)) - month(DATA_HORA) < 7)]) / 180,
    limite_disponivel_med_diario_3d = sum(LIMITE_DISPONIVEL_APOS_TRANSACAO[(day(data.table::last(DATA_HORA)) - day(DATA_HORA) < 4)]) / 3,
    
    prop_compra_online_6m = sum(!is.na(COMPRA_PRESENCIAL[(COMPRA_PRESENCIAL == "N PRES") &(month(data.table::last(DATA_HORA)) - month(DATA_HORA) < 7)]))/ sum(!is.na(COMPRA_PRESENCIAL[(month(data.table::last(DATA_HORA)) - month(DATA_HORA) < 7)])),
    prop_compra_online_3d = sum(!is.na(COMPRA_PRESENCIAL[(COMPRA_PRESENCIAL == "N PRES") & (day(data.table::last(DATA_HORA)) - day(DATA_HORA) < 4)]))/ sum(!is.na(COMPRA_PRESENCIAL[(day(data.table::last(DATA_HORA)) - day(DATA_HORA) < 4)])),
    
    num_compra_online_6m = sum(!is.na(COMPRA_PRESENCIAL[(COMPRA_PRESENCIAL == "N PRES") &(month(data.table::last(DATA_HORA)) - month(DATA_HORA) < 7)])),
    num_compra_online_3d = sum(!is.na(COMPRA_PRESENCIAL[(COMPRA_PRESENCIAL == "N PRES") & (day(data.table::last(DATA_HORA)) - day(DATA_HORA) < 4)])),
    
    prop_aprovacao_6m = sum(!is.na(APROVADO_NEGADO[(APROVADO_NEGADO == "APR") &(month(data.table::last(DATA_HORA)) - month(DATA_HORA) < 7)]))/ sum(!is.na(APROVADO_NEGADO[(month(data.table::last(DATA_HORA)) - month(DATA_HORA) < 7)])),
    prop_aprovacao_3d = sum(!is.na(APROVADO_NEGADO[(APROVADO_NEGADO == "APR") & (day(data.table::last(DATA_HORA)) - day(DATA_HORA) < 4) ])) / sum(!is.na(APROVADO_NEGADO[(day(data.table::last(DATA_HORA)) - day(DATA_HORA) < 4)])),
  )


base_transacoes_individuo$total_dias_observados <- base_transacoes_individuo$DATA_TRANSACAO_MAIS_RECENTE - base_transacoes_individuo$DATA_TRANSACAO_MAIS_ANTIGA


base_transacoes_individuo_ramo_6m <- filter(base_transacoes, (month(data.table::last(DATA_HORA)) - month(DATA_HORA)) < 7 ) %>%
  dplyr::group_by(ID, RAMO_LOJISTA) %>%
  dplyr::summarise(
    numero_transacoes = sum(!is.na(RAMO_LOJISTA))
    
  )

base_transacoes_individuo_ramo_3d <- filter(base_transacoes, (day(data.table::last(DATA_HORA)) - day(DATA_HORA)) < 4 ) %>%
  dplyr::group_by(ID, RAMO_LOJISTA) %>%
  dplyr::summarise(
    numero_transacoes = sum(!is.na(RAMO_LOJISTA))
    
  )

base_transacoes_individuo_ramo_6m_final <- cast(base_transacoes_individuo_ramo_6m, ID ~ RAMO_LOJISTA, sum, value = 'numero_transacoes')

base_transacoes_individuo_ramo_6m_final_prop <-ddply(base_transacoes_individuo_ramo_6m, .(ID), summarise, RAMO_LOJISTA = RAMO_LOJISTA, pct = numero_transacoes / sum(numero_transacoes) )
base_transacoes_individuo_ramo_6m_final_prop <- cast(base_transacoes_individuo_ramo_6m_final_prop, ID ~ RAMO_LOJISTA, value = 'pct')

base_transacoes_individuo_ramo_6m_final[is.na(base_transacoes_individuo_ramo_6m_final)] <- 0
base_transacoes_individuo_ramo_6m_final_prop[is.na(base_transacoes_individuo_ramo_6m_final_prop)] <- 0

base_transacoes_individuo_ramo_3d_final <- cast(base_transacoes_individuo_ramo_3d, ID ~ RAMO_LOJISTA, sum, value = 'numero_transacoes')

base_transacoes_individuo_ramo_3d_final_prop <-ddply(base_transacoes_individuo_ramo_3d, .(ID), summarise, RAMO_LOJISTA = RAMO_LOJISTA, pct = numero_transacoes / sum(numero_transacoes) )
base_transacoes_individuo_ramo_3d_final_prop <- cast(base_transacoes_individuo_ramo_3d_final_prop, ID ~ RAMO_LOJISTA, value = 'pct')

base_transacoes_individuo_ramo_3d_final[is.na(base_transacoes_individuo_ramo_3d_final)] <- 0
base_transacoes_individuo_ramo_3d_final_prop[is.na(base_transacoes_individuo_ramo_3d_final_prop)] <- 0

setnames(base_transacoes_individuo_ramo_3d_final, paste0(names(base_transacoes_individuo_ramo_3d_final), "_3d"))
setnames(base_transacoes_individuo_ramo_3d_final_prop, paste0(names(base_transacoes_individuo_ramo_3d_final_prop), "_3d_prop"))
setnames(base_transacoes_individuo_ramo_6m_final, paste0(names(base_transacoes_individuo_ramo_6m_final), "_6m"))
setnames(base_transacoes_individuo_ramo_6m_final_prop, paste0(names(base_transacoes_individuo_ramo_6m_final_prop), "_6m_prop"))

#unificando bases agregadas
base_transacoes_individuo <- left_join(base_transacoes_individuo, base_transacoes_individuo_ramo_6m_final, by=c("ID"="ID_6m"))
base_transacoes_individuo <- left_join(base_transacoes_individuo, base_transacoes_individuo_ramo_6m_final_prop, by=c("ID"="ID_6m_prop"))
base_transacoes_individuo <- left_join(base_transacoes_individuo, base_transacoes_individuo_ramo_3d_final, by=c("ID"="ID_3d"))
base_transacoes_individuo <- left_join(base_transacoes_individuo, base_transacoes_individuo_ramo_3d_final_prop,by=c("ID"="ID_3d_prop"))

base_transacoes_individuo[is.na(base_transacoes_individuo)] <- 0

write.csv(base_transacoes_individuo, "C:\\Bases - Hackapan\\base_tratada_transacoes.csv", row.names=FALSE)

#limpando os objetos que nao iremos utilizar mais
rm(list=setdiff(ls(), c("base_transacoes", "base_transacoes_individuo")))
gc()


# Tratamento Base Atrasos -------------------------------------------------


base_atrasos <- fread("C:\\Bases - Hackapan\\BASE_ATRASOS_HACKAPAN.txt", dec=",")

base_cartoes <- fread("C:\\Bases - Hackapan\\BASE_CARTOES_HACKAPAN.txt", dec=",")

base_atrasos <- left_join(base_atrasos, select(base_cartoes, DATA_ATIVACAO, ID), by="ID")

base_atrasos$REFERENCIA <- as.Date(base_atrasos$REFERENCIA)
base_atrasos$DATA_ATIVACAO <- as.Date(base_atrasos$DATA_ATIVACAO)

base_atrasos$DIFERENCA_EM_DIAS_REFERENCIA_ATIVACAO <- base_atrasos$REFERENCIA - base_atrasos$DATA_ATIVACAO

#optamos por remover as observacoes que apresentaram diferenca negativa entre as datas, indicando
#que nao se trata de informacao referente ao cartao atual do individuo
base_atrasos <- filter(base_atrasos,  DIFERENCA_EM_DIAS_REFERENCIA_ATIVACAO > 0 | DIFERENCA_EM_DIAS_REFERENCIA_ATIVACAO == 0)

#quantidade 4 meses ou mais de atraso
base_atrasos$numero_vezes_4m_mais_sem_pagar <- ifelse((base_atrasos$QT_DIAS_ATRASO > 120) | (base_atrasos$QT_DIAS_ATRASO == 120), 1, 0)

#quantidade 1 mes ou mais de atraso
base_atrasos$numero_vezes_1m_mais_sem_pagar <- ifelse((base_atrasos$QT_DIAS_ATRASO > 30) | (base_atrasos$QT_DIAS_ATRASO == 30), 1, 0)

#quantidade 2 semanas ou mais de atraso
base_atrasos$numero_vezes_2sem_mais_sem_pagar <- ifelse((base_atrasos$QT_DIAS_ATRASO > 14) | (base_atrasos$QT_DIAS_ATRASO == 14), 1, 0)

#atraso
base_atrasos$atraso <- ifelse(base_atrasos$QT_DIAS_ATRASO > 0, 1, 0)


base_atrasos_total <- base_atrasos %>%
  dplyr::group_by(ID) %>%
  dplyr::summarise(
    numero_vezes_4m_mais_sem_pagar = sum(numero_vezes_4m_mais_sem_pagar),
    numero_vezes_1m_mais_sem_pagar = sum(numero_vezes_1m_mais_sem_pagar),
    numero_vezes_2sem_mais_sem_pagar = sum(numero_vezes_2sem_mais_sem_pagar),
    media_dias_atraso_quando_atrasou = mean(QT_DIAS_ATRASO[QT_DIAS_ATRASO > 0]) ,
    mediana_dias_atraso_quando_atrasou = median(QT_DIAS_ATRASO[QT_DIAS_ATRASO > 0]),
    dias_atraso_acumulados = sum(QT_DIAS_ATRASO),
    maximo_dias_atraso = max(QT_DIAS_ATRASO),
    total_atrasos = sum(atraso),
    prop_atrasos = sum(!is.na(REFERENCIA[QT_DIAS_ATRASO>0])) / sum(!is.na(REFERENCIA))
    
  )

base_atrasos_total[is.na(base_atrasos_total)] <- 0 

write.csv(base_atrasos_total, "C:\\Bases - Hackapan\\base_tratada_atrasos.csv", row.names=FALSE)

#limpando os objetos que nao iremos utilizar mais
rm(list=setdiff(ls(), c("base_atrasos", "base_atrasos_total")))
gc()



# Tratamento de Missings Atrasos --------------------------------------------------


#Testes imputacao para os missings
base_atraso_total <- read.csv("C:\\Bases - Hackapan/base_tratada_atrasos.csv", header=TRUE)
base_pessoa <- read.csv("C:\\Bases - Hackapan/base_vinculada_tratada_pat.csv", header=TRUE)


base_pessoa <- left_join(base_pessoa, base_atraso_total, by="ID")

sapply(base_pessoa, function(x) sum(is.na(x)))

#base_pessoa_missing_score_fraude <- filter(base_pessoa, is.na(SCORE_FRAUDE))
#sapply(base_pessoa_missing_score_fraude, function(x) sum(is.na(x)))
#h? variaveis para imputacao


#Imputando valores para o grupo de homens, e depois o de mulheres, onde:
#O ESCORE_FRAUDE ? missing,
#N?O ? UM CARTAO FRAUDADO,
#ATRASOS: (nao tem, teve <=2 semanas, teve >2semanas <=1 mes, teve >1m)
#E LIMITE CREDITO DO CARTAO MENOR QUE A MEDIA
str(base_pessoa)

base_pessoa <- filter(base_pessoa, !is.na(base_pessoa$total_atrasos))

base_pessoa$FRAUDE <- as.character(base_pessoa$FRAUDE)
base_pessoa$FRAUDE <- str_trim(base_pessoa$FRAUDE)
base_pessoa$GENERO <- as.character(base_pessoa$GENERO)

base_pessoa$SCORE_FRAUDE_IMPUTADO <- base_pessoa$SCORE_FRAUDE
base_pessoa$SCORE_FRAUDE_IMPUTADO <- ifelse(is.na(base_pessoa$SCORE_FRAUDE_IMPUTADO) & base_pessoa$FRAUDE == "0" & base_pessoa$total_atrasos == 0 & base_pessoa$GENERO == "MASCULINO" & base_pessoa$LIMITE_CREDITO_CARTAO < mean(base_pessoa$LIMITE_CREDITO_CARTAO), mean(base_pessoa$SCORE_FRAUDE[!is.na(base_pessoa$SCORE_FRAUDE_IMPUTADO) & base_pessoa$FRAUDE == "0" & base_pessoa$total_atrasos == 0 & base_pessoa$GENERO == "MASCULINO" & base_pessoa$LIMITE_CREDITO_CARTAO < mean(base_pessoa$LIMITE_CREDITO_CARTAO)]) ,base_pessoa$SCORE_FRAUDE_IMPUTADO )
base_pessoa$SCORE_FRAUDE_IMPUTADO <- ifelse(is.na(base_pessoa$SCORE_FRAUDE_IMPUTADO) & base_pessoa$FRAUDE == "0" & base_pessoa$total_atrasos > 0 & base_pessoa$numero_vezes_1m_mais_sem_pagar == 0 & base_pessoa$GENERO == "MASCULINO" & base_pessoa$LIMITE_CREDITO_CARTAO < mean(base_pessoa$LIMITE_CREDITO_CARTAO), mean(base_pessoa$SCORE_FRAUDE[!is.na(base_pessoa$SCORE_FRAUDE_IMPUTADO) & base_pessoa$FRAUDE == "0" & base_pessoa$total_atrasos > 0 & base_pessoa$numero_vezes_1m_mais_sem_pagar == 0 & base_pessoa$GENERO == "MASCULINO" & base_pessoa$LIMITE_CREDITO_CARTAO < mean(base_pessoa$LIMITE_CREDITO_CARTAO)]) ,base_pessoa$SCORE_FRAUDE_IMPUTADO )
base_pessoa$SCORE_FRAUDE_IMPUTADO <- ifelse(is.na(base_pessoa$SCORE_FRAUDE_IMPUTADO) & base_pessoa$FRAUDE == "0" & base_pessoa$numero_vezes_1m_mais_sem_pagar > 0 & base_pessoa$numero_vezes_4m_mais_sem_pagar ==0 & base_pessoa$GENERO == "MASCULINO" & base_pessoa$LIMITE_CREDITO_CARTAO < mean(base_pessoa$LIMITE_CREDITO_CARTAO) , mean(base_pessoa$SCORE_FRAUDE[!is.na(base_pessoa$SCORE_FRAUDE_IMPUTADO) & base_pessoa$FRAUDE == "0" & base_pessoa$numero_vezes_1m_mais_sem_pagar > 0 & base_pessoa$numero_vezes_4m_mais_sem_pagar ==0 & base_pessoa$GENERO == "MASCULINO" & base_pessoa$LIMITE_CREDITO_CARTAO < mean(base_pessoa$LIMITE_CREDITO_CARTAO)]) ,base_pessoa$SCORE_FRAUDE_IMPUTADO )
base_pessoa$SCORE_FRAUDE_IMPUTADO <- ifelse(is.na(base_pessoa$SCORE_FRAUDE_IMPUTADO) & base_pessoa$FRAUDE == "0" & base_pessoa$numero_vezes_4m_mais_sem_pagar > 0 & base_pessoa$GENERO == "MASCULINO" & base_pessoa$LIMITE_CREDITO_CARTAO < mean(base_pessoa$LIMITE_CREDITO_CARTAO) , mean(base_pessoa$SCORE_FRAUDE[!is.na(base_pessoa$SCORE_FRAUDE_IMPUTADO) & base_pessoa$FRAUDE == "0" & base_pessoa$numero_vezes_4m_mais_sem_pagar > 0 & base_pessoa$GENERO == "MASCULINO" & base_pessoa$LIMITE_CREDITO_CARTAO < mean(base_pessoa$LIMITE_CREDITO_CARTAO)] ) ,base_pessoa$SCORE_FRAUDE_IMPUTADO )
base_pessoa$SCORE_FRAUDE_IMPUTADO <- ifelse(is.na(base_pessoa$SCORE_FRAUDE_IMPUTADO) & base_pessoa$FRAUDE == "0" & base_pessoa$total_atrasos == 0 & base_pessoa$GENERO == "MASCULINO" & base_pessoa$LIMITE_CREDITO_CARTAO > mean(base_pessoa$LIMITE_CREDITO_CARTAO), mean(base_pessoa$SCORE_FRAUDE[!is.na(base_pessoa$SCORE_FRAUDE_IMPUTADO) & base_pessoa$FRAUDE == "0" & base_pessoa$total_atrasos == 0 & base_pessoa$GENERO == "MASCULINO" & base_pessoa$LIMITE_CREDITO_CARTAO > mean(base_pessoa$LIMITE_CREDITO_CARTAO)]) ,base_pessoa$SCORE_FRAUDE_IMPUTADO )
base_pessoa$SCORE_FRAUDE_IMPUTADO <- ifelse(is.na(base_pessoa$SCORE_FRAUDE_IMPUTADO) & base_pessoa$FRAUDE == "0" & base_pessoa$total_atrasos > 0 & base_pessoa$numero_vezes_1m_mais_sem_pagar == 0 & base_pessoa$GENERO == "MASCULINO" & base_pessoa$LIMITE_CREDITO_CARTAO > mean(base_pessoa$LIMITE_CREDITO_CARTAO), mean(base_pessoa$SCORE_FRAUDE[!is.na(base_pessoa$SCORE_FRAUDE_IMPUTADO) & base_pessoa$FRAUDE == "0" & base_pessoa$total_atrasos > 0 & base_pessoa$numero_vezes_1m_mais_sem_pagar == 0 & base_pessoa$GENERO == "MASCULINO" & base_pessoa$LIMITE_CREDITO_CARTAO > mean(base_pessoa$LIMITE_CREDITO_CARTAO)]) ,base_pessoa$SCORE_FRAUDE_IMPUTADO )
base_pessoa$SCORE_FRAUDE_IMPUTADO <- ifelse(is.na(base_pessoa$SCORE_FRAUDE_IMPUTADO) & base_pessoa$FRAUDE == "0" & base_pessoa$numero_vezes_1m_mais_sem_pagar > 0 & base_pessoa$numero_vezes_4m_mais_sem_pagar ==0 & base_pessoa$GENERO == "MASCULINO" & base_pessoa$LIMITE_CREDITO_CARTAO > mean(base_pessoa$LIMITE_CREDITO_CARTAO) , mean(base_pessoa$SCORE_FRAUDE[!is.na(base_pessoa$SCORE_FRAUDE_IMPUTADO) & base_pessoa$FRAUDE == "0" & base_pessoa$numero_vezes_1m_mais_sem_pagar > 0 & base_pessoa$numero_vezes_4m_mais_sem_pagar ==0 & base_pessoa$GENERO == "MASCULINO" & base_pessoa$LIMITE_CREDITO_CARTAO > mean(base_pessoa$LIMITE_CREDITO_CARTAO)]) ,base_pessoa$SCORE_FRAUDE_IMPUTADO )
base_pessoa$SCORE_FRAUDE_IMPUTADO <- ifelse(is.na(base_pessoa$SCORE_FRAUDE_IMPUTADO) & base_pessoa$FRAUDE == "0" & base_pessoa$numero_vezes_4m_mais_sem_pagar > 0 & base_pessoa$GENERO == "MASCULINO" & base_pessoa$LIMITE_CREDITO_CARTAO > mean(base_pessoa$LIMITE_CREDITO_CARTAO) , mean(base_pessoa$SCORE_FRAUDE[!is.na(base_pessoa$SCORE_FRAUDE_IMPUTADO) & base_pessoa$FRAUDE == "0" & base_pessoa$numero_vezes_4m_mais_sem_pagar > 0 & base_pessoa$GENERO == "MASCULINO" & base_pessoa$LIMITE_CREDITO_CARTAO > mean(base_pessoa$LIMITE_CREDITO_CARTAO)] ) ,base_pessoa$SCORE_FRAUDE_IMPUTADO )

#Imputando valores para o grupo de homens, e depois o de mulheres, onde:
#O ESCORE_FRAUDE ? missing,
#N?O ? UM CARTAO FRAUDADO,
#ATRASOS: (nao tem, teve <=2 semanas, teve >2semanas <=1 mes, teve >1m)
#E LIMITE CREDITO DO CARTAO MAIOR QUE A MEDIA
base_pessoa$SCORE_FRAUDE_IMPUTADO <- ifelse(is.na(base_pessoa$SCORE_FRAUDE_IMPUTADO) & base_pessoa$FRAUDE == "0" & base_pessoa$total_atrasos == 0 & base_pessoa$GENERO == "FEMININO" & base_pessoa$LIMITE_CREDITO_CARTAO < mean(base_pessoa$LIMITE_CREDITO_CARTAO), mean(base_pessoa$SCORE_FRAUDE[!is.na(base_pessoa$SCORE_FRAUDE_IMPUTADO) & base_pessoa$FRAUDE == "0" & base_pessoa$total_atrasos == 0 & base_pessoa$GENERO == "FEMININO" & base_pessoa$LIMITE_CREDITO_CARTAO < mean(base_pessoa$LIMITE_CREDITO_CARTAO)]) ,base_pessoa$SCORE_FRAUDE_IMPUTADO )
base_pessoa$SCORE_FRAUDE_IMPUTADO <- ifelse(is.na(base_pessoa$SCORE_FRAUDE_IMPUTADO) & base_pessoa$FRAUDE == "0" & base_pessoa$total_atrasos > 0 & base_pessoa$numero_vezes_1m_mais_sem_pagar == 0 & base_pessoa$GENERO == "FEMININO" & base_pessoa$LIMITE_CREDITO_CARTAO < mean(base_pessoa$LIMITE_CREDITO_CARTAO), mean(base_pessoa$SCORE_FRAUDE[!is.na(base_pessoa$SCORE_FRAUDE_IMPUTADO) & base_pessoa$FRAUDE == "0" & base_pessoa$total_atrasos > 0 & base_pessoa$numero_vezes_1m_mais_sem_pagar == 0 & base_pessoa$GENERO == "FEMININO" & base_pessoa$LIMITE_CREDITO_CARTAO < mean(base_pessoa$LIMITE_CREDITO_CARTAO)]) ,base_pessoa$SCORE_FRAUDE_IMPUTADO )
base_pessoa$SCORE_FRAUDE_IMPUTADO <- ifelse(is.na(base_pessoa$SCORE_FRAUDE_IMPUTADO) & base_pessoa$FRAUDE == "0" & base_pessoa$numero_vezes_1m_mais_sem_pagar > 0 & base_pessoa$numero_vezes_4m_mais_sem_pagar ==0 & base_pessoa$GENERO == "FEMININO" & base_pessoa$LIMITE_CREDITO_CARTAO < mean(base_pessoa$LIMITE_CREDITO_CARTAO) , mean(base_pessoa$SCORE_FRAUDE[!is.na(base_pessoa$SCORE_FRAUDE_IMPUTADO) & base_pessoa$FRAUDE == "0" & base_pessoa$numero_vezes_1m_mais_sem_pagar > 0 & base_pessoa$numero_vezes_4m_mais_sem_pagar ==0 & base_pessoa$GENERO == "FEMININO" & base_pessoa$LIMITE_CREDITO_CARTAO < mean(base_pessoa$LIMITE_CREDITO_CARTAO)]) ,base_pessoa$SCORE_FRAUDE_IMPUTADO )
base_pessoa$SCORE_FRAUDE_IMPUTADO <- ifelse(is.na(base_pessoa$SCORE_FRAUDE_IMPUTADO) & base_pessoa$FRAUDE == "0" & base_pessoa$numero_vezes_4m_mais_sem_pagar > 0 & base_pessoa$GENERO == "FEMININO" & base_pessoa$LIMITE_CREDITO_CARTAO < mean(base_pessoa$LIMITE_CREDITO_CARTAO) , mean(base_pessoa$SCORE_FRAUDE[!is.na(base_pessoa$SCORE_FRAUDE_IMPUTADO) & base_pessoa$FRAUDE == "0" & base_pessoa$numero_vezes_4m_mais_sem_pagar > 0 & base_pessoa$GENERO == "FEMININO" & base_pessoa$LIMITE_CREDITO_CARTAO < mean(base_pessoa$LIMITE_CREDITO_CARTAO)] ) ,base_pessoa$SCORE_FRAUDE_IMPUTADO )
base_pessoa$SCORE_FRAUDE_IMPUTADO <- ifelse(is.na(base_pessoa$SCORE_FRAUDE_IMPUTADO) & base_pessoa$FRAUDE == "0" & base_pessoa$total_atrasos == 0 & base_pessoa$GENERO == "FEMININO" & base_pessoa$LIMITE_CREDITO_CARTAO > mean(base_pessoa$LIMITE_CREDITO_CARTAO), mean(base_pessoa$SCORE_FRAUDE[!is.na(base_pessoa$SCORE_FRAUDE_IMPUTADO) & base_pessoa$FRAUDE == "0" & base_pessoa$total_atrasos == 0 & base_pessoa$GENERO == "FEMININO" & base_pessoa$LIMITE_CREDITO_CARTAO > mean(base_pessoa$LIMITE_CREDITO_CARTAO)]) ,base_pessoa$SCORE_FRAUDE_IMPUTADO )
base_pessoa$SCORE_FRAUDE_IMPUTADO <- ifelse(is.na(base_pessoa$SCORE_FRAUDE_IMPUTADO) & base_pessoa$FRAUDE == "0" & base_pessoa$total_atrasos > 0 & base_pessoa$numero_vezes_1m_mais_sem_pagar == 0 & base_pessoa$GENERO == "FEMININO" & base_pessoa$LIMITE_CREDITO_CARTAO > mean(base_pessoa$LIMITE_CREDITO_CARTAO), mean(base_pessoa$SCORE_FRAUDE[!is.na(base_pessoa$SCORE_FRAUDE_IMPUTADO) & base_pessoa$FRAUDE == "0" & base_pessoa$total_atrasos > 0 & base_pessoa$numero_vezes_1m_mais_sem_pagar == 0 & base_pessoa$GENERO == "FEMININO" & base_pessoa$LIMITE_CREDITO_CARTAO > mean(base_pessoa$LIMITE_CREDITO_CARTAO)]) ,base_pessoa$SCORE_FRAUDE_IMPUTADO )
base_pessoa$SCORE_FRAUDE_IMPUTADO <- ifelse(is.na(base_pessoa$SCORE_FRAUDE_IMPUTADO) & base_pessoa$FRAUDE == "0" & base_pessoa$numero_vezes_1m_mais_sem_pagar > 0 & base_pessoa$numero_vezes_4m_mais_sem_pagar ==0 & base_pessoa$GENERO == "FEMININO" & base_pessoa$LIMITE_CREDITO_CARTAO > mean(base_pessoa$LIMITE_CREDITO_CARTAO) , mean(base_pessoa$SCORE_FRAUDE[!is.na(base_pessoa$SCORE_FRAUDE_IMPUTADO) & base_pessoa$FRAUDE == "0" & base_pessoa$numero_vezes_1m_mais_sem_pagar > 0 & base_pessoa$numero_vezes_4m_mais_sem_pagar ==0 & base_pessoa$GENERO == "FEMININO" & base_pessoa$LIMITE_CREDITO_CARTAO > mean(base_pessoa$LIMITE_CREDITO_CARTAO)]) ,base_pessoa$SCORE_FRAUDE_IMPUTADO )
base_pessoa$SCORE_FRAUDE_IMPUTADO <- ifelse(is.na(base_pessoa$SCORE_FRAUDE_IMPUTADO) & base_pessoa$FRAUDE == "0" & base_pessoa$numero_vezes_4m_mais_sem_pagar > 0 & base_pessoa$GENERO == "FEMININO" & base_pessoa$LIMITE_CREDITO_CARTAO > mean(base_pessoa$LIMITE_CREDITO_CARTAO) , mean(base_pessoa$SCORE_FRAUDE[!is.na(base_pessoa$SCORE_FRAUDE_IMPUTADO) & base_pessoa$FRAUDE == "0" & base_pessoa$numero_vezes_4m_mais_sem_pagar > 0 & base_pessoa$GENERO == "FEMININO" & base_pessoa$LIMITE_CREDITO_CARTAO > mean(base_pessoa$LIMITE_CREDITO_CARTAO)] ) ,base_pessoa$SCORE_FRAUDE_IMPUTADO )

#Imputando valores para o grupo de homens, e depois o de mulheres, onde:
#O ESCORE_FRAUDE ? missing,
#? UM CARTAO FRAUDADO,
#ATRASOS: (nao tem, teve <=2 semanas, teve >2semanas <=1 mes, teve >1m)
#E LIMITE CREDITO DO CARTAO MENOR QUE A MEDIA
base_pessoa$SCORE_FRAUDE_IMPUTADO <- ifelse(is.na(base_pessoa$SCORE_FRAUDE_IMPUTADO) & base_pessoa$FRAUDE == "1" & base_pessoa$total_atrasos == 0 & base_pessoa$GENERO == "MASCULINO" & base_pessoa$LIMITE_CREDITO_CARTAO < mean(base_pessoa$LIMITE_CREDITO_CARTAO), mean(base_pessoa$SCORE_FRAUDE[!is.na(base_pessoa$SCORE_FRAUDE_IMPUTADO) & base_pessoa$FRAUDE == "1" & base_pessoa$total_atrasos == 0 & base_pessoa$GENERO == "MASCULINO" & base_pessoa$LIMITE_CREDITO_CARTAO < mean(base_pessoa$LIMITE_CREDITO_CARTAO)]) ,base_pessoa$SCORE_FRAUDE_IMPUTADO )
base_pessoa$SCORE_FRAUDE_IMPUTADO <- ifelse(is.na(base_pessoa$SCORE_FRAUDE_IMPUTADO) & base_pessoa$FRAUDE == "1" & base_pessoa$total_atrasos > 0 & base_pessoa$numero_vezes_1m_mais_sem_pagar == 0 & base_pessoa$GENERO == "MASCULINO" & base_pessoa$LIMITE_CREDITO_CARTAO < mean(base_pessoa$LIMITE_CREDITO_CARTAO), mean(base_pessoa$SCORE_FRAUDE[!is.na(base_pessoa$SCORE_FRAUDE_IMPUTADO) & base_pessoa$FRAUDE == "1" & base_pessoa$total_atrasos > 0 & base_pessoa$numero_vezes_1m_mais_sem_pagar == 0 & base_pessoa$GENERO == "MASCULINO" & base_pessoa$LIMITE_CREDITO_CARTAO < mean(base_pessoa$LIMITE_CREDITO_CARTAO)]) ,base_pessoa$SCORE_FRAUDE_IMPUTADO )
base_pessoa$SCORE_FRAUDE_IMPUTADO <- ifelse(is.na(base_pessoa$SCORE_FRAUDE_IMPUTADO) & base_pessoa$FRAUDE == "1" & base_pessoa$numero_vezes_1m_mais_sem_pagar > 0 & base_pessoa$numero_vezes_4m_mais_sem_pagar ==0 & base_pessoa$GENERO == "MASCULINO" & base_pessoa$LIMITE_CREDITO_CARTAO < mean(base_pessoa$LIMITE_CREDITO_CARTAO) , mean(base_pessoa$SCORE_FRAUDE[!is.na(base_pessoa$SCORE_FRAUDE_IMPUTADO) & base_pessoa$FRAUDE == "1" & base_pessoa$numero_vezes_1m_mais_sem_pagar > 0 & base_pessoa$numero_vezes_4m_mais_sem_pagar ==0 & base_pessoa$GENERO == "MASCULINO" & base_pessoa$LIMITE_CREDITO_CARTAO < mean(base_pessoa$LIMITE_CREDITO_CARTAO)]) ,base_pessoa$SCORE_FRAUDE_IMPUTADO )
base_pessoa$SCORE_FRAUDE_IMPUTADO <- ifelse(is.na(base_pessoa$SCORE_FRAUDE_IMPUTADO) & base_pessoa$FRAUDE == "1" & base_pessoa$numero_vezes_4m_mais_sem_pagar > 0 & base_pessoa$GENERO == "MASCULINO" & base_pessoa$LIMITE_CREDITO_CARTAO < mean(base_pessoa$LIMITE_CREDITO_CARTAO) , mean(base_pessoa$SCORE_FRAUDE[!is.na(base_pessoa$SCORE_FRAUDE_IMPUTADO) & base_pessoa$FRAUDE == "1" & base_pessoa$numero_vezes_4m_mais_sem_pagar > 0 & base_pessoa$GENERO == "MASCULINO" & base_pessoa$LIMITE_CREDITO_CARTAO < mean(base_pessoa$LIMITE_CREDITO_CARTAO)] ) ,base_pessoa$SCORE_FRAUDE_IMPUTADO )
base_pessoa$SCORE_FRAUDE_IMPUTADO <- ifelse(is.na(base_pessoa$SCORE_FRAUDE_IMPUTADO) & base_pessoa$FRAUDE == "1" & base_pessoa$total_atrasos == 0 & base_pessoa$GENERO == "MASCULINO" & base_pessoa$LIMITE_CREDITO_CARTAO > mean(base_pessoa$LIMITE_CREDITO_CARTAO), mean(base_pessoa$SCORE_FRAUDE[!is.na(base_pessoa$SCORE_FRAUDE_IMPUTADO) & base_pessoa$FRAUDE == "1" & base_pessoa$total_atrasos == 0 & base_pessoa$GENERO == "MASCULINO" & base_pessoa$LIMITE_CREDITO_CARTAO > mean(base_pessoa$LIMITE_CREDITO_CARTAO)]) ,base_pessoa$SCORE_FRAUDE_IMPUTADO )
base_pessoa$SCORE_FRAUDE_IMPUTADO <- ifelse(is.na(base_pessoa$SCORE_FRAUDE_IMPUTADO) & base_pessoa$FRAUDE == "1" & base_pessoa$total_atrasos > 0 & base_pessoa$numero_vezes_1m_mais_sem_pagar == 0 & base_pessoa$GENERO == "MASCULINO" & base_pessoa$LIMITE_CREDITO_CARTAO > mean(base_pessoa$LIMITE_CREDITO_CARTAO), mean(base_pessoa$SCORE_FRAUDE[!is.na(base_pessoa$SCORE_FRAUDE_IMPUTADO) & base_pessoa$FRAUDE == "1" & base_pessoa$total_atrasos > 0 & base_pessoa$numero_vezes_1m_mais_sem_pagar == 0 & base_pessoa$GENERO == "MASCULINO" & base_pessoa$LIMITE_CREDITO_CARTAO > mean(base_pessoa$LIMITE_CREDITO_CARTAO)]) ,base_pessoa$SCORE_FRAUDE_IMPUTADO )
base_pessoa$SCORE_FRAUDE_IMPUTADO <- ifelse(is.na(base_pessoa$SCORE_FRAUDE_IMPUTADO) & base_pessoa$FRAUDE == "1" & base_pessoa$numero_vezes_1m_mais_sem_pagar > 0 & base_pessoa$numero_vezes_4m_mais_sem_pagar ==0 & base_pessoa$GENERO == "MASCULINO" & base_pessoa$LIMITE_CREDITO_CARTAO > mean(base_pessoa$LIMITE_CREDITO_CARTAO) , mean(base_pessoa$SCORE_FRAUDE[!is.na(base_pessoa$SCORE_FRAUDE_IMPUTADO) & base_pessoa$FRAUDE == "1" & base_pessoa$numero_vezes_1m_mais_sem_pagar > 0 & base_pessoa$numero_vezes_4m_mais_sem_pagar ==0 & base_pessoa$GENERO == "MASCULINO" & base_pessoa$LIMITE_CREDITO_CARTAO > mean(base_pessoa$LIMITE_CREDITO_CARTAO)]) ,base_pessoa$SCORE_FRAUDE_IMPUTADO )
base_pessoa$SCORE_FRAUDE_IMPUTADO <- ifelse(is.na(base_pessoa$SCORE_FRAUDE_IMPUTADO) & base_pessoa$FRAUDE == "1" & base_pessoa$numero_vezes_4m_mais_sem_pagar > 0 & base_pessoa$GENERO == "MASCULINO" & base_pessoa$LIMITE_CREDITO_CARTAO > mean(base_pessoa$LIMITE_CREDITO_CARTAO) , mean(base_pessoa$SCORE_FRAUDE[!is.na(base_pessoa$SCORE_FRAUDE_IMPUTADO) & base_pessoa$FRAUDE == "1" & base_pessoa$numero_vezes_4m_mais_sem_pagar > 0 & base_pessoa$GENERO == "MASCULINO" & base_pessoa$LIMITE_CREDITO_CARTAO > mean(base_pessoa$LIMITE_CREDITO_CARTAO)] ) ,base_pessoa$SCORE_FRAUDE_IMPUTADO )

#Imputando valores para o grupo de homens, e depois o de mulheres, onde:
#O ESCORE_FRAUDE ? missing,
#? UM CARTAO FRAUDADO,
#ATRASOS: (nao tem, teve <=2 semanas, teve >2semanas <=1 mes, teve >1m)
#E LIMITE CREDITO DO CARTAO MAIOR QUE A MEDIA

base_pessoa$SCORE_FRAUDE_IMPUTADO <- ifelse(is.na(base_pessoa$SCORE_FRAUDE_IMPUTADO) & base_pessoa$FRAUDE == "1" & base_pessoa$total_atrasos == 0 & base_pessoa$GENERO == "FEMININO" & base_pessoa$LIMITE_CREDITO_CARTAO < mean(base_pessoa$LIMITE_CREDITO_CARTAO), mean(base_pessoa$SCORE_FRAUDE[!is.na(base_pessoa$SCORE_FRAUDE_IMPUTADO) & base_pessoa$FRAUDE == "1" & base_pessoa$total_atrasos == 0 & base_pessoa$GENERO == "FEMININO" & base_pessoa$LIMITE_CREDITO_CARTAO < mean(base_pessoa$LIMITE_CREDITO_CARTAO)]) ,base_pessoa$SCORE_FRAUDE_IMPUTADO )
base_pessoa$SCORE_FRAUDE_IMPUTADO <- ifelse(is.na(base_pessoa$SCORE_FRAUDE_IMPUTADO) & base_pessoa$FRAUDE == "1" & base_pessoa$total_atrasos > 0 & base_pessoa$numero_vezes_1m_mais_sem_pagar == 0 & base_pessoa$GENERO == "FEMININO" & base_pessoa$LIMITE_CREDITO_CARTAO < mean(base_pessoa$LIMITE_CREDITO_CARTAO), mean(base_pessoa$SCORE_FRAUDE[!is.na(base_pessoa$SCORE_FRAUDE_IMPUTADO) & base_pessoa$FRAUDE == "1" & base_pessoa$total_atrasos > 0 & base_pessoa$numero_vezes_1m_mais_sem_pagar == 0 & base_pessoa$GENERO == "FEMININO" & base_pessoa$LIMITE_CREDITO_CARTAO < mean(base_pessoa$LIMITE_CREDITO_CARTAO)]) ,base_pessoa$SCORE_FRAUDE_IMPUTADO )
base_pessoa$SCORE_FRAUDE_IMPUTADO <- ifelse(is.na(base_pessoa$SCORE_FRAUDE_IMPUTADO) & base_pessoa$FRAUDE == "1" & base_pessoa$numero_vezes_1m_mais_sem_pagar > 0 & base_pessoa$numero_vezes_4m_mais_sem_pagar ==0 & base_pessoa$GENERO == "FEMININO" & base_pessoa$LIMITE_CREDITO_CARTAO < mean(base_pessoa$LIMITE_CREDITO_CARTAO) , mean(base_pessoa$SCORE_FRAUDE[!is.na(base_pessoa$SCORE_FRAUDE_IMPUTADO) & base_pessoa$FRAUDE == "1" & base_pessoa$numero_vezes_1m_mais_sem_pagar > 0 & base_pessoa$numero_vezes_4m_mais_sem_pagar ==0 & base_pessoa$GENERO == "FEMININO" & base_pessoa$LIMITE_CREDITO_CARTAO < mean(base_pessoa$LIMITE_CREDITO_CARTAO)]) ,base_pessoa$SCORE_FRAUDE_IMPUTADO )
base_pessoa$SCORE_FRAUDE_IMPUTADO <- ifelse(is.na(base_pessoa$SCORE_FRAUDE_IMPUTADO) & base_pessoa$FRAUDE == "1" & base_pessoa$numero_vezes_4m_mais_sem_pagar > 0 & base_pessoa$GENERO == "FEMININO" & base_pessoa$LIMITE_CREDITO_CARTAO < mean(base_pessoa$LIMITE_CREDITO_CARTAO) , mean(base_pessoa$SCORE_FRAUDE[!is.na(base_pessoa$SCORE_FRAUDE_IMPUTADO) & base_pessoa$FRAUDE == "1" & base_pessoa$numero_vezes_4m_mais_sem_pagar > 0 & base_pessoa$GENERO == "FEMININO" & base_pessoa$LIMITE_CREDITO_CARTAO < mean(base_pessoa$LIMITE_CREDITO_CARTAO)] ) ,base_pessoa$SCORE_FRAUDE_IMPUTADO )
base_pessoa$SCORE_FRAUDE_IMPUTADO <- ifelse(is.na(base_pessoa$SCORE_FRAUDE_IMPUTADO) & base_pessoa$FRAUDE == "1" & base_pessoa$total_atrasos == 0 & base_pessoa$GENERO == "FEMININO" & base_pessoa$LIMITE_CREDITO_CARTAO > mean(base_pessoa$LIMITE_CREDITO_CARTAO), mean(base_pessoa$SCORE_FRAUDE[!is.na(base_pessoa$SCORE_FRAUDE_IMPUTADO) & base_pessoa$FRAUDE == "1" & base_pessoa$total_atrasos == 0 & base_pessoa$GENERO == "FEMININO" & base_pessoa$LIMITE_CREDITO_CARTAO > mean(base_pessoa$LIMITE_CREDITO_CARTAO)]) ,base_pessoa$SCORE_FRAUDE_IMPUTADO )
base_pessoa$SCORE_FRAUDE_IMPUTADO <- ifelse(is.na(base_pessoa$SCORE_FRAUDE_IMPUTADO) & base_pessoa$FRAUDE == "1" & base_pessoa$total_atrasos > 0 & base_pessoa$numero_vezes_1m_mais_sem_pagar == 0 & base_pessoa$GENERO == "FEMININO" & base_pessoa$LIMITE_CREDITO_CARTAO > mean(base_pessoa$LIMITE_CREDITO_CARTAO), mean(base_pessoa$SCORE_FRAUDE[!is.na(base_pessoa$SCORE_FRAUDE_IMPUTADO) & base_pessoa$FRAUDE == "1" & base_pessoa$total_atrasos > 0 & base_pessoa$numero_vezes_1m_mais_sem_pagar == 0 & base_pessoa$GENERO == "FEMININO" & base_pessoa$LIMITE_CREDITO_CARTAO > mean(base_pessoa$LIMITE_CREDITO_CARTAO)]) ,base_pessoa$SCORE_FRAUDE_IMPUTADO )
base_pessoa$SCORE_FRAUDE_IMPUTADO <- ifelse(is.na(base_pessoa$SCORE_FRAUDE_IMPUTADO) & base_pessoa$FRAUDE == "1" & base_pessoa$numero_vezes_1m_mais_sem_pagar > 0 & base_pessoa$numero_vezes_4m_mais_sem_pagar ==0 & base_pessoa$GENERO == "FEMININO" & base_pessoa$LIMITE_CREDITO_CARTAO > mean(base_pessoa$LIMITE_CREDITO_CARTAO) , mean(base_pessoa$SCORE_FRAUDE[!is.na(base_pessoa$SCORE_FRAUDE_IMPUTADO) & base_pessoa$FRAUDE == "1" & base_pessoa$numero_vezes_1m_mais_sem_pagar > 0 & base_pessoa$numero_vezes_4m_mais_sem_pagar ==0 & base_pessoa$GENERO == "FEMININO" & base_pessoa$LIMITE_CREDITO_CARTAO > mean(base_pessoa$LIMITE_CREDITO_CARTAO)]) ,base_pessoa$SCORE_FRAUDE_IMPUTADO )
base_pessoa$SCORE_FRAUDE_IMPUTADO <- ifelse(is.na(base_pessoa$SCORE_FRAUDE_IMPUTADO) & base_pessoa$FRAUDE == "1" & base_pessoa$numero_vezes_4m_mais_sem_pagar > 0 & base_pessoa$GENERO == "FEMININO" & base_pessoa$LIMITE_CREDITO_CARTAO > mean(base_pessoa$LIMITE_CREDITO_CARTAO) , mean(base_pessoa$SCORE_FRAUDE[!is.na(base_pessoa$SCORE_FRAUDE_IMPUTADO) & base_pessoa$FRAUDE == "1" & base_pessoa$numero_vezes_4m_mais_sem_pagar > 0 & base_pessoa$GENERO == "FEMININO" & base_pessoa$LIMITE_CREDITO_CARTAO > mean(base_pessoa$LIMITE_CREDITO_CARTAO)] ) ,base_pessoa$SCORE_FRAUDE_IMPUTADO )


sapply(base_pessoa, function(x) sum(is.na(x)))

#excluindo campos muito missing
base_pessoa <- select(base_pessoa, -c(SCORE_CREDITO_BUREAU2, COD_PROFISSAO_BUREAU1, SCORE_FRAUDE))

base_pessoa$FRAUDE[is.na(base_pessoa$SCORE_CREDITO_BUREAU1)]
table(base_pessoa$FRAUDE[is.na(base_pessoa$renda_con)])
#nenhum caso dentre esses com missing,

#removendo missings
rm(base_pessoa_sem_missing)
base_pessoa_sem_missing <- filter(base_pessoa, !is.na(base_pessoa$renda_con), !is.na(base_pessoa$SCORE_CREDITO_BUREAU1), !is.na(base_pessoa$idade))

sapply(base_pessoa_sem_missing, function(x) sum(is.na(x)))

base_transacao <- read.csv("C:\\Bases - Hackapan\\base_tratada_transacoes.csv", header=T)
base_pessoa_sem_missing <- inner_join(base_pessoa_sem_missing, base_transacao, by="ID")

sapply(base_pessoa_sem_missing, function(x) sum(is.na(x)))

#adicionando indicador de anormalidade de transacao
anormalidade <- read.csv("C:\\Bases - Hackapan\\indicador_transacoes_anormais.csv", header=TRUE)

base_pessoa_sem_missing <- left_join(base_pessoa_sem_missing,anormalidade, id="ID" )
base_pessoa_sem_missing[is.na(base_pessoa_sem_missing)] <- 0

base_pessoa_sem_missing$UF_COMERCIAL_INFORMADO <- NULL

base_pessoa_sem_missing$REGIAO <- ifelse(base_pessoa_sem_missing$UF_RESIDENCIAL_INFORMADO=="AC" |
                                           base_pessoa_sem_missing$UF_RESIDENCIAL_INFORMADO=="AP" |
                                           base_pessoa_sem_missing$UF_RESIDENCIAL_INFORMADO=="AM" |
                                           base_pessoa_sem_missing$UF_RESIDENCIAL_INFORMADO=="PA" |
                                           base_pessoa_sem_missing$UF_RESIDENCIAL_INFORMADO=="RO" |
                                           base_pessoa_sem_missing$UF_RESIDENCIAL_INFORMADO=="RR" |
                                           base_pessoa_sem_missing$UF_RESIDENCIAL_INFORMADO=="TO",
                                         "NORTE",
                                         ifelse(base_pessoa_sem_missing$UF_RESIDENCIAL_INFORMADO=="AL" |
                                                  base_pessoa_sem_missing$UF_RESIDENCIAL_INFORMADO=="BA" |
                                                  base_pessoa_sem_missing$UF_RESIDENCIAL_INFORMADO=="CE" |
                                                  base_pessoa_sem_missing$UF_RESIDENCIAL_INFORMADO=="MA" |
                                                  base_pessoa_sem_missing$UF_RESIDENCIAL_INFORMADO=="PB" |
                                                  base_pessoa_sem_missing$UF_RESIDENCIAL_INFORMADO=="PE" |
                                                  base_pessoa_sem_missing$UF_RESIDENCIAL_INFORMADO=="PI" |
                                                  base_pessoa_sem_missing$UF_RESIDENCIAL_INFORMADO=="RN" |
                                                  base_pessoa_sem_missing$UF_RESIDENCIAL_INFORMADO=="SE",
                                                "NORDESTE", 
                                                ifelse(base_pessoa_sem_missing$UF_RESIDENCIAL_INFORMADO=="DF" |
                                                         base_pessoa_sem_missing$UF_RESIDENCIAL_INFORMADO=="GO" |
                                                         base_pessoa_sem_missing$UF_RESIDENCIAL_INFORMADO=="MT" |
                                                         base_pessoa_sem_missing$UF_RESIDENCIAL_INFORMADO=="MS", "CENTRO-OESTE",
                                                       ifelse(base_pessoa_sem_missing$UF_RESIDENCIAL_INFORMADO=="ES" |
                                                                base_pessoa_sem_missing$UF_RESIDENCIAL_INFORMADO=="MG" |
                                                                base_pessoa_sem_missing$UF_RESIDENCIAL_INFORMADO=="MT" |
                                                                base_pessoa_sem_missing$UF_RESIDENCIAL_INFORMADO=="RJ"|
                                                                base_pessoa_sem_missing$UF_RESIDENCIAL_INFORMADO=="SP","SUDESTE",
                                                              ifelse(base_pessoa_sem_missing$UF_RESIDENCIAL_INFORMADO=="PR" |
                                                                       base_pessoa_sem_missing$UF_RESIDENCIAL_INFORMADO=="RS" |
                                                                       base_pessoa_sem_missing$UF_RESIDENCIAL_INFORMADO=="SC","SUL",NA)))))



write.csv(base_pessoa_sem_missing, "C:\\Bases - Hackapan\\base_unificada_sem_missing.csv", row.names=FALSE)



# Preparacao para Bancos Finais -------------------------------------------


setwd('Bases - Hackapan')
base1 <- data.table::fread('BASE_ATRASOS_HACKAPAN.txt', dec = ',', stringsAsFactors = TRUE)
base2 <- data.table::fread('BASE_CARTOES_HACKAPAN.txt', dec = ',', stringsAsFactors = TRUE)
base3 <- data.table::fread('BASE_TRANSACOES_HACKAPAN.txt', dec = ',', stringsAsFactors = TRUE)

# Tratamento de base ------------------------------------------------------------

base_pessoa <- base2
str(base_pessoa)

## tratamento de variaveis
base_pessoa$DT_CONTRATACAO <- as.Date(base_pessoa$DT_CONTRATACAO)
base_pessoa$DATA_ATIVACAO <- as.Date(base_pessoa$DATA_ATIVACAO)
base_pessoa$DATA_NASCIMENTO_CLIENTE <- as.Date(base_pessoa$DATA_NASCIMENTO_CLIENTE)
base_pessoa$ID <- as.factor(base_pessoa$ID)
base_pessoa$FRAUDE <- as.factor(base_pessoa$FRAUDE)
base_pessoa$FLAG_DESENVOLVIMENTO <- as.factor(base_pessoa$FLAG_DESENVOLVIMENTO)
base_pessoa$COD_PROFISSAO_BUREAU1 <- as.factor(base_pessoa$COD_PROFISSAO_BUREAU1)

# Estudo de variaveis -----------------------------------------------------

## variavel tempo ate a ativacao
base_pessoa$tempo_ativacao <- base_pessoa$DATA_ATIVACAO %>% 
  difftime(base_pessoa$DT_CONTRATACAO, units= 'days') %>% 
  as.numeric()

## variave idade no dia da ativacao do cartao
base_pessoa$idade <- base_pessoa$DATA_ATIVACAO %>% 
  difftime(base_pessoa$DATA_NASCIMENTO_CLIENTE, units= 'days') %>% 
  as.numeric()

base_pessoa$idade <- base_pessoa$idade/365
base_pessoa$idade <- ifelse(base_pessoa$idade > 0, base_pessoa$idade, 'NA')
base_pessoa$idade <- as.numeric(base_pessoa$idade)

## preenchimento de NAs por 0 para comparacao
base_pessoa1 <- base_pessoa[,1:18]
base_pessoa2 <- base_pessoa[,19:49]
base_pessoa3 <- base_pessoa[,50:ncol(base_pessoa)]

base_pessoa2[is.na(base_pessoa2)] <- 0
base_pessoa <- cbind(base_pessoa1, base_pessoa2, base_pessoa3)

## consistencia do cep
base_pessoa$cep_con <- ifelse(base_pessoa$CEP_RESIDENCIAL_INFORMADO == base_pessoa$CEP1_BUREAU1 |
                                base_pessoa$CEP_RESIDENCIAL_INFORMADO == base_pessoa$CEP2_BUREAU1 |
                                base_pessoa$CEP_RESIDENCIAL_INFORMADO == base_pessoa$CEP3_BUREAU1 |
                                base_pessoa$CEP_RESIDENCIAL_INFORMADO == base_pessoa$CEP1_BUREAU2 |
                                base_pessoa$CEP_RESIDENCIAL_INFORMADO == base_pessoa$CEP2_BUREAU2 |
                                base_pessoa$CEP_RESIDENCIAL_INFORMADO == base_pessoa$CEP3_BUREAU2 |
                                base_pessoa$CEP_RESIDENCIAL_INFORMADO == base_pessoa$CEP4_BUREAU2 |
                                base_pessoa$CEP_COMERCIAL_INFORMADO == base_pessoa$CEP1_BUREAU1 |
                                base_pessoa$CEP_COMERCIAL_INFORMADO == base_pessoa$CEP2_BUREAU1 |
                                base_pessoa$CEP_COMERCIAL_INFORMADO == base_pessoa$CEP3_BUREAU1 |
                                base_pessoa$CEP_COMERCIAL_INFORMADO == base_pessoa$CEP1_BUREAU2 |
                                base_pessoa$CEP_COMERCIAL_INFORMADO == base_pessoa$CEP2_BUREAU2 |
                                base_pessoa$CEP_COMERCIAL_INFORMADO == base_pessoa$CEP3_BUREAU2 |
                                base_pessoa$CEP_COMERCIAL_INFORMADO == base_pessoa$CEP4_BUREAU2,
                              0, 1)

## consistencia do DDD
base_pessoa$ddd_con <- ifelse(base_pessoa$DDD_CELULAR == base_pessoa$DDD1_BUREAU1 |
                                base_pessoa$DDD_CELULAR == base_pessoa$DDD2_BUREAU1 |
                                base_pessoa$DDD_CELULAR == base_pessoa$DDD3_BUREAU1 |
                                base_pessoa$DDD_CELULAR == base_pessoa$DDD4_BUREAU1 |
                                base_pessoa$DDD_CELULAR == base_pessoa$DDD5_BUREAU1 |
                                base_pessoa$DDD_RESIDENCIAL_INFORMADO == base_pessoa$DDD1_BUREAU1 |
                                base_pessoa$DDD_RESIDENCIAL_INFORMADO == base_pessoa$DDD2_BUREAU1 |
                                base_pessoa$DDD_RESIDENCIAL_INFORMADO == base_pessoa$DDD3_BUREAU1 |
                                base_pessoa$DDD_RESIDENCIAL_INFORMADO == base_pessoa$DDD4_BUREAU1 |
                                base_pessoa$DDD_RESIDENCIAL_INFORMADO == base_pessoa$DDD5_BUREAU1 |
                                base_pessoa$DDD_COMERCIAL_INFORMADO == base_pessoa$DDD1_BUREAU1 |
                                base_pessoa$DDD_COMERCIAL_INFORMADO == base_pessoa$DDD2_BUREAU1 |
                                base_pessoa$DDD_COMERCIAL_INFORMADO == base_pessoa$DDD3_BUREAU1 |
                                base_pessoa$DDD_COMERCIAL_INFORMADO == base_pessoa$DDD4_BUREAU1 |
                                base_pessoa$DDD_COMERCIAL_INFORMADO == base_pessoa$DDD5_BUREAU1,
                              0, 1)

## consistencia de UF
base_pessoa$uf_con <- ifelse(as.character(base_pessoa$UF_RESIDENCIAL_INFORMADO) == as.character(base_pessoa$UF1_BUREAU1) |
                               as.character(base_pessoa$UF_RESIDENCIAL_INFORMADO) == as.character(base_pessoa$UF2_BUREAU1) |
                               as.character(base_pessoa$UF_RESIDENCIAL_INFORMADO) == as.character(base_pessoa$UF3_BUREAU1) |
                               as.character(base_pessoa$UF_RESIDENCIAL_INFORMADO) == as.character(base_pessoa$UF4_BUREAU1) |
                               as.character(base_pessoa$UF_RESIDENCIAL_INFORMADO) == as.character(base_pessoa$UF5_BUREAU1) |
                               as.character(base_pessoa$UF_COMERCIAL_INFORMADO) == as.character(base_pessoa$UF1_BUREAU1) |
                               as.character(base_pessoa$UF_COMERCIAL_INFORMADO) == as.character(base_pessoa$UF2_BUREAU1) |
                               as.character(base_pessoa$UF_COMERCIAL_INFORMADO) == as.character(base_pessoa$UF3_BUREAU1) |
                               as.character(base_pessoa$UF_COMERCIAL_INFORMADO) == as.character(base_pessoa$UF4_BUREAU1) |
                               as.character(base_pessoa$UF_COMERCIAL_INFORMADO) == as.character(base_pessoa$UF5_BUREAU1),
                             0, 1)

## consistencia de cidade
base_pessoa$cid_con <- ifelse(as.character(base_pessoa$CIDADE_RESIDENCIAL_INFORMADO) == as.character(base_pessoa$CIDADE1_BUREAU1) |
                                as.character(base_pessoa$CIDADE_RESIDENCIAL_INFORMADO) == as.character(base_pessoa$CIDADE2_BUREAU1) |
                                as.character(base_pessoa$CIDADE_RESIDENCIAL_INFORMADO) == as.character(base_pessoa$CIDADE3_BUREAU1) |
                                as.character(base_pessoa$CIDADE_RESIDENCIAL_INFORMADO) == as.character(base_pessoa$CIDADE4_BUREAU1) |
                                as.character(base_pessoa$CIDADE_RESIDENCIAL_INFORMADO) == as.character(base_pessoa$CIDADE5_BUREAU1),
                              0, 1)

## diferenca de renda informada / renda bureau
base_pessoa$renda1_con <- base_pessoa$RENDA_INFORMADA - base_pessoa$RENDA_BUREAU1
base_pessoa$renda2_con <- base_pessoa$RENDA_INFORMADA - base_pessoa$RENDA_BUREAU2
base_pessoa$renda_con <- (base_pessoa$renda1_con + base_pessoa$renda2_con)/2

## variavel email categorizada


## filtro de variaveis
base_pessoa <- base_pessoa %>% 
  dplyr::select(-c(ESTADO_CIVIL,
                   NATURALIDADE,
                   ESCOLARIDADE,
                   DT_CONTRATACAO, 
                   DATA_ATIVACAO, 
                   DATA_NASCIMENTO_CLIENTE,
                   CEP_RESIDENCIAL_INFORMADO,
                   CEP_COMERCIAL_INFORMADO,
                   CEP1_BUREAU1,                 
                   CEP2_BUREAU1,                
                   CEP3_BUREAU1,
                   CEP1_BUREAU2,
                   CEP2_BUREAU2,
                   CEP3_BUREAU2,
                   CEP4_BUREAU2,
                   DDD_CELULAR,                 
                   DDD_RESIDENCIAL_INFORMADO,   
                   DDD_COMERCIAL_INFORMADO,     
                   DDD1_BUREAU1,              
                   DDD2_BUREAU1,                
                   DDD3_BUREAU1,                
                   DDD4_BUREAU1,                
                   DDD5_BUREAU1,                
                   UF_EMISSAO_RG,
                   UF1_BUREAU1,                 
                   UF2_BUREAU1,                 
                   UF3_BUREAU1,                 
                   UF4_BUREAU1,                 
                   UF5_BUREAU1,                 
                   CIDADE1_BUREAU1,             
                   CIDADE2_BUREAU1,             
                   CIDADE3_BUREAU1,             
                   CIDADE4_BUREAU1,             
                   CIDADE5_BUREAU1,  
                   RENDA_BUREAU1,               
                   RENDA_BUREAU2))


# Salvando a base
write.csv(base_pessoa, "C:/Bases - Hackapan/base_final.csv", row.names=FALSE)


# Plot de Graficos --------------------------------------------------------


cols <- c("total_dias_observados",                                  
       "LIMITE_CREDITO_CARTAO",                           
       "prop_aprovacao_6m",                                     
       "FLAG_VALIDACAO_CADASTRAL",                
       "ORIGEM_VENDA_CONTRATOOUTBOUND",
       "tempo_ativacao",                                             
       "SCORE_CREDITO_BUREAU1",                         
       "prop_aprovacao_3d",                                      
       "dias_atraso_acumulados",                              
       "numero_vezes_2sem_mais_sem_pagar"    
)

rows <- c(     41.9819792,
               8.1753471,
               7.7721192,
               6.2161017,
               4.5907103,
               3.6894673,
               3.4566451,
               3.3841943,
               2.0694504,
               1.6974908
)
data <- data.frame(cols, rows)

data <- arrange(data, rows)
# Axis treated as discrete variable
#data$rows<-as.factor(data$rows)


x11()
ggplot(data=data,aes(x= reorder(cols,rows),y=rows))+geom_bar(stat ="identity", color="pink", fill="white",size=2)+
  theme(text = element_text(size=10),
        axis.text.x = element_text(angle=90, hjust=1)) + labs(y = "Variable Importance") + labs(x = "") +
  coord_flip() 



