# Pacotes -----------------------------------------------------------------

library(data.table)
library(dplyr)
library(lubridate)
library(ggplot2)
library(extrafont)
library(ggthemes)
library(raptools)
library(caret)
library (RcmdrMisc)
library (tidyverse)
library (plyr)
library (pROC)
library (data.table)
library (stringr)
library (glmnet)
library (InformationValue)
library(DMwR)

# Subindo base de dados ---------------------------------------------------

base <- read.csv2('base_final.csv', sep = ',', dec = '.')
str(base)

## tratamento de variaveis
base$FRAUDE <- as.factor(base$FRAUDE)
base$FLAG_DESENVOLVIMENTO <- as.factor(base$FLAG_DESENVOLVIMENTO)
base$cep_con <- as.factor(base$cep_con)
base$ddd_con <- as.factor(base$ddd_con)
base$uf_con <- as.factor(base$uf_con)
base$cid_con <- as.factor(base$cid_con)

## separando base em treino e teste
treino <- base %>% 
  dplyr::filter(FLAG_DESENVOLVIMENTO == 1) %>% 
  dplyr::select(-c(DOMINIO_EMAIL,
                   CIDADE_RESIDENCIAL_INFORMADO,
                   renda1_con,
                   renda2_con,
                   renda_con,
                   DATA_TRANSACAO_MAIS_ANTIGA,
                   DATA_TRANSACAO_MAIS_RECENTE,
                   FLAG_DESENVOLVIMENTO))

teste <- base %>% 
  dplyr::filter(FLAG_DESENVOLVIMENTO == 0) %>% 
  dplyr::select(-c(DOMINIO_EMAIL,
                   CIDADE_RESIDENCIAL_INFORMADO,
                   renda1_con,
                   renda2_con,
                   renda_con,
                   DATA_TRANSACAO_MAIS_ANTIGA,
                   DATA_TRANSACAO_MAIS_RECENTE,
                   FLAG_DESENVOLVIMENTO))

# GBM ----------------------------------------------------------

## criando levels para o desfecho
levels(treino$FRAUDE) <- make.names(levels(factor(treino$FRAUDE)))
levels(teste$FRAUDE) <- make.names(levels(factor(teste$FRAUDE)))

## train control
set.seed(1)
ctrl <- caret::trainControl(method = "cv",
                            number = 10,
                            savePredictions = TRUE,
                            classProbs = TRUE,
                            verboseIter = TRUE,
                            sampling = "down")

## modelo
set.seed(1)
modelo <- caret::train(FRAUDE ~.,
                            data = dplyr::select(treino, -ID),
                            method = "gbm",
                            trControl = ctrl,
                            metric = "ROC")

summary(modelo)
getModelInfo("modelo", FALSE)[[1]]$coeff

## predicao na base de teste
pred <- predict(modelo, dplyr::select(teste, - c(FRAUDE, ID)), type = "prob")
predTRUE <- pred[,"X1"]


## curva ROC
curvaRoc <- pROC::roc(response = teste$FRAUDE, 
                             predictor = predTRUE,
                             levels = rev(levels(teste$FRAUDE)))

pROC::auc(curvaRoc)
pROC::ci.auc(curvaRoc)
plot(curvaRoc)

## predicao usando treshold escolhido
class <- factor(ifelse(predTRUE > 0.9, "X1", "X0"),
                          levels = levels(teste$FRAUDE))


## matriz de confusao
caret::confusionMatrix(data = class, 
                       reference = teste$FRAUDE, 
                       positive = "X1",
                       mode = "everything")

## teste ks
ksTest <- plyr::revalue(teste$FRAUDE, c("X1" = "1", "X0" = "0"))
InformationValue::ks_stat(as.numeric(as.character(ksTest)), predTRUE, returnKSTable = FALSE)
