install.packages("MatchIt")
install.packages("dplyr")
install.packages("comorbidity")
install.packages("demoGraphic")
install.packages("xml2")
install.packages("survival")
install.packages("survminer")
install.packages("moonBook")
install.packages("ggplot2")
install.packages("incidence")
install.packages("optmatch")
install.packages("lubridate")
install.packages("reshape")
install.packages("plyr")
install.packages("vcd")
library(reshape)
library(lubridate)
library(optmatch)
library(ggplot2)
library(MatchIt)
library(dplyr)
library(comorbidity)
library(demoGraphic)
library(survival)
library(survminer)
library(MASS)
library(moonBook)
library(incidence)
library(plyr)
library(vcd)

getwd()
setwd('C:/Users/JohnKim_PC/Downloads/data')
file_list<-list.files()
final <- NULL
for(i in 1:length(file_list)){
    file<-read.csv(file_list[i])
    final<-rbind(final, file)
    cat("\n", i)
}
dim(final)
head(final)
# data1 <-read.csv('C:/Users/JohnKim_PC/Downloads/sample1.csv', fileEncoding = "UCS-2LE")
# data2 <- read.csv('C:/Users/JohnKim_PC/Downloads/sample2.csv', fileEncoding = "UCS-2LE")
# data<- rbind(data1, data2)
str(final)
#데이터 불러오기 (폴더 안에서 하나씩)
# for ps matching 
PS<-final[,c('ID2', 'cohort_type','age','gender','SU','alpha','dpp4i','gnd','sglt2','tzd','BUN','Creatinine')]
nrow(PS)
PS<-unique(PS)
nrow(PS)
sum(is.na(PS))
# N0: (total, target, control), 각 변수별 
table(PS$cohort_type)
length(unique(PS$ID2)) 
# S1:  MatchIt
matched <- matchit(cohort_type ~ age + gender + SU + alpha + dpp4i + gnd + sglt2 + tzd + BUN + Creatinine ,  method="nearest", data= PS, ratio= 2)

m_data <- match.data(matched, data= final, distance ='prop.score')
head(matched)
str(m_data)
# N1: 
table(m_data$cohort_type)
length(unique(m_data$ID2)) 
str(m_data)
# S2: T-TEST, 용량별



# S3: Paired T-test, 성분별 
