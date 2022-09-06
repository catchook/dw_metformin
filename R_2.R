install.packages("dplyr")
install.packages('MatchIt')
install.packages('tableone')
install.packages('optmatch')
install.packages("moonBook")
library('moonBook')
library('MatchIt')
library('dplyr')
library('tableone')
library('optmatch')
##데이터 읽기 

data = read.csv("C:/Users/USER/Documents/vs_code/syk/syk/dw_metformin/data5.csv")
data
#rm(m.out1)
## 데이터 전처리(이상치 제거 및 결측값 채우기)
##결측값 채우기. 
data$Creatinine[is.na(data$Creatinine)]<-1
data$BUN[is.na(data$BUN)]<-10
#이상치 3%trim해야1:1도 얼추 맞음
quantile(data$Creatinine, 0.970) # 2.11 #1.9
quantile(data$Creatinine, 0.030) # 0.5 #0.52
quantile(data$BUN, 0.965) # 34.7 #31.6
quantile(data$BUN, 0.035) # 6.8 #7.1

data <-subset(data, 7.5 <= data$BUN & data$BUN <= 29.622 )
data <-subset(data, 0.52 <=data$Creatinine & data$Creatinine <= 1.9 )

str(data)
nrow(distinct(data, ID))
nrow(distinct(data[data$cohort_type==1, ], ID))
nrow(distinct(data[data$cohort_type==0, ], ID))
## 데이터 전처리(egfr 계산)
sample <-  unique(data[ ,c('age', 'gender', 'Creatinine', 'ID')])
str(sample)
sample$gender <- ifelse(sample$gender ==1, 0.742, 1)
sample$egfr <- 175* (sample$Creatinine^(-1.154))* (sample$age^(-0.203))* sample$gender
sample <- unique(sample[, c('ID', 'egfr')])
str(sample)
#rm(sample)
##데이터 전처리 (cci계산)
list<-names(data)[15:35]
list<-append(list, "ID")
list
sample2 <- unique(data[,list])
colSums(is.na(sample2))
sample2$cci <-sample2$MI + sample2$HF + sample2$PV + sample2$CV + sample2$Dementia + sample2$CPD + sample2$RD + sample2$PUD + sample2$MLD + sample2$D + sample2$DCC *2 + sample2$HP*2 +
  sample2$RD.1*2 + sample2$M*2 + sample2$MSLD*3 + sample2$MST*6 + sample2$AIDS*6
sample2$cci2 <- sample2$HF*2 + sample2$Dementia*2 + sample2$CPD + sample2$RD + sample2$MLD*2 + sample2$DCC  + sample2$HP*2 +
  sample2$RD.1 + sample2$M*2 + sample2$MSLD*4 + sample2$MST*6 + sample2$AIDS*4
sample2 <-sample2[,c('ID', 'cci')]
str(sample2)

## 데이터 합치기 
data <- left_join(data, sample, by='ID')
data<-left_join(data, sample2, by='ID')
#rm(sample)
#rm(sample2)
str(data)

#성향 점수 매칭할 데이터만  추출
ps <-  unique(data[, c("cohort_type", "age",  "gender", "SU", "alpha"   ,"dpp4i","gnd","sglt2"   ,   "tzd" ,  
                  "BUN",  "Creatinine"  ,  "MI"   , "HF"   ,   "PV"     ,   "CV"    ,   "CPD" ,    "RD" , 
                  "PUD"  ,    "MLD"      ,   "DCC" ,  "HP"  ,    "MSLD"  ,  "AIDS"   ,   "HT"       ,    "HL"  ,   
                  "Sepsis"         ,        "HTT"      ,        "ID"      ,    "egfr",  'cci' )])                 
str(ps)              
rm(data)
rm(sample)
#전처리()
colSums(is.na(ps))
#ps<-na.omit(ps)
ps[is.na(ps)]<-0

## 성향 점수 매칭 (1:1)
m.out1 <- matchit(cohort_type ~ cci+ age + gender +Creatinine  + BUN+ egfr +  SU + alpha+ dpp4i + gnd + sglt2 +tzd + MI + HF +PV + CV + CPD + RD+ PUD +MLD + DCC +HP + MSLD + AIDS + HT+ HL + Sepsis+ HTT
                    , data = ps, method ='nearest', distance ='logit',  ratio =1)

#m.out1 <- matchit(cohort_type ~ cci2+ age + gender +Creatinine  + BUN+ egfr +  SU + alpha+ dpp4i + gnd + sglt2 +tzd + HT+ HL + Sepsis+ HTT
#                  , data = ps, method ='nearest', distance ='logit',  ratio =1)
#rm(m.out1)
summary(m.out1)
m.data<-match.data(m.out1, data = ps, distance = 'prop.score')
str(m.data)
id1 <- unique(m.data[, 'ID'])
id <- data.frame(ID=matrix(unlist(id1), nrow=length(id1), byrow=TRUE))
str(id)
nrow(distinct(data, ID))
nrow(distinct(data[data$cohort_type==1, ], ID))
nrow(distinct(data[data$cohort_type==0, ], ID))

d1 <- left_join(id, data, by='ID' )
nrow(distinct(m.data, ID))
nrow(distinct(m.data[m.data$cohort_type==1, ], ID))
nrow(distinct(m.data[m.data$cohort_type==0, ], ID))
colSums(is.na(d11))
str(d1)
d11 <- d1[d1$rrow==1, ]
d12<- na.omit(d11)
nrow(distinct(d12, ID))
nrow(distinct(d12[d12$cohort_type==1, ], ID))
nrow(distinct(d12[d12$cohort_type==0, ], ID))


caliper_n=sd(m.data$prop.score) * 0.25
summary(m.out2)
nrow(distinct(m.data, ID))
nrow(distinct(m.data[m.data$cohort_type==1, ], ID))
nrow(distinct(m.data[m.data$cohort_type==0, ], ID))
## 성향 점수 매칭 (2:1)
m.out2 <- matchit(cohort_type ~cci+ age + gender + BUN +  Creatinine  + egfr +  SU + alpha+ dpp4i + gnd + sglt2 +tzd + MI + HF +PV + 
                    CV + CPD + RD+ PUD +MLD + DCC +HP + MSLD + AIDS + HT+ HL + Sepsis+ HTT, data = ps, method ='nearest', distance ='logit',  ratio =2, caliper =0.1, replacement=T)
#m.out2 <- matchit(cohort_type ~cci2+ age + gender + BUN +  Creatinine  + egfr +  SU + alpha+ dpp4i + gnd + sglt2 +tzd + HT+ HL + Sepsis+ HTT, data = ps, method ='nearest', distance ='logit',  ratio =2, caliper =0.1, replacement=T)
m.data2<-match.data(m.out2, data = ps, distance = 'prop.score')
str(m.data2)
summary(m.out2)
nrow(distinct(m.data2, ID))
nrow(distinct(m.data2[m.data2$cohort_type==1, ], ID))
nrow(distinct(m.data2[m.data2$cohort_type==0, ], ID))

id2 <- unique(m.data2[, 'ID'])
id <- data.frame(ID=matrix(unlist(id2), nrow=length(id2), byrow=TRUE))
d2 <- left_join(id, data, by='ID' )
d21 <- d2[d2$rrow==1, ]
d22<- na.omit(d21)
colSums(is.na(d22))
str(d1)

nrow(distinct(d22, ID))
nrow(distinct(d22[d22$cohort_type==1, ], ID))
nrow(distinct(d22[d22$cohort_type==0, ], ID))


## smd 및 ps매칭 확인. 
names(m.data)
#모든 변수 
myVars <- names(m.data)[2:30]
myVars<-myVars[-27]
#myVars<-myVars[-19]
myVars
## 범주형 변수 
catVars <- c('gender', 'SU', 'alpha', 'dpp4i', 'gnd', 'sglt2', 'tzd', 'MI', 'HF', 'PV', 'CV', 'CPD', 'RD', 'PUD', 'MLD', 'DCC', 'HP', 'MSLD', 'AIDS', 'HT', 'HL', 'Sepsis', 'HTT')
t1<-CreateTableOne(vars= myVars, factorVars = catVars, data = m.data, strata='cohort_type', smd=TRUE)
#rm(data)
t2<-CreateTableOne(vars= myVars, factorVars = catVars, data = m.data2, strata='cohort_type', smd =TRUE)

write.csv(m.data, "C:/Users/USER/Documents/vs_code/syk/syk/dw_metformin/m_data_1.csv")
write.csv(m.data2, "C:/Users/USER/Documents/vs_code/syk/syk/dw_metformin/m_data_2.csv")

tab3Mat <- print(t1,  quote = FALSE, noSpaces = TRUE, printToggle = FALSE)
tab3Mat2 <- print(t2,  quote = FALSE, noSpaces = TRUE, printToggle = FALSE)
tab3Mat2
## Save to a CSV file
write.csv(tab3Mat2, file = "C:/Users/USER/Documents/vs_code/syk/syk/dw_metformin/myTable2.csv")

?shapiro.test


data = read.csv("C:/Users/USER/Downloads/kdh_final1.csv")

library(data.table)

d1 = fread("C:/Users/USER/Downloads/EUMC.csv")
d2 = fread("C:/Users/USER/Downloads/INHA.csv")
d3 = fread("C:/Users/USER/Downloads/KHMC.csv")
d4 = fread("C:/Users/USER/Downloads/KWMC.csv")
d5 = fread("C:/Users/USER/Downloads/SCHSU.csv")
d6 = fread("C:/Users/USER/Downloads/SEJONG_BCN.csv")
d7 = fread("C:/Users/USER/Downloads/SEJONG_ICN.csv")
d8 = fread("C:/Users/USER/Downloads/WKUH.csv")
d9 = fread("C:/Users/USER/Downloads/YWMC.csv")


dd<-rbind(d1,d2,d3,d4,d5,d6,d7,d8,d9, fill=T)

fwrite(dd,"합쳐드렸어요.csv")

View(dd)

?rbind
