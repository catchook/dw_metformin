# env setting
# install.packages("modules")
# install.packages("dplyr")
# install.packages("MatchIt")
# install.packages("tableone")
# install.packages("optmatch")
# install.packages("moonBook")
# install.packages("data.table")
# install.packages("modules")
# install.packages("ggplot2")
# install.packages("dplyr")
# install.packages("purrr")
# install.packages("tidyr")
# install.packages("gridExtra")
# install.packages("cowplot")
# install.packages("knitr")
# install.packages("sjlabelled")
# install.packages("lawstat")
# install.packages("gplots")
# install.packages("nortest")
# install.packages("stringr")
# install.packages("reshape2")
# install.packages("reshape")
# install.packages("strex")

##library 
library(moonBook)
library(MatchIt)
library(dplyr)
library(tableone)
library(optmatch)
library(data.table)
library(modules)
library(ggplot2)
library(dplyr)
library(purrr)
library(tidyr)
library(gridExtra)
library(cowplot)
library(knitr)
library(sjlabelled)
library(lawstat)
library(gplots)
library(nortest)
library(stringr)
library(reshape2)
library(reshape)
library(strex)
library(modules)
library(data.table)
library(RPostgreSQL)
library(lubridate)

# ## 기본 함수
ff <- module({
    import("DBI") 
    import("RSQLite")
    import("dplyr")
    import("ggplot2")
    import("gridExtra")
    import("cowplot")
    import("sjlabelled")
    import("moonBook")
    import("stats")
    import("gplots")
    import("stats")
    import("lawstat")
    import("grDevices")
    import("nortest")
    import('data.table')
    import('RPostgreSQL')
    import('lubridate')
    import('data.table')
    import('base')
    import('utils')

save_query <- function(sql, schema, db_target, db_control, con){
    sql <- gsub("_results_fnet_v276", "_results_dq_v276", sql)
    sql <- gsub("cdm_hira_2017", schema, sql) 
    sql <- gsub("target", db_target, sql)
    sql <- gsub("control", db_control, sql)
    result <- dbGetQuery(con, sql)
    result <- unique(result)
    result <- as.data.frame(result)
    return(result)
    }

count_n <- function(df, step){
  table<-df %>% group_by(measurement_type, cohort_type) %>% summarise(pl = n_distinct(ID)) %>% arrange(measurement_type)
  tt <- as.data.frame(table)
  groups <- split(tt, tt$cohort_type)
  print("start header.true function in count_n ")
  header.true <- function(df) {
  names(df) <- as.character(unlist(df[1,]))
  df[-1,] }
  target <- as.data.frame(t(groups$T[, c('measurement_type', "pl")]))
  target <- header.true(target)
  control <- as.data.frame(t(groups$C[, c('measurement_type',"pl")]))
  control <- header.true(control)
  cols <- c("CRP", "ESR","BUN", "Triglyceride",  "SBP", "Hb", 'Glucose_Fasting',  'Creatinine', "HDL",  "AST", "Albumin", "insulin", "BMI", "HbA1c", "DBP",'Total cholesterol', 'LDL', 'NT-proBNP')
  n = length(cols)
  control_list = vector("list", length = n)
  target_list = vector("list", length = n )
  for(i in cols) {
    x <- ifelse(is.null(control[[i]]), 0, control[[i]])
    y <- ifelse(is.null(target[[i]]), 0, target[[i]])
    control_list[[i]] <- x
    target_list[[i]] <- y
      }
  control <- t(do.call(rbind, control_list))
  target <- t(do.call(rbind, target_list))
  total <- data.frame(cohort_type=c('Control' ,'Target'), rbind(control, target))
  total$step <- step
  print("show count_n")
  str(total)
  return(total)
  }

 count_total <- function(df, step){
  table  <- df %>% group_by(cohort_type) %>% summarise(N= n_distinct(ID))
  ttable <- t(table)
  ttable  <- as.data.frame(ttable)
  print("start header.true function in count_total")
  header.true <- function(df) {
  names(df) <- as.character(unlist(df[1,]))
  df[-1,] }
  table  <- header.true(ttable)
  table$step <- step
  table <- subset(table, select = c( T, C , step))
  print("show count_total")
  str(table)
  return(table)
 }


dose_count_n <- function(df, step){
  table<-df %>% group_by(measurement_type, dose_type) %>% summarise(pl = n_distinct(ID)) %>% arrange(measurement_type)
  tt <- as.data.frame(table)
  groups <- split(tt, tt$dose_type)
  print("dose_count_n::: start header.true function in count_n ")
  header.true <- function(df) {
  names(df) <- as.character(unlist(df[1,]))
  df[-1,] }
  high <- as.data.frame(t(groups$high[, c('measurement_type', "pl")]))
  high <- header.true(high)
  low <- as.data.frame(t(groups$low[, c('measurement_type',"pl")]))
  low <- header.true(low)
  cols <- c("CRP", "ESR","BUN", "Triglyceride",  "SBP", "Hb", 'Glucose_Fasting',  'Creatinine', "HDL",  "AST", "Albumin", "insulin", "BMI", "HbA1c", "DBP",'Total cholesterol', 'LDL', 'NT-proBNP')
  n = length(cols)
  high_list = vector("list", length = n)
  low_list = vector("list", length = n )
  for(i in cols) {
    x <- ifelse(is.null(high[[i]]), 0, high[[i]])
    y <- ifelse(is.null(low[[i]]), 0, low[[i]])
    high_list[[i]] <- x
    low_list[[i]] <- y
      }
  high <- t(do.call(rbind, high_list))
  low <- t(do.call(rbind, low_list))
  total <- data.frame(dose_type=c('high' ,'low'), rbind(high, low))
  total$step <- step
  print("show count_n")
  str(total)
  return(total)
  }

 dose_count_total <- function(df, step){
  table  <- df %>% group_by(dose_type) %>% summarise(N= n_distinct(ID))
  ttable <- t(table)
  ttable  <- as.data.frame(ttable)
  print("start header.true function in count_total")
  header.true <- function(df) {
  names(df) <- as.character(unlist(df[1,]))
  df[-1,] }
  table  <- header.true(ttable)
  table$step <- step
  print("dose_count_total:::show count_total")
  str(table)
  return(table)
 }












chr_to_date <- function(data){
  chr_cols <- c("cohort_start_date", "cohort_end_date", "drug_exposure_start_date", "drug_exposure_end_date")
  data[, chr_cols] <- lapply(data[, chr_cols], as.Date)
  return(data)
}
trim <- function(data, n) {
a <- (100-n)*0.01
b <- n*0.01 
print("how much trim?")
print(b)
print("replace cr, bun null to 1, 10 ")
data$Creatinine[is.na(data$Creatinine)] <- 1
data$BUN[is.na(data$BUN)] <- 10
cr_high <- stats::quantile(data$Creatinine, a, na.rm =TRUE)
cr_low <- stats::quantile(data$Creatinine, b, na.rm = TRUE)
bun_high <- stats::quantile(data$BUN, a, na.rm = TRUE)
bun_low <- stats::quantile(data$BUN, b, na.rm = TRUE)
data <- subset(data, cr_low <= Creatinine & Creatinine <= cr_high )
data <- subset(data, bun_low <= BUN & BUN <= bun_high )
return(data)
}
})


# ## Stat module 
stat <- module({
  import("dplyr")
  import("ggplot2")
  import("gridExtra")
  import("cowplot")
  import("sjlabelled")
  import("moonBook")
  import("stats")
  import("gplots")
  import("stats")
  import("lawstat")
  import("grDevices")
  import("nortest")
  import("tableone")

## 시각화 , 단계별로 
fig <- function(df, step) {
  df <- df %>% mutate( cohort_type2 = recode(cohort_type,  "C"="control", "T" ="target"))
  cohort_type_ = as.factor(df$cohort_type2)
  print("options")
  options(repr.plot.width=15, repr.plot.height =5)
  print("ggplot")
  a1 <- df %>% ggplot(aes(x=BUN, fill= cohort_type_)) + theme_classic() + geom_histogram(color="gray80", alpha=0.2, position = "identity", bins=100) 
  a2 <- df %>% ggplot(aes(x=Creatinine, fill= cohort_type_)) + theme_classic() + geom_histogram(color="gray80", alpha=0.2, position = "identity", bins= 100)
  a3 <- df %>% ggplot(aes(x=age, fill= cohort_type_)) + theme_classic() + geom_histogram(color="gray80", alpha=0.2, position = "identity", bins=100)
  print("grid1")
  fig <- plot_grid(a1, a2, a3, labels =c("BUN", "Creatinine", "Age"), align = "h", ncol=3)
  title <- ggdraw() + draw_label( paste0(step, " numeric variable") , fontface = "bold") 
  print("grid2")
  fig <- plot_grid(title, fig, cols = 1, rel_heights = c(0.1, 1))
  print("ggsave")
  ggsave(paste0("/data/results/", step ,"_numeric.png"), fig, device = "png",  dpi=300, width=15, height=5)
  options(repr.plot.width = 15, repr.plot.height = 10)
  print("b1")
  b1=df %>% ggplot(aes(x=SU, fill= cohort_type_ )) + theme_classic() + geom_bar()
  print("b2")
  b2=df %>% ggplot(aes(x= alpha, fill= cohort_type_)) + theme_classic()+ geom_bar()
  print("b3")
  b3=df %>% ggplot(aes(x=dpp4i, fill= cohort_type_)) + theme_classic()+ geom_bar()
  print("b4")
  b4=df %>% ggplot(aes(x=gnd, fill= cohort_type_)) + theme_classic()  + geom_bar()
  print("b5")  
  b5=df %>% ggplot(aes(x=sglt2, fill= cohort_type_)) + theme_classic()+ geom_bar()
  print("b6")
  b6=df %>% ggplot(aes(x=tzd, fill= cohort_type_)) + theme_classic()  + geom_bar()
  print("fig")
  fig<- plot_grid(b1, b2 ,b3, b4, b5, b6,labels=c("SU", "Alpha", "DPP-4I", "GND", "SGLT2","TZD") , ncol=3)
  title<-ggdraw() + draw_label( paste0(step, " co-medication "),  fontface = "bold") 
  print("fig2")
  fig2<-plot_grid(title, fig, ncol = 1, rel_heights = c(0.1, 1))
  print("ggsave")
  ggsave(paste0("/data/results/",step,"_co_drug.png"), fig2 , device = "png",  dpi=300, width=15, height=10)
}

############################

dose_fig <- function(df, step) {
  dose_type_ = as.factor(df$dose_type)
  print("options")
  options(repr.plot.width=15, repr.plot.height =5)
  print("ggplot")
  a1 <- df %>% ggplot(aes(x=BUN, fill= dose_type_)) + theme_classic() + geom_histogram(color="gray80", alpha=0.2, position = "identity", bins=100) 
  a2 <- df %>% ggplot(aes(x=Creatinine, fill= dose_type_)) + theme_classic() + geom_histogram(color="gray80", alpha=0.2, position = "identity", bins= 100)
  a3 <- df %>% ggplot(aes(x=age, fill= dose_type_)) + theme_classic() + geom_histogram(color="gray80", alpha=0.2, position = "identity", bins=100)
  print("grid1")
  fig <- plot_grid(a1, a2, a3, labels =c("BUN", "Creatinine", "Age"), align = "h", ncol=3)
  title <- ggdraw() + draw_label( paste0(step, " numeric variable") , fontface = "bold") 
  print("grid2")
  fig <- plot_grid(title, fig, cols = 1, rel_heights = c(0.1, 1))
  print("ggsave")
  ggsave(paste0("/data/results/dose_", step ,"_numeric.png"), fig, device = "png",  dpi=300, width=15, height=5)
  options(repr.plot.width = 15, repr.plot.height = 10)
  print("b1")
  b1=df %>% ggplot(aes(x=SU, fill= dose_type_ )) + theme_classic() + geom_bar()
  print("b2")
  b2=df %>% ggplot(aes(x= alpha, fill= dose_type_)) + theme_classic()+ geom_bar()
  print("b3")
  b3=df %>% ggplot(aes(x=dpp4i, fill= dose_type_)) + theme_classic()+ geom_bar()
  print("b4")
  b4=df %>% ggplot(aes(x=gnd, fill= dose_type_)) + theme_classic()  + geom_bar()
  print("b5")  
  b5=df %>% ggplot(aes(x=sglt2, fill= dose_type_)) + theme_classic()+ geom_bar()
  print("b6")
  b6=df %>% ggplot(aes(x=tzd, fill= dose_type_)) + theme_classic()  + geom_bar()
  print("fig")
  fig<- plot_grid(b1, b2 ,b3, b4, b5, b6,labels=c("SU", "Alpha", "DPP-4I", "GND", "SGLT2","TZD") , ncol=3)
  title<-ggdraw() + draw_label( paste0("by dose: ",step, " co-medication "),  fontface = "bold") 
  print("fig2")
  fig2<-plot_grid(title, fig, ncol = 1, rel_heights = c(0.1, 1))
  print("ggsave")
  ggsave(paste0("/data/results/dose_",step,"_co_drug.png"), fig2 , device = "png",  dpi=300, width=15, height=10)
}

###############################
## ps매칭 전후, smd
smd <- function( DF, step ){
  # label생성 
  print("start smd")
  DF$gender <- set_label(DF$gender, label = "gender")
  DF$gender <- set_labels(DF$gender, labels= c("male" = 'M' , "female" = 'F'))
  print("smd:: gender done")
  DF$cohort_type <- set_label(DF$cohort_type, label ="cohort_type")
  DF$cohort_type <- set_labels(DF$cohort_type, labels=c("target"='T', "control"='C'))
  print("smd:: cohort_type done")
  out = mytable(cohort_type~ age + gender + BUN +  Creatinine  + egfr +  SU + alpha+ dpp4i + gnd + sglt2 +tzd + MI + HF +PV + 
                    CV + CPD + RD+ PUD +MLD + DCC +HP + MSLD + AIDS + HT2+ HL2 + Sepsis+ HTT + cci  , data =DF)
  print("out done")
  mycsv(out, file = paste0("/data/results/smd_", step, ".csv"))
  print("mycsv done")
} ##이후 mycsv(out, file="") 파일명으로 단계 구분하기. 
## dose type version: ps매칭 전후, smd
dose_smd <- function(DF, step ){
  # label생성 
  print("start dose_smd::")
  DF$gender <- set_label(DF$gender, label ="gender")
  DF$gender<- set_labels(DF$gender, labels=  c("male" = 'M' , "female" = 'F'))
  print("dose_smd:: gender_type done")
  out = mytable(dose_type~ age + gender + BUN +  Creatinine  + egfr +  SU + alpha+ dpp4i + gnd + sglt2 +tzd + MI + HF +PV + 
                    CV + CPD + RD+ PUD +MLD + DCC +HP + MSLD + AIDS + HT2+ HL2 + Sepsis+ HTT + cci  , data =DF)
  print("dose_smd::: out done")
  mycsv(out, file = paste0("/data/results/dose_smd_", step, ".csv"))
  print("dose _smd:: mycsv done")

} ##이후 mycsv(out, file="") 파일명으로 단계 구분하기. 

# ##qqplot --normal distribution 정규성 시각화 
# qq <- function(stat){
# for(i in unique(stat$measurement_type) ){
# qqnorm(stat[which( stat$measurement_type== i ), diff] , main= paste(i,"Normal Distribution") )
# qqline(stat[which( stat$measurement_type==i ), diff])
# }}
# ## 정규성 검정. 
# shapiro <- function(stat){
#   results<- data.frame(cohort_type= NA, measruement_type= NA, Pvalue=NA)

#   for ( i in unique(stat$cohort_type)){
#     for( j in unique(stat$measurement_type)){
#       if(length(stat[which(stat$measurement_type==j & stat$cohort_type==i), diff])<3 |length(stat[which(stat$measurement_type==j & stat$cohort_type==i), diff]) >5000 ) {
#         out<- data.frame(cohort_type=i, measurement_type= j, Pvalue=0)
#         names(out)<-names(results)
#         results<- rbind(results, out)
#       } else{      
#         out<- shapiro.test( stat[which(stat$measurement_type==j & stat$cohort_type==i), diff])
#         outs<- data.frame(cohort_type=i, measurement_type= j,Pvalue= out[[2]])
#         names(outs)<-names(results)
#         results<- rbind(results, outs)

#       }
#     }
#   }
#   return(results)
# }

# ## rate: 정규성, 등분산성, ttest, wilcox
test_rate <- function(stat){
  results<- data.frame( m_type= NA, normality_pvalue_target= NA, normality_pvalue_control= NA, var_pvalue= NA, ttest_pvalue = NA,  wilcoxon_pvalue = NA, target_mean = NA, control_mean = NA)
  m_list <- unique(stat$measurement_type)
  for( j in m_list){
      target <- stat %>% dplyr::filter(measurement_type == j  & cohort_type =='T') %>% distinct(ID, rate) 
      control<- stat %>% dplyr::filter(measurement_type == j  & cohort_type =='C') %>% distinct(ID, rate) 
      total <- stat %>% dplyr::filter(measurement_type == j ) %>% distinct(ID, cohort_type, rate)
      target <- as.data.frame(target)  
      control  <- as.data.frame(control) 
      total <- as.data.frame(total) 

      if ((nrow(target) <3 | nrow(target) >5000) | (nrow(control) <3 |nrow(control) >5000)) {
        out<- data.frame(j,NA, NA,NA, NA,NA,NA,NA )
        names(out)<-names(results)
        results<- rbind(results, out)
          } else {      
        normality_t <- shapiro.test(target$rate)
        normality_c <- shapiro.test(control$rate)
        variance <- levene.test(total$rate, total$cohort_type)
        ttest <- t.test(target$rate, control$rate)
        wilcox <- wilcox.test(target$rate, control$rate)
        ## 소수점 2자리에서 반올림
        normality_t <- round(normality_t$p.value , digits = 2)
        normality_c <- round(normality_c$p.value , digits = 2)
        variance <- round(variance$p.value , digits = 2)
        ttest_p <- round(ttest$p.value, digits = 2)
        wilcox <- round(wilcox$p.value, digits = 2) 
        means <- as.numeric(gsub("^[a-z]","", ttest$estimate) )
        targetmean <- round(means[1], digits=2)
        controlmean <- round(means[2], digits=2)
        ## 합치기 
        outs <- data.frame( j, normality_t, normality_c, variance, ttest_p, wilcox, targetmean, controlmean)
        names(outs) <- names(results)
        results <- rbind(results, outs)
        ## 그림 
        bars<- tapply(total$rate, total$cohort_type, mean )
        lower<- tapply(total$rate, total$cohort_type, function(x) t.test(x)$conf.int[1])
        upper<- tapply(total$rate, total$cohort_type, function(x) t.test(x)$conf.int[2])
        
        png(file=paste0("/data/results/",j,"_rate_test.png"))
        barplot2(bars, space=0.4, xlim=c(0,3.0), plot.ci=TRUE, ci.l = lower, ci.u= upper, ci.color ="maroon", ci.lwd=4, names.arg=c("target","control"), col=c("coral", "darkkhaki"), xlab ="cohort type", ylab= "rate", 
        main=paste0(j,"Rate by cohort type with Confidence Interval" ) )
        dev.off()
      }
    }
      results <- na.omit(results)
      return(results)
  }

## diff: 정규성, 등분산성, ttest, wilcox, describe 
test_diff <- function(stat){
  results<- data.frame( m_type= NA, normality_pvalue_target= NA, normality_pvalue_control= NA, var_pvalue= NA, ttest_pvalue = NA,  wilcoxon_pvalue = NA, target_mean = NA, control_mean = NA)
  m_list <- unique(stat$measurement_type)
  for( j in m_list){
      target <- stat %>% dplyr::filter(measurement_type == j  & cohort_type =='T') %>% distinct(ID, diff) 
      control<- stat %>% dplyr::filter(measurement_type == j  & cohort_type =='C') %>% distinct(ID, diff) 
      total <- stat %>% dplyr::filter(measurement_type == j ) %>% distinct(ID, cohort_type, diff)
      target <- as.data.frame(target)  
      control  <- as.data.frame(control) 
      total <- as.data.frame(total) 
      if ((nrow(target) <3 | nrow(target) >5000) | (nrow(control) <3 |nrow(control) >5000)) {
        out<- data.frame(j,NA, NA,NA, NA,NA,NA, NA )
        names(out)<-names(results)
        results<- rbind(results, out)
          } else {      
        normality_t <- shapiro.test(target$diff)
        normality_c <- shapiro.test(control$diff)
        variance <- levene.test(total$diff, total$cohort_type)
        ttest <- t.test(target$diff, control$diff)
        wilcox <- wilcox.test(target$diff, control$diff)
        ## 소수점 2자리에서 반올림
        normality_t = round(normality_t$p.value , digits = 2)
        normality_c = round(normality_c$p.value , digits = 2)
        variance = round(variance$p.value , digits = 2)
        ttest_p<- round(ttest$p.value, digits = 2)
        wilcox <-round(wilcox$p.value, digits = 2) 
        means <- as.numeric(gsub("^[a-z]","", ttest$estimate) )
        targetmean <- round(means[1], digits=2)
        controlmean <- round(means[2], digits=2)
        ## 합치기 
        outs<- data.frame(j, normality_t, normality_c, variance, ttest_p, wilcox, targetmean, controlmean)
        names(outs)<-names(results)
        results<- rbind(results, outs)
        ## 그림 
        bars<- tapply(total$diff, total$cohort_type, mean )
        lower<- tapply(total$diff, total$cohort_type, function(x) t.test(x)$conf.int[1])
        upper<- tapply(total$diff, total$cohort_type, function(x) t.test(x)$conf.int[2])
        png(file=paste0("/data/results/",j,"_diff_test.png"))
        barplot2(bars, space=0.4, xlim=c(0,3.0), plot.ci=TRUE, ci.l = lower, ci.u= upper, ci.color ="maroon", ci.lwd=4, names.arg=c("target","control"), col=c("coral", "darkkhaki"), xlab ="cohort type", ylab= "diff", 
        main=paste0(j,"diff by cohort type with Confidence Interval" ) )
        dev.off()
      }
    }
      results <- na.omit(results)
      return(results)
  }
# ## paired ttest rate test: 등분산성, 정규성, t-test, wilcox test 
# ## target +  병용 약물 별  
ptest_drug <- function(stat){
  target <- stat %>% dplyr::filter(cohort_type == 'T') %>% dplyr::distinct(ID, value_as_number.before, value_as_number.after, drug_group, measurement_type)
  target <- as.data.frame(target)
  results<- data.frame(d_type=NA,  m_type= NA, normality_pvalue_pre= NA, normality_pvalue_post= NA, var_pvalue= NA, ttest_pvalue = NA,  wilcoxon_pvalue = NA, pre_mean = NA, post_mean = NA, mean_diff= NA)
  drug_list=c("alpha", "SU","metformin", "gnd","tzd","dpp4i", "sglt2")
  m_list <- unique(target$measurement_type)
  for (j in m_list){  
        total_m  <- target %>% dplyr::filter(measurement_type == j)
        if (nrow(total_m) <3 | nrow(total_m) > 5000) {
          out<- data.frame("total",j, NA, NA,NA, NA,NA, NA, NA, NA )
          names(out)<-names(results)
          results<- rbind(results, out)
            } else {      
          normality_pre <- shapiro.test(total_m$value_as_number.before)
          normality_post <- shapiro.test(total_m$value_as_number.after)
          variance <- var.test(total_m$value_as_number.before , total_m$value_as_number.after)
          ptest <- t.test(total_m$value_as_number.after , total_m$value_as_number.before, paired =TRUE)
          wilcox <- wilcox.test(total_m$value_as_number.before , total_m$value_as_number.after, exact = FALSE)
          ## 소수점 2자리에서 반올림
          normality_pre <- round(normality_pre$p.value , digits = 2)
          normality_post <- round(normality_post$p.value , digits = 2)
          variance <- round(variance$p.value, digits = 2)
          ptest_p <- round(ptest$p.value, digits = 2)
          wilcox <- round(wilcox$p.value, digits = 2) 
          mean_diff <- round(as.numeric(gsub("^[a-z]","", ptest$estimate)), digits=2 )
          pre_mean <- round(mean(target$value_as_number.before, na.rm=TRUE), digits=2)
          post_mean <- round(mean(target$value_as_number.after, na.rm=TRUE), digits=2)
          ## 합치기 
          outs<- data.frame("total",j, normality_pre, normality_post, variance, ptest_p, wilcox, pre_mean, post_mean, mean_diff)
          names(outs)<-names(results)
          results<- rbind(results, outs)
          ## 그림 

          sub <- subset(total_m, select =c("value_as_number.before", "value_as_number.after"))
          bars<- sapply( sub , function(x) mean(x, na.rm=TRUE))
          lower <- sapply(sub , function(x) t.test(x)$conf.int[1])
          upper<- sapply(sub, function(x) t.test(x)$conf.int[2])
          png(file=paste0("/data/results/total_",j,"_value_ptest.png"))
          barplot2(bars, space=0.4, plot.ci=TRUE, ci.l= lower, ci.u= upper, ci.color="maroon", ci.lwd=4, names.arg=c("pre","post"), col=c("coral","darkkhaki"), xlab="pre vs post", ylab = "value",  main =paste0("target total  : ",j," value with Confidence Interval"))
          dev.off()
      }} ## drug type별로 paired test
  for( i in drug_list){
    target_drug <-target[grep(i, target$drug_group), ]
    for (j in m_list){  
        target_ <-target_drug %>% dplyr::filter(measurement_type == j)
        target_ <- as.data.frame(target_)
        
        if (nrow(target_) <3 | nrow(target_) >5000) {
          out<- data.frame(i,j, NA, NA,NA, NA,NA, NA, NA, NA )
          names(out)<-names(results)
          results<- rbind(results, out)
            } else {      
          normality_pre <- shapiro.test(target_$value_as_number.before)
          normality_post <- shapiro.test(target_$value_as_number.after)
          variance <- var.test(target_$value_as_number.before , target_$value_as_number.after)
          ptest <- t.test(target_$value_as_number.after , target_$value_as_number.before, paired =TRUE)
          wilcox <- wilcox.test(target_$value_as_number.before , target_$value_as_number.after, exact = FALSE)

          ## 소수점 2자리에서 반올림
          normality_pre = round(normality_pre$p.value , digits = 2)
          normality_post = round(normality_post$p.value , digits = 2)
          variance = round(variance$p.value , digits = 2)
          ptest_p<- round(ptest$p.value, digits = 2)
          wilcox <-round(wilcox$p.value, digits = 2) 
          mean_diff <- round(as.numeric(gsub("^[a-z]","", ptest$estimate)), digits=2 )
          pre_mean <- round(mean(target_$value_as_number.before, na.rm=TRUE), digits=2)
          post_mean <- round(mean(target_$value_as_number.after, na.rm=TRUE), digits=2)

          ## 합치기 
          outs<- data.frame(i,j, normality_pre, normality_post, variance, ptest_p, wilcox, pre_mean, post_mean, mean_diff)
          names(outs) <- names(results)
          results<- rbind(results, outs)
    
          ## 그림 
          sub <- subset(target_, select = c("value_as_number.before", "value_as_number.after"))
          bars<- sapply( sub , function(x) mean(x, na.rm=TRUE))
          lower <- sapply(sub , function(x) t.test(x)$conf.int[1])
          upper<- sapply(sub, function(x) t.test(x)$conf.int[2])
          png(file=paste0("/data/results/total_",i,j,"_value_ptest.png"))
          barplot2(bars, space=0.4, plot.ci=TRUE, ci.l= lower, ci.u= upper, ci.color="maroon", ci.lwd=4, names.arg=c("pre","post"), col=c("coral","darkkhaki"), xlab="pre vs post", ylab = "value", 
          main =paste0(i," : ",j," value with Confidence Interval"))
          dev.off()
      }}}

            results <- na.omit(results)
            return(results)
          }
# 용량별 t-test, paired t-test
# high dose vs low dose  ttest 
## diff: 정규성, 등분산성, ttest, wilcox, describe 
dose_diff_rate <- function(stat){
  high <- stat %>% dplyr::filter(dose_type == 'high')
  low <- stat %>% dplyr::filter(dose_type == 'low')
  results<- data.frame( m_type= NA, rate_normality_high= NA, rate_normality_low= NA, rate_variance= NA, rate_ttest_p = NA,  rate_wilcox = NA, rate_high_mean = NA, rate_low_mean = NA, diff_normality_high= NA  , diff_normality_low= NA, diff_variance= NA, diff_ttest_p = NA,  diff_wilcox = NA, diff_high_mean = NA, diff_low_mean = NA)
  m_list <- unique(stat$measurement_type)
  for( j in m_list){

      high_m <- high %>% dplyr::filter(measurement_type==j) %>% dplyr::distinct(ID, rate, diff, dose_type)
      low_m <- low %>% dplyr::filter(measurement_type ==j ) %>% dplyr::distinct(ID, rate, diff, dose_type)
      total_m <- stat %>% dplyr::filter(measurement_type ==j) %>% dplyr::distinct(ID, rate, diff, dose_type)
      if ((nrow(high_m) <3 | nrow(high_m) >5000) | (nrow(low_m) <3 |nrow(low_m) >5000)) {
        out<- data.frame(j,NA, NA,NA, NA,NA,NA, NA, NA, NA, NA, NA, NA,NA,NA )
        names(out)<-names(results)
        results<- rbind(results, out)
          } else {      
        r_normality_h <- shapiro.test(high_m$rate)
        r_normality_l <- shapiro.test(low_m$rate)
        r_variance <- levene.test(total_m$rate, total_m$dose_type)
        r_ttest <- t.test(high_m$rate, low_m$rate)
        r_wilcox <- wilcox.test(high_m$rate, low_m$rate)
    
        d_normality_h <- shapiro.test(high_m$diff)
        d_normality_l <- shapiro.test(low_m$diff)
        d_variance <- levene.test(total_m$diff, total_m$dose_type)
        d_ttest <- t.test(high_m$diff, low_m$diff)
        d_wilcox <- wilcox.test(high_m$diff, low_m$diff)
        ## 소수점 2자리에서 반올림
        r_normality_h = round(r_normality_h$p.value , digits = 2)
        r_normality_l = round(r_normality_l$p.value , digits = 2)
        r_variance = round(r_variance$p.value , digits = 2)
        r_ttest_p<- round(r_ttest$p.value, digits = 2)
        r_wilcox <-round(r_wilcox$p.value, digits = 2) 
        r_means <- as.numeric(gsub("^[a-z]","", r_ttest$estimate) )
        r_high_mean <- round(r_means[1], digits=2)
        r_low_mean <- round(r_means[2], digits=2)

        d_normality_h = round(d_normality_h$p.value , digits = 2)
        d_normality_l = round(d_normality_l$p.value , digits = 2)
        d_variance = round(d_variance$p.value , digits = 2)
        d_ttest_p<- round(d_ttest$p.value, digits = 2)
        d_wilcox <-round(d_wilcox$p.value, digits = 2) 
        d_means <- as.numeric(gsub("^[a-z]","", d_ttest$estimate) )
        d_high_mean <- round(d_means[1], digits=2)
        d_low_mean <- round(d_means[2], digits=2)

        ## 합치기 
        outs<- data.frame(j, r_normality_h, r_normality_l, r_variance, r_ttest_p, r_wilcox, r_high_mean, r_low_mean, d_normality_h, d_normality_l, d_variance, d_ttest_p, d_wilcox, d_high_mean, d_low_mean)
        names(outs)<-names(results)
        results<- rbind(results, outs)
        ## 그림 
        r_bars <- tapply(total_m$rate, total_m$dose_type, function(x) mean(x, na.rm=TRUE))
        r_lower<- tapply(total_m$rate, total_m$dose_type, function(x) t.test(x)$conf.int[1])
        r_upper<- tapply(total_m$rate, total_m$dose_type, function(x) t.test(x)$conf.int[2])
        png(file=paste0("/data/results/dose_",j,"_rate.png"))
        barplot2(r_bars, space=0.4, xlim=c(0,3.0), plot.ci=TRUE, ci.l = r_lower, ci.u= r_upper, ci.color ="maroon", ci.lwd=4, names.arg=c("high","low"), col=c("coral", "darkkhaki"), xlab ="dose type", ylab= "rate", 
        main=paste0(j," rate by dose type with Confidence Interval" ) )
        dev.off()
        d_bars <- tapply(total_m$diff, total_m$dose_type, function(x) mean(x, na.rm=TRUE))
        d_lower<- tapply(total_m$diff, total_m$dose_type, function(x) t.test(x)$conf.int[1])
        d_upper<- tapply(total_m$diff, total_m$dose_type, function(x) t.test(x)$conf.int[2])
        png(file=paste0("/data/results/dose_",j,"_diff.png"))
        barplot2(d_bars, space=0.4, xlim=c(0,3.0), plot.ci=TRUE, ci.l = d_lower, ci.u= d_upper, ci.color ="maroon", ci.lwd=4, names.arg=c("high","low"), col=c("coral", "darkkhaki"), xlab ="dose type", ylab= "diff", 
        main=paste0(j," diff by dose type with Confidence Interval" ) )
        dev.off()
      }
    }
      results <- na.omit(results)
      return(results)
  }
# paired ttest rate test: 등분산성, 정규성, t-test, wilcox test 
## target + 용량군별 
dose_ptest <- function(total){
  dose_list  <- c("high", "low")
  results<- data.frame(dose_type =NA,m_type= NA, normality_pvalue_pre= NA, normality_pvalue_post= NA, var_pvalue= NA, ttest_pvalue = NA,  wilcoxon_pvalue = NA,pre_mean = NA, post_mean = NA, mean_diff= NA)
  m_list <- unique(total$measurement_type)
  for (j in m_list){  
        total_m <- total %>% dplyr::filter(measurement_type == j ) %>% dplyr::distinct(ID, value_as_number.before, value_as_number.after, dose_type, measurement_type)
       if (nrow(total_m) < 7) {
        out<- data.frame("total",j,NA, NA, NA, NA, NA, NA, NA, NA )
        names(out)<-names(results)
        results<- rbind(results, out)
          } else {      
        normality_pre <- ad.test(total_m$value_as_number.before)
        normality_post <- ad.test(total_m$value_as_number.after)
        variance <- var.test(total_m$value_as_number.before , total_m$value_as_number.after)
        ptest <- t.test(total_m$value_as_number.after , total_m$value_as_number.before, paired =TRUE)
        wilcox <- wilcox.test(total_m$value_as_number.before , total_m$value_as_number.after,exact = FALSE)
        ## 소수점 2자리에서 반올림
        normality_pre = round(normality_pre$p.value , digits = 2)
        normality_post = round(normality_post$p.value , digits = 2)
        variance = round(variance$p.value , digits = 2)
        ptest_p<- round(ptest$p.value, digits = 2)
        wilcox <-round(wilcox$p.value, digits = 2) 
        mean_diff <- round(as.numeric(gsub("^[a-z]","", ptest$estimate)), digits=2 )
        pre_mean <- round(mean(total_m$value_as_number.before, na.rm=TRUE), digits=2)
        post_mean <- round(mean(total_m$value_as_number.after, na.rm=TRUE), digits=2)
        ## 합치기 
        outs<- data.frame('total' ,j, normality_pre, normality_post, variance, ptest_p, wilcox, pre_mean, post_mean, mean_diff)
        names(outs)<-names(results)
        results<- rbind(results, outs)
        ## 그림 
        sub <- subset(total_m, select =c("value_as_number.before", "value_as_number.after"))
        bars<- sapply( sub , function(x) mean(x, na.rm=TRUE))
        lower <- sapply(sub , function(x) t.test(x)$conf.int[1])
        upper<- sapply(sub, function(x) t.test(x)$conf.int[2])
        png(file=paste0("/data/results/dose_total_",j,"_value_ptest.png"))
        barplot2(bars, space=0.4, plot.ci=TRUE, ci.l= lower, ci.u= upper, ci.color="maroon", ci.lwd=4, names.arg=c("pre","post"), col=c("coral","darkkhaki"), xlab="pre vs post", ylab = "value", 
        main =paste0("total_dose : ",j," with Confidence Interval"))
        dev.off()
       }} ##dose_Type 
      for( i in dose_list){
    target_dose <-total[grep(i, total$dose_type), ]
    for (j in m_list){  
        target_ <- target_dose %>% dplyr::filter(measurement_type == j)
        target_ <- as.data.frame(target_)
        
        if (nrow(target_) <3 | nrow(target_) >5000) {
          out<- data.frame(i,j, NA, NA,NA, NA,NA, NA, NA, NA )
          names(out)<-names(results)
          results<- rbind(results, out)
            } else {      
          normality_pre <- shapiro.test(target_$value_as_number.before)
          normality_post <- shapiro.test(target_$value_as_number.after)
          variance <- var.test(target_$value_as_number.before , target_$value_as_number.after)
          ptest <- t.test(target_$value_as_number.after , target_$value_as_number.before, paired =TRUE)
          wilcox <- wilcox.test(target_$value_as_number.before , target_$value_as_number.after, exact = FALSE)

          ## 소수점 2자리에서 반올림
          normality_pre = round(normality_pre$p.value , digits = 2)
          normality_post = round(normality_post$p.value , digits = 2)
          variance = round(variance$p.value , digits = 2)
          ptest_p<- round(ptest$p.value, digits = 2)
          wilcox <-round(wilcox$p.value, digits = 2) 
          mean_diff <- round(as.numeric(gsub("^[a-z]","", ptest$estimate)), digits=2 )
          pre_mean <- round(mean(target_$value_as_number.before, na.rm=TRUE), digits=2)
          post_mean <- round(mean(target_$value_as_number.after, na.rm=TRUE), digits=2)

          ## 합치기 
          outs<- data.frame(i,j, normality_pre, normality_post, variance, ptest_p, wilcox, pre_mean, post_mean, mean_diff)
          names(outs) <- names(results)
          results<- rbind(results, outs)
    
          ## 그림 
          sub <- subset(target_, select = c("value_as_number.before", "value_as_number.after"))
          bars<- sapply( sub , function(x) mean(x, na.rm=TRUE))
          lower <- sapply(sub , function(x) t.test(x)$conf.int[1])
          upper<- sapply(sub, function(x) t.test(x)$conf.int[2])
          png(file=paste0("/data/results/does_total_",i,j,"_value_ptest.png"))
          barplot2(bars, space=0.4, plot.ci=TRUE, ci.l= lower, ci.u= upper, ci.color="maroon", ci.lwd=4, names.arg=c("pre","post"), col=c("coral","darkkhaki"), xlab="pre vs post", ylab = "value", 
          main =paste0(i," : ",j," value with Confidence Interval"))
          dev.off()
      }}}

            results <- na.omit(results)
            return(results)
          }
})

# ## ps매칭에 필요한 변수 정의 : drug, disease history, Renal 수치  
ps <- module({
  import("strex")
  import("reshape")
  import("dplyr")
  import("tidyr")
  import('data.table')
  import('lubridate')
  import('utils')
  import('base')
  import('stats')
# drug history # ps_1st
##파일 합치기. 
drug_history <- function(data, t1){
  dc <- left_join(data, t1[,c("drug_concept_id","Name","type1","type2")], by = c("drug_concept_id") ) 
  dc<- unique(dc)
## get dummies 
  dc2 <- reshape2::melt(dc[,c("ID", "type1","type2")], id.vars =c("ID"), measure.vars=c("type1", "type2"))
  dc2 <- dc2 %>% group_by(ID) %>% summarise(type = list(unique(value)))
  columns = c("SU", "alpha", "dpp4i", "gnd", "metformin", "sglt2", "tzd")
  dc3 <- sapply(dc2$type, function(x) table(factor(x, levels= columns)))
  dc3 <-t(dc3)
  dc4<-cbind(dc2, dc3)  #id, type(all drug list ), dummmies variable SU", "alpha", "dpp4i", "gnd", "metformin", "sglt2", "tzd
  print("describe drug_history ")

  columns = c("ID","SU", "alpha", "dpp4i", "gnd", "metformin", "sglt2", "tzd")
  dc4 <- dplyr::select(dc4, all_of(columns))
  str(dc4)
  return(dc4)
}

disease_history <- function(df, data2, data3){
df$condition_type  <-  case_when(df$ancestor_concept_id == 4329847 ~ "MI",
                                                df$ancestor_concept_id == 316139  ~ "HF",
                                                df$ancestor_concept_id == 321052  ~ "PV",
                                                df$ancestor_concept_id %in% c(381591, 434056)  ~ "CV",
                                                df$ancestor_concept_id == 4182210 ~ 'Dementia',
                                                df$ancestor_concept_id == 4063381 ~ 'CPD',
                                                df$ancestor_concept_id %in% c(257628, 134442, 80800, 80809, 256197, 255348) ~ 'Rheuma',
                                                df$ancestor_concept_id == 4247120 ~ 'PUD',
                                                df$ancestor_concept_id %in% c(4064161, 4212540) ~ 'MLD',
                                                df$ancestor_concept_id == 201820 ~ 'D',
                                                df$ancestor_concept_id %in% c(443767,442793) ~ 'DCC',
                                                df$ancestor_concept_id %in% c(192606, 374022) ~ 'HP',
                                                df$ancestor_concept_id %in% c(4030518,	4239233, 4245042) ~ 'Renal',
                                                df$ancestor_concept_id == 443392  ~ 'M',
                                                df$ancestor_concept_id %in% c(4245975, 4029488, 192680, 24966) ~ 'MSLD',
                                                df$ancestor_concept_id == 432851  ~ 'MST',
                                                df$ancestor_concept_id == 439727  ~ 'AIDS',
                                                df$ancestor_concept_id == 316866  ~ 'HT',
                                                df$ancestor_concept_id == 432867  ~ 'HL',
                                                df$ancestor_concept_id == 132797  ~ 'Sepsis',
                                                df$ancestor_concept_id == 4254542 ~ 'HTT',
                                                TRUE ~ 'error')
# # disease history 
  dd <- reshape2::melt(df[,c("ID", "condition_type")], id.vars = "ID" , measure.vars="condition_type") 
  #l <- unique(dd$value)
  dd2 <-dd %>% group_by(ID) %>% summarise(dtype = list(unique(value)))
  cols = c("MI", "HF", "PV", "CV" , "Dementia","CPD", "Rheuma", "PUD", "MLD", "D", "DCC", "HP", 'Renal', "M", "MSLD","MST", "AIDS",'HT','HL','Sepsis', 'HTT')
  dd3 <- sapply(dd2$dtype ,function(x) table(factor(x, levels = cols)) )
  dd4 <- t(dd3)
  dd4 <- cbind(dd2, dd4)
  print("describe disease_history : ")
  str(dd4)
## 고혈압 데이터랑 합치기 
  d_hp <- left_join(dd4, data2, by = 'ID')
  d_hp$hypertension_drug[is.na(d_hp$hypertension_drug)] <-0

## 합친 데이터가 0이상이면 1로 변경. 
  d_hp <- d_hp %>% mutate( HT2 = HT + hypertension_drug )
  d_hp$HT2[d_hp$HT2 > 0] <- 1

## 고지혈증 데이터랑 합치기 
  dd5 <- left_join(d_hp, data3, by = 'ID')
  dd5$hyperlipidemia_drug[is.na(dd5$hyperlipidemia_drug)] <-0 
## 합친 데이터가 0이상이면 1로 변경. 
  dd5 <- dd5 %>% mutate( HL2 = HL + hyperlipidemia_drug )
  dd5$HL2[dd5$HL2 > 0] <- 1
  cols <- c("ID", "MI", "HF", "PV", "CV" , "Dementia","CPD", "Rheuma", "PUD", "MLD", "D", "DCC", "HP", 'Renal', "M", "MSLD","MST", "AIDS",'HT2','HL2','Sepsis', 'HTT')  
  dd5 <- select(dd5, all_of(cols) )
  print("describe final disease_history: ")
  str(dd5)
  return(dd5)
} 
# id, type(all disease list), dummies variabel: codition_types
# #신기능 수치 추출  #ID, 날짜, 신기능 수치들..

renal <- function(data){
# cleaning 
data$value_as_number[data$value_as_number == 'None'] <-1

# Before, Cr, BUN 추출 
renal1 <- data %>% dplyr::filter(measurement_type %in% c("BUN","Creatinine") & measurement_date < cohort_start_date) %>% dplyr::distinct(ID, cohort_start_date, measurement_type, value_as_number, measurement_date, gender, age)
renal1 <- as.data.frame(renal1) 
#check n 수 
print('renal 1: filter data')
N1=length(unique(renal1$ID))
print(N1)

renal2 <- renal1 %>% arrange(ID, measurement_type, desc(measurement_date)) %>% group_by(ID, measurement_type) %>% mutate(row = row_number() )
renal2<- as.data.frame(renal2)
print('renal 2; new column row ')
str(renal2)   
N2=length(unique(renal2$ID))
print(N2)
print("renal 3: select row ==1")
renal3 <- renal2 %>% dplyr::filter(row ==1)
str(renal3)   
N3=length(unique(renal3$ID))
print(N3)
print("start renal4; pivot ")
renal4 <- renal3 %>% distinct(ID, measurement_type, value_as_number, age, gender) %>% tidyr::pivot_wider(names_from = measurement_type, values_from = value_as_number)
renal4 <- as.data.frame(renal4)
N4= length(unique(renal4$ID))
print(N4)
print('renal4; pivot_wider')
str(renal4)
print("extract before bun, cr done")

#null, na 값은 1, 10 로 대체. 
renal4$Creatinine[is.na(renal4$Creatinine)] <- 1
renal4$BUN[is.na(renal4$BUN)] <- 10
# eGFR 계산
renal4$gender <- ifelse(renal4$gender == 'M', 0.742, 1)
renal4$egfr <- round(175* (renal4$Creatinine^(-1.154))* (renal4$age^(-0.203))* renal4$gender, 2)
renal4 <- unique(renal4[, c("ID", "BUN", "Creatinine",'egfr') ])
print('describe renal::')
str(renal4)
return(renal4)
}
## CCI 계산
## disease_history 결과값을 INPUT 값으로 넣기. 
cci<- function(data){
  data <- data %>% dplyr::distinct(ID, MI, HF, PV, CV, Dementia, CPD, Rheuma, PUD, MLD, D, DCC, HP, Renal, M, MSLD, MST, AIDS)
  data <- data %>% dplyr::mutate(cci = MI + HF + PV + CV + Dementia + CPD + Rheuma + PUD  + MLD + D + DCC*2 + HP*2 + Renal*2 + M*2 + MSLD*3 + MST*6 +AIDS*6)
  data <- data[,c("ID", "cci")]
  print("describe cci:: ")
  str(data)
  return(data)
}

})


## labeling so9
##파일 합치기. 
simplify <- module({
  import("strex")
  import("reshape")
  import("dplyr")
  import("tidyr")
  import('data.table')
  import('stats')
  import('base')
  import('utils')
pair<- function(data){
                    # pair 
                    before <- data %>% dplyr::filter(measurement_date < cohort_start_date) %>% dplyr::distinct(ID, measurement_type, value_as_number, measurement_date, cohort_type, cohort_start_date) 
                    before <- as.data.frame(before)
                    before2 <- before %>% dplyr::arrange(ID, measurement_type, desc(measurement_date)) %>% group_by(ID, measurement_type) %>% mutate( row = row_number())
                    before2<- as.data.frame(before2)
                    before3 <- before2 %>% dplyr::filter(row ==1)
                    before <- before3 %>% dplyr::distinct(ID, measurement_type, value_as_number, cohort_type, measurement_date) 
                    print("check before:::")
                    str(before)
                    after <- data %>% dplyr::filter(measurement_date >= cohort_start_date) 
                    after<- as.data.frame(after)
                    print("unique after")
                    after <- unique(after)
                    print("join start")
                    pair = inner_join(before, after, by= c("ID","measurement_type","cohort_type"), suffix =c(".before", ".after"))
                    print("unique pair")
                    pair = unique(pair)
                    print("check pair")
                    str(pair)
                    return(pair)
                    }
exposure <- function(pair){
                    #exposure
                    print("start exposure, filter latest") 
                    exposure <- pair %>% dplyr::filter(drug_exposure_start_date <= measurement_date.after & measurement_date.after <= drug_exposure_end_date) 
                    exposure <- as.data.frame(exposure)      
                    exposure2 <- exposure %>% arrange(ID, measurement_type, desc(measurement_date.after)) %>% group_by(ID, measurement_type) %>% mutate(row= row_number())
                    exposure <- as.data.frame(exposure2)               
                    print("start unique exposure")
                    exposure <- unique(exposure)
                    print("check exposure")
                    str(exposure)
                    return(exposure)
                    }
# # # rule out 
ruleout <- function(exposure, t1){
          # # # 1) ingredient 3 out 
                  print("start join exposure, and t1")
                  dc <- left_join(exposure, t1[,c("drug_concept_id","Name","type1","type2")], by = c("drug_concept_id") ) 
                  print("start unique join data")
                  dc<- unique(dc)
                  print("check dc ")
                  str(dc)
                  # ## 측정날짜 기준으로 약물 취합. 
                  # ## 용량군 정의 
                  print("fill NA of Name to  error_1  " )
                  dc$Name[is.na(dc$Name)] <- "no name"
                  print("start:: define dose group")
                  #fun<- function(x){strex::str_extract_numbers(x, decimals =TRUE)}
                  dose_list <- sapply(dc$Name, function(x) {strex::str_extract_numbers(x, decimals =TRUE)})
                  n= length(dose_list)
                  results <- vector(length = n)
                  print("complete extract dose from Name")
                  for( i in 1:n) {
                      num <- length(dose_list[[i]])
                      if(num == 1){
                        results[[i]] <- dose_list[[i]]
                      } else if(num == 2){ 
                        results[[i]] <- max(dose_list[[i]])
                      }else{
                        results[[i]] <- 1 
                      }  
                      }
                    print("complete extract only one dose  ")
                    dc$dose <- results
                    print("put dose columns in data frame ")
                  #   ##계산
                    dc$total_dose <- (dc$dose * as.numeric(dc$quantity)) / as.numeric(dc$days_supply)
                   print("complete put total_dose columns in data frame")
                    ## 용량군 정의 
                    dc$dose_type <- ifelse(dc$total_dose >= 1000, "high", "low")
                  print("complete put dose_type columns in data frame")
                    
                    ## 성분 만 추출
                  print("start:::extract ingredient")
                    dc2 <- as.data.frame(dc) # error 방지용 
                  print("start reshpae2::melt")
                    dc2 <- reshape2::melt(data= dc[,c("ID", "measurement_date.after","type1","type2")], id.vars =c("ID", "measurement_date.after"), measure.vars = c("type1", "type2"))
                  print("start making drug_list")
                    dc2 <- dc2 %>% group_by(ID, measurement_date.after) %>% summarise( drug_list = list(unique(value)))
                  
                    #성분 / metformin 갯수/ 병용 약물군 정의  
                  print("count ingredient")
                  fun <- function(x){
                    x1 = x[x !='']
                    n= length(x1)
                    return(n)
                  }
                  count_ <- sapply(dc2$drug_list, fun)
                  print("put ingredient_count columns in df")
                  dc2$ingredient_count <- count_
                  print("put metformin_count")
                  dc2$metformin_count <- sapply(  dc2$drug_list,  function(x) length(grep( "metformin", x)))
                  print("put drug_group")
                  dc2$drug_group <- sapply(dc2$drug_list, function(x) paste(x, collapse="/"))
                  #합치기. 
                  dc3 <-left_join(dc, dc2[,c("ID","measurement_date.after","ingredient_count","metformin_count","drug_group")], by=c("ID", "measurement_date.after"))
                      ## 필터링.
                  ruleout <- dc3 %>% dplyr::filter(ingredient_count < 3 )
                  ruleout <- ruleout %>% dplyr::filter((cohort_type=='T' & metformin_count !=0) | cohort_type=='C') 
                  #                  unique(dc3[which(dc3$ingredient_count<3) & ((dc3$cohort_type=='T')& (dc3$metformin_count !=0 )) ],)
                  ruleout <- ruleout %>% dplyr::distinct(ID, cohort_type, measurement_type, value_as_number.before, value_as_number.after, gender, age, dose_type, drug_group, measurement_date.before, measurement_date.after)
                  ## select latest measurement_data
                  print("ruleout:: select latest data")
                  ruleout2 <- ruleout %>% dplyr::arrange(ID, measurement_type, desc(measurement_date.after)) %>% group_by(ID, measurement_type) %>% dplyr::mutate( row = row_number())
                  ruleout2<- as.data.frame(ruleout2)
                  ruleout3 <- ruleout2 %>% dplyr::filter(row ==1)
                  ruleout3 <- ruleout3 %>% select(-row)
                  print("ruleout3:::")
                  str(ruleout3)
                  return(ruleout3)
                  }
})






