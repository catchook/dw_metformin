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

save_query <- function(sql, schema, db_target, db_control, con){
    sql <- gsub("_results_fnet_v276", "_results_dq_v276", sql)
    sql <- gsub("cdm_hira_2017", schema, sql) 
    sql <- gsub("target", db_target, sql)
    sql <- gsub("control", db_control, sql)
    result <- dbGetQuery(con, sql)
    result <- unique(result)
    result <- data.table::as.data.table(result)
    return(result)
    }

count_n <- function(df, step){
  table<-df %>% group_by(measurement_type, cohort_type) %>% summarise(pl = n_distinct(ID)) %>% arrange(measurement_type)
  tt <- as.data.table(table)
  groups <- split(tt, tt$cohort_type)
  test <- as.data.frame(rbind( t(groups$C[, c("measurement_type","pl")]) , t(groups$T[,"pl"] )) )
  names(test) <- as.character(unlist(test[1,]))
## delete rows,  add columns 
  test<- test[-1,]
  test[,'cohort_type']<- c( 'Control', 'Target' )
  test[,'step'] <- step 
  return(test)
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

## 시각화 , 단계별로 
fig2 <- function(df, step) {
  df <- df %>% mutate( cohort_type2 = recode(cohort_type,  "1"="control", "0" ="target"))
  cohort_type_ = as.factor(df$cohort_type2)
  options(repr.plot.width=15, repr.plot.height =5)
  a1=df %>% ggplot(aes(x=BUN, fill= cohort_type_)) + theme_classic() + geom_histogram(color="gray80", alpha=0.2, position = "identity", bins=100) 
  a2=df %>% ggplot(aes(x=Creatinine, fill= cohort_type_)) + theme_classic() + geom_histogram(color="gray80", alpha=0.2, position = "identity", bins= 100)
  a3=df %>% ggplot(aes(x=age, fill= cohort_type_)) + theme_classic() + geom_histogram(color="gray80", alpha=0.2, position = "identity", bins=100)

  fig<-plot_grid(a1, a2, a3, labels =c("BUN", "Cr" ,"Age"), align = "h", ncol=3)
  title<-ggdraw() + draw_label( paste0(step, " numeric variable") , fontface = "bold") 
  fig<-plot_grid(title, fig, cols=1, rel_heights = c(0.1, 1))
  ggsave(paste0(step,"_numeric.png"), fig, device = "png",  dpi=300, width=15, height=5)
  options(repr.plot.width=15, repr.plot.height =10)

  b1=df %>% ggplot(aes(x=SU, fill= cohort_type_ )) + theme_classic() + geom_bar()
  b2=df %>% ggplot(aes(x= alpha, fill= cohort_type_)) + theme_classic()+ geom_bar()
  b3=df %>% ggplot(aes(x=dpp4i, fill= cohort_type_)) + theme_classic()+ geom_bar()
  b4=df %>% ggplot(aes(x=gnd, fill= cohort_type_)) + theme_classic()  + geom_bar()
  b5=df %>% ggplot(aes(x=sglt2, fill= cohort_type_)) + theme_classic()+ geom_bar()
  b6=df %>% ggplot(aes(x=tzd, fill= cohort_type_)) + theme_classic()  + geom_bar()
  fig<- plot_grid(b1, b2 ,b3, b4, b5, b6,labels=c("SU", "ALPHA", "DPP-4I", "GND", "SGLT2","TZD") , ncol=3)
  title<-ggdraw() + draw_label( paste0(step, " co-medication "),  fontface = "bold") 
  fig<-plot_grid(title, fig, cols=1, rel_heights = c(0.1, 1))
  ggsave(paste0(step,"_co_drug.png"),fig, device = "png",  dpi=300, width=15, height=10)
}
## ps매칭 전후, smd
smd <- function(DF, step ){
  # label생성 
  DF$gender <- set_label(DF$gender, label ="gender")
  DF$gender<- set_labels(DF$gender, labels=c("male", "female"))
  DF$cohort_type <- set_label(DF$cohort_type, label ="cohort_type")
  DF$cohort_type<- set_labels(DF$cohort_type, labels=c("target", "control"))
  out = mytable(cohort_type~ age + gender + BUN +  Creatinine  + egfr +  SU + alpha+ dpp4i + gnd + sglt2 +tzd + MI + HF +PV + 
                    CV + CPD + RD+ PUD +MLD + DCC +HP + MSLD + AIDS + HT+ HL + Sepsis+ HTT + cci  , data =DF)
  mycsv(out, file=paste0("smd_", step, ".csv"))
  return(out)
} ##이후 mycsv(out, file="") 파일명으로 단계 구분하기. 
## dose type version: ps매칭 전후, smd
dose_smd <- function(DF, step ){
  # label생성 
  DF$gender <- set_label(DF$gender, label ="gender")
  DF$gender<- set_labels(DF$gender, labels=c("male", "female"))
  DF$dose_type <- set_label(DF$dose_type, label ="dose_type")
  DF$dose_type<- set_labels(DF$dose_type, labels=c("low", "high"))
  out = mytable(dose_type~ age + gender + BUN +  Creatinine  + egfr +  SU + alpha+ dpp4i + gnd + sglt2 +tzd + MI + HF +PV + 
                    CV + CPD + RD+ PUD +MLD + DCC +HP + MSLD + AIDS + HT+ HL + Sepsis+ HTT + cci  , data =DF)
  mycsv(out, file=paste0("dose_smd_", step, ".csv"))
  return(out)
} ##이후 mycsv(out, file="") 파일명으로 단계 구분하기. 

##qqplot --normal distribution 정규성 시각화 
qq <- function(stat){
for(i in unique(stat$measurement_type) ){
qqnorm(stat[which( stat$measurement_type== i ), diff] , main= paste(i,"Normal Distribution") )
qqline(stat[which( stat$measurement_type==i ), diff])
}}
## 정규성 검정. 
shapiro <- function(stat){
  results<- data.frame(cohort_type= NA, measruement_type= NA, Pvalue=NA)

  for ( i in unique(stat$cohort_type)){
    for( j in unique(stat$measurement_type)){
      if(length(stat[which(stat$measurement_type==j & stat$cohort_type==i), diff])<3 |length(stat[which(stat$measurement_type==j & stat$cohort_type==i), diff]) >5000 ) {
        out<- data.frame(cohort_type=i, measurement_type= j,Pvalue=0)
        names(out)<-names(results)
        results<- rbind(results, out)
      } else{      
        out<- shapiro.test( stat[which(stat$measurement_type==j & stat$cohort_type==i), diff])
        outs<- data.frame(cohort_type=i, measurement_type= j,Pvalue= out[[2]])
        names(outs)<-names(results)
        results<- rbind(results, outs)

      }
    }
  }
  return(results)
}

# ## rate: 정규성, 등분산성, ttest, wilcox
test_rate <- function(stat){
  results<- data.frame( m_type= NA, normality_pvalue_target= NA, normality_pvalue_control= NA, var_pvalue= NA, ttest_pvalue = NA,  wilcoxon_pvalue = NA, target_mean = NA, control_mean = NA)
  m_list <- unique(stat$measurement_type)
  for( j in m_list){
      condition = stat$measurement_type==j &  stat$cohort_type==0
      target=  unique(subset(stat[which(condition)], select =c("ID","rate")))
      condition = stat$measurement_type==j &  stat$cohort_type==1
      control= unique(subset(stat[which(condition)], select =c("ID","rate")))
      condition = stat$measurement_type==j
      total= unique(subset(stat[which(condition)], select =c("ID","cohort_type","rate")))

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
        bars<- tapply(total$rate, total$cohort_type, mean )
        lower<- tapply(total$rate, total$cohort_type, function(x) t.test(x)$conf.int[1])
        upper<- tapply(total$rate, total$cohort_type, function(x) t.test(x)$conf.int[2])
        png(file=paste(j,"rate_test.png"))
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
      condition = stat$measurement_type==j &  stat$cohort_type==0
      target=  unique(subset(stat[which(condition)], select =c("ID","diff")))
      condition = stat$measurement_type==j &  stat$cohort_type==1
      control= unique(subset(stat[which(condition)], select =c("ID","diff")))
      condition = stat$measurement_type==j
      total= unique(subset(stat[which(condition)], select =c("ID","cohort_type","diff")))

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
        png(file=paste(j,"diff_test.png"))
        barplot2(bars, space=0.4, xlim=c(0,3.0), plot.ci=TRUE, ci.l = lower, ci.u= upper, ci.color ="maroon", ci.lwd=4, names.arg=c("target","control"), col=c("coral", "darkkhaki"), xlab ="cohort type", ylab= "rate", 
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
  total=  unique(subset(stat[which(stat$cohort_type==0)] ))
  results<- data.frame(d_type=NA,  m_type= NA, normality_pvalue_pre= NA, normality_pvalue_post= NA, var_pvalue= NA, ttest_pvalue = NA,  wilcoxon_pvalue = NA, pre_mean = NA, post_mean = NA, mean_diff= NA)
  drug_list=c("alpha", "SU","metformin", "gnd","tzd","dpp4i", "sglt2")
  m_list <- unique(total$measurement_type)
  for (j in m_list){  
        total_m <- unique(subset(total[which(target$measurement_type==j)]))
    
        if (nrow(total_m) <3 | nrow(total_m) >5000) {
          out<- data.frame("total",j, NA, NA,NA, NA,NA, NA, NA, NA )
          names(out)<-names(results)
          results<- rbind(results, out)
            } else {      
          normality_pre <- shapiro.test(total_m$value_as_number_before)
          normality_post <- shapiro.test(total_m$value_as_number_after)
          variance <- var.test(total_m$value_as_number_before , total_m$value_as_number_after)
          ptest <- t.test(total_m$value_as_number_after , total_m$value_as_number_before, paired =TRUE)
          wilcox <- wilcox.test(total_m$value_as_number_before , total_m$value_as_number_after,exact = FALSE)

          ## 소수점 2자리에서 반올림
          normality_pre = round(normality_pre$p.value , digits = 2)
          normality_post = round(normality_post$p.value , digits = 2)
          variance = round(variance$p.value , digits = 2)
          ptest_p<- round(ptest$p.value, digits = 2)
          wilcox <-round(wilcox$p.value, digits = 2) 
          mean_diff <- round(as.numeric(gsub("^[a-z]","", ptest$estimate)), digits=2 )
          pre_mean <- round(mean(target$value_as_number_before, na.rm=TRUE), digits=2)
          post_mean <- round(mean(target$value_as_number_after, na.rm=TRUE), digits=2)

          ## 합치기 
          outs<- data.frame("total",j, normality_pre, normality_post, variance, ptest_p, wilcox, pre_mean, post_mean, mean_diff)
          names(outs)<-names(results)
          results<- rbind(results, outs)
    
          ## 그림 
          bars<- sapply(total_m[,4:5], function(x) mean(x, na.rm=TRUE))
          lower <- sapply(total_m[,4:5], function(x) t.test(x)$conf.int[1])
          upper<- sapply(total_m[,4:5], function(x) t.test(x)$conf.int[2])
          png(file=paste("total_",j,"_value_ptest.png"))
          barplot2(bars, space=0.4, plot.ci=TRUE, ci.l= lower, ci.u= upper, ci.color="maroon", ci.lwd=4, names.arg=c("pre","post"), col=c("coral","darkkhaki"), xlab="pre vs post", ylab = "value", 
          main =paste0("target total"," : ",j," value with Confidence Interval"))
          dev.off()
      }}
  for( i in drug_list){
    target_drug <-total[grep(i, total$drug_group) , ]
    for (j in m_list){  
        target <- unique(subset(target_drug[which(target_drug$measurement_type==j)]))
    
        if (nrow(target) <3 | nrow(target) >5000) {
          out<- data.frame(i,j, NA, NA,NA, NA,NA, NA, NA, NA )
          names(out)<-names(results)
          results<- rbind(results, out)
            } else {      
          normality_pre <- shapiro.test(target$value_as_number_before)
          normality_post <- shapiro.test(target$value_as_number_after)
          variance <- var.test(target$value_as_number_before , target$value_as_number_after)
          ptest <- t.test(target$value_as_number_after , target$value_as_number_before, paired =TRUE)
          wilcox <- wilcox.test(target$value_as_number_before , target$value_as_number_after,exact = FALSE)

          ## 소수점 2자리에서 반올림
          normality_pre = round(normality_pre$p.value , digits = 2)
          normality_post = round(normality_post$p.value , digits = 2)
          variance = round(variance$p.value , digits = 2)
          ptest_p<- round(ptest$p.value, digits = 2)
          wilcox <-round(wilcox$p.value, digits = 2) 
          mean_diff <- round(as.numeric(gsub("^[a-z]","", ptest$estimate)), digits=2 )
          pre_mean <- round(mean(target$value_as_number_before, na.rm=TRUE), digits=2)
          post_mean <- round(mean(target$value_as_number_after, na.rm=TRUE), digits=2)

          ## 합치기 
          outs<- data.frame(i,j, normality_pre, normality_post, variance, ptest_p, wilcox, pre_mean, post_mean, mean_diff)
          names(outs)<-names(results)
          results<- rbind(results, outs)
    
          ## 그림 
          bars<- sapply(target[,4:5], function(x) mean(x, na.rm=TRUE))
          lower <- sapply(target[,4:5], function(x) t.test(x)$conf.int[1])
          upper<- sapply(target[,4:5], function(x) t.test(x)$conf.int[2])
          png(file=paste(i,j,"value_ptest.png"))
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
  condition = stat$dose_type==1
  high = unique(stat[which(condition),] ) 
  low = unique(stat[which(!condition),]) 

  results<- data.frame( m_type= NA, r_normality_h= NA, r_normality_l= NA, r_variance= NA, r_ttest_p = NA,  r_wilcox = NA, r_high_mean = NA, r_low_mean = NA,
   d_normality_h= NA  , d_normality_l= NA, d_variance= NA, d_ttest_p = NA,  d_wilcox = NA, d_high_mean = NA, d_low_mean = NA, p)
  m_list <- unique(stat$measurement_type)
  for( j in m_list){
      high_m=  unique(high[which(high$measurement_type==j), ] )
      low_m=  unique(low[which(low$measurement_type==j), ] )
      total_m = unique(stat[which(stat$measurement_type==j), ] )
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
        png(file=paste0("dose_",j,"_rate.png"))
        barplot2(r_bars, space=0.4, xlim=c(0,3.0), plot.ci=TRUE, ci.l = r_lower, ci.u= r_upper, ci.color ="maroon", ci.lwd=4, names.arg=c("high","low"), col=c("coral", "darkkhaki"), xlab ="dose type", ylab= "rate", 
        main=paste0(j," rate by dose type with Confidence Interval" ) )
        dev.off()
        d_bars <- tapply(total_m$diff, total_m$dose_type, function(x) mean(x, na.rm=TRUE))
        d_lower<- tapply(total_m$diff, total_m$dose_type, function(x) t.test(x)$conf.int[1])
        d_upper<- tapply(total_m$diff, total_m$dose_type, function(x) t.test(x)$conf.int[2])
        png(file=paste0("dose_",j,"_diff.png"))
        barplot2(d_bars, space=0.4, xlim=c(0,3.0), plot.ci=TRUE, ci.l = d_lower, ci.u= d_upper, ci.color ="maroon", ci.lwd=4, names.arg=c("high","low"), col=c("coral", "darkkhaki"), xlab ="dose type", ylab= "diff", 
        main=paste0(j," diff by dose type with Confidence Interval" ) )
        dev.off()
      }
    }
      results <- na.omit(results)
      return(results)
  }
# paired ttest rate test: 등분산성, 정규성, t-test, wilcox test 
## 용량군별,, 따로. 
dose_ptest <- function(total, type){
  results<- data.frame(dose_type =NA,m_type= NA, normality_pvalue_pre= NA, normality_pvalue_post= NA, var_pvalue= NA, ttest_pvalue = NA,  wilcoxon_pvalue = NA,pre_mean = NA, post_mean = NA, mean_diff= NA)
  m_list <- unique(total$measurement_type)
  for (j in m_list){  
        total_m <- unique(total[which(total$measurement_type==j),])

       if (nrow(total_m) < 7) {
        out<- data.frame(j,NA, NA, NA, NA, NA, NA, NA, NA )
        names(out)<-names(results)
        results<- rbind(results, out)
          } else {      
        normality_pre <- ad.test(total_m$value_as_number_before)
        normality_post <- ad.test(total_m$value_as_number_after)
        variance <- var.test(total_m$value_as_number_before , total_m$value_as_number_after)
        ptest <- t.test(total_m$value_as_number_after , total_m$value_as_number_before, paired =TRUE)
        wilcox <- wilcox.test(total_m$value_as_number_before , total_m$value_as_number_after,exact = FALSE)
        ## 소수점 2자리에서 반올림
        normality_pre = round(normality_pre$p.value , digits = 2)
        normality_post = round(normality_post$p.value , digits = 2)
        variance = round(variance$p.value , digits = 2)
        ptest_p<- round(ptest$p.value, digits = 2)
        wilcox <-round(wilcox$p.value, digits = 2) 
        mean_diff <- round(as.numeric(gsub("^[a-z]","", ptest$estimate)), digits=2 )
        pre_mean <- round(mean(total_m$value_as_number_before, na.rm=TRUE), digits=2)
        post_mean <- round(mean(total_m$value_as_number_after, na.rm=TRUE), digits=2)
        ## 합치기 
        outs<- data.frame(type ,j, normality_pre, normality_post, variance, ptest_p, wilcox, pre_mean, post_mean, mean_diff)
        names(outs)<-names(results)
        results<- rbind(results, outs)
        ## 그림 
        bars<- sapply(total_m[,6:7], function(x) mean(x, na.rm=TRUE))
        lower <- sapply(total_m[,6:7], function(x) t.test(x)$conf.int[1])
        upper<- sapply(total_m[,6:7], function(x) t.test(x)$conf.int[2])
        png(file=paste0(type,"_dose_",j,"_ptest.png"))
        barplot2(bars, space=0.4, plot.ci=TRUE, ci.l= lower, ci.u= upper, ci.color="maroon", ci.lwd=4, names.arg=c("pre","post"), col=c("coral","darkkhaki"), xlab="pre vs post", ylab = "value", 
        main =paste0(type,"_dose"," : ",j," with Confidence Interval"))
        dev.off()
       }}
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
# drug history # ps_1st
##파일 합치기. 
drug_history <- function(data, t1){
  dc <- left_join(data, t1[,c("drug_concept_id","Name","type1","type2")], by = c("drug_concept_id") ) 
  dc<- unique(dc)
## get dummies 
  dc2 <- melt(dc[,c("ID", "type1","type2")], id.vars =c("ID"), measure.vars=c("type1", "type2"))
  dc2<-dc2 %>% group_by(ID) %>% summarise(type = list(unique(value)))
  columns = c("SU", "alpha", "dpp4i", "gnd", "metformin", "sglt2", "tzd")
  dc3 <- sapply(dc2$type, function(x) table(factor(x, levels= columns)))
  dc3 <-t(dc3)
  dc4<-cbind(dc2, dc3)  #id, type(all drug list ), dummmies variable SU", "alpha", "dpp4i", "gnd", "metformin", "sglt2", "tzd
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
  dd <- melt(df[,c("ID", "condition_type")], id.vars = "ID" , measure.vars="condition_type") 
  l <- unique(dd$value)
  dd2 <-dd %>% group_by(ID) %>% summarise(dtype = list(unique(value)))
  dd3 <- sapply(dd2$dtype ,function(x) table(factor(x, levels =l)) )
  dd4 <- t(dd3)
  dd4 <- cbind(dd2, dd4)
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
  return(dd5)
} 
# id, type(all disease list), dummies variabel: codition_types
# #신기능 수치 추출  #ID, 날짜, 신기능 수치들..

renal <- function(data){
# Before, Cr, BUN 추출 
renal <- data %>% dplyr::distinct( ID, cohort_start_date, measurement_type, value_as_number, measurement_date, gender, age) %>% dplyr::filter( (measurement_date < cohort_start_date) & (measurement_type %in% c("BUN","Creatinine")))  %>% dplyr::group_by(ID,measurement_type ) %>% dplyr::filter(measurement_date == max(measurement_date)) %>% tidyr::pivot_wider(names_from = measurement_type, values_from= value_as_number )
print("complete, extract latest bun, cr")
#null, na 값은 1, 10 로 대체. 
renal$Creatinine[is.na(renal$Creatinine)] <- 1
renal$Creatinine[renal$Creatinine== 'None'] <-1
renal$BUN[is.na(renal$BUN)] <- 10
renal$BUN[renal$BUN =='None'] <-10
print("replace completet")
# eGFR 계산
print("ifelse")
renal$gender <- ifelse(renal$gender == 'M', 0.742, 1)
print("as.numeric")
#class(renal$age) ='Numeric'
# print("apply function")
renal[,c("age","Creatinine")] <- lapply(renal[,c("age","Creatinine")], as.numeric)
# renal<-transform(renal, Creatinine = as.numeric(Creatinine),
#                 age = as.numeric(age))
# renal$Creatinine <- as.numeric(renal$Creatinine)
# #renal$age <- as.numeric(renal$age)
 print("calculate egfr")
renal$egfr <- round(175* (renal$Creatinine^(-1.154))* (renal$age^(-0.203))* renal$gender, 2)
renal <- unique(renal[, c("ID", "BUN", "Creatinine") ])
return(renal)
}
## CCI 계산
## disease_history 결과값을 INPUT 값으로 넣기. 
cci<- function(sample2){  
  sample2$cci <-sample2$MI + sample2$HF + sample2$PV + sample2$CV + sample2$Dementia + sample2$CPD + sample2$Rheuma + sample2$PUD + sample2$MLD + sample2$D + sample2$DCC *2 + sample2$HP*2 + sample2$Renal*2 + sample2$M*2 + sample2$MSLD*3 + sample2$MST*6 + sample2$AIDS*6
  # sample2$cci <-sample2$MI + sample2$HF + sample2$PV + sample2$CV +  sample2$CPD + sample2$RD + sample2$PUD + sample2$D + sample2$DCC *2 + sample2$RD*2  + sample2$MSLD*3 + sample2$MST*6 + sample2$AIDS*6
  # sample2 <-sample2[,c("ID", "cci")]
  return(sample2)
}

})


## labeling 
##파일 합치기. 
simplify <- module({
  import("strex")
  import("reshape")
  import("dplyr")
  import("tidyr")
  import('data.table')
pair<- function(data){
                    # pair 
                    before <-  unique(data[which(data$measurement_date < data$cohort_start_date), c("ID", "measurement_type", "value_as_number", "measurement_date","cohort_type")])
                    before <- before %>% group_by(ID) %>% filter(measurement_date == max(measurement_date))
                    after <-  unique(data[which(data$measurement_date >= data$cohort_start_date), ])
                    pair = inner_join(before, after, by= c("ID","measurement_type","cohort_type"), suffix =c(".before", ".after"))
                    pair = unique(pair)
                    return(pair)
                    }
exposure <- function(pair){
                    #exposure 
                    exposure <- unique(pair[which((pair$drug_exposure_start_date <= pair$measurement_date.after ) & (pair$measurement_date.after <= pair$drug_exposure_end_date)), ])
                    # exposure 중에 가장 최근걸로. 고르기. 
                    exposure2 <- exposure %>% group_by(ID) %>% filter(measurement_date.after == max(measurement_date.after))
                    exposure2 = unique(exposure2)
                    return(exposure2)
                    }

# # # rule out 
ruleout <- function(exposure2, t1){
          # # # 1) ingredient 3 out 
                  dc <- left_join(exposure2, t1[,c("drug_concept_id","Name","type1","type2")], by = c("drug_concept_id") ) 
                  dc<- unique(dc)
                  # ## 측정날짜 기준으로 약물 취합. 
                  # ## 용량군 정의 
                  fun<- function(x){str_extract_numbers(x, decimals =TRUE)}
                  dose_list <- lapply(dc["Name"],  fun )
                  results <- list()
                  for( i in dose_list$Name){
                      if(length(i)==1){
                        x= as.numeric(i)
                        results <- append(results, x)
                      } else if (length(i)==2){ 
                        x= as.numeric(i[1])
                        y=as.numeric(i[2])
                            if(x > y){
                              results <-append(results, x)
                            }else{results<- append(results, y)}
                      }else{
                        results<-append(results, "error")
                      }    }
                    dc$dose <- unlist(results)
                  #   ##계산
                    dc$total_dose <- (dc$dose * as.numeric(dc$quantity)) / as.numeric(dc$days_supply)
                    ## 용량군 정의 
                    results <- list()
                    for( i in dc$total_dose ){
                          if(i >= 1000){
                                  results <- append(results, "high")
                          }else{
                            results<- append(results, "low")}
                        }
                      dc$dose_type <- unlist(results) 
                    ## 성분 만 추출
                      dc <- as.data.frame(dc) # error 방지용 
                      dc2 <- reshape::melt(data= dc[,c("ID", "measurement_date.after","type1","type2")], id.vars =c("ID", "measurement_date.after"), measure.vars = c("type1", "type2"))
                      dc2<-dc2 %>% group_by(ID, measurement_date.after) %>% summarise( drug_list = list(unique(value)))
                      #성분 / metformin 갯수/ 병용 약물군 정의  
                      dc2$ingredient_count <- length(unique(unlist(dc2$drug_list)))
                      dc2$metformin_count<- lapply(   dc2$drug_list,  function(x) length(grep( "metformin", x) ))
                      dc2$drug_group <- lapply(dc2$drug_list, function(x) paste(x, collapse="/"))
                  #합치기. 
                      dc3 <-left_join(dc, dc2[c("ID","measurement_date.after","ingredient_count","metformin_count","drug_group")], by=c("ID", "measurement_date.after"))
                      ## 필터링.
                      exposure3 <-unique(dc3[which(dc3$ingredient_count<3) & ((dc3$cohort_type==0)& (dc3$metformin_count !=0 )) ],)
                      return(exposure3)
                  }
})






