library(knitr)
library(texreg)
library(foreign)
library(sandwich)
library(lmtest)
library(lfe)
library(plm)



nfirms <- 500 # Number of firms
nobs <- 10      # Number of observations per firm

# Add a time index, first define a group apply function
# that applies by group index.
gapply <- function(x, group, fun) {
  returner <- numeric(length(group))
  for (i in unique(group)) 
    returner[i==group] <- get("fun")(x[i==group])
  returner
}

#create dataset to store all regression results
final <- data.frame(var=1,res=1,res=1,res=1,res=1)
colnames(final)=c("var","res2","res2","res2","res2")

#Table-1: Std errors with firm fixed effects
for(i in 1:4){
  
  #create dataset to store average regression results from simulations
  result2 <- data.frame(var=1:5)
  
  for(j in 1:4){
    
    count1=0
    count2=0
    
    #create dataset to store regression results
    result <- data.frame(var=1:5)
    
    #run 5000 simulations
    for(k in 1:5){
        sim=5000
      
        # Let's say: x = mu + nu and e = gamma + eta
        x.sd  <- 1 # Specify the total standard deviation of x
        e.sd <- 2 #Specify the total standard deviation of error
        mu.sd <- (i-1)*0.25 # Spefify the standard deviation of the fixed effect in x; Change this variablility from 0 to 75 at intervals of 25
        gamma.sd <-(j-1)*0.25 # Spefify the standard deviation of the fixed effect in x; Change this variablility from 0 to 75 at intervals of 25
        
        #generate data using the time invariant effects for x and e
        constantdata <- data.frame(id=1:nfirms, mu=rnorm(nfirms))
        constantdata$gamma <- rnorm(nfirms)
        
        # expand data by nobs
        fulldata <- constantdata[rep(1:nfirms, each=nobs),]
        
        # Using the generalized apply function coded above
        fulldata$t <- gapply(rep(1,length(fulldata$id)), 
                             group=fulldata$id, 
                             fun=cumsum)
        
        # Now caculate the time varying component of x and e
        fulldata$x <- fulldata$mu + rnorm(nobs*nfirms)
        fulldata$e <- fulldata$gamma + rnorm(nobs*nfirms)
        
        # simulate y variables
        fulldata$y <- 1*fulldata$x + fulldata$e
        
        #simple OLS, and OLS with clustered std errors at firm level
        sum1=summary(lm(formula = y ~ x, data = fulldata))
        sum2=summary(felm(y ~ x | 0 | 0 | id, data=fulldata)) # cluster by firm only
        
        #store the results
        res=matrix(NA,5,1)
        res[1]=sum1$coefficients[2]
        res[2]=sum1$coefficients[4]
        res[3]=sum1$coefficients[6]
        res[4]=sum2$coefficients[4]
        res[5]=sum2$coefficients[6]
        
        if(abs(res[3])>2.58){count1=count1+1}
        if(abs(res[5])>2.58){count2=count2+1}
        
        
        result <- cbind(result,res)
    }

    res2=matrix(NA,5,1)
    res2[1]=rowMeans(result[1,])
    res2[2]=rowMeans(result[2,])
    res2[3]=count1/sim
    res2[4]=rowMeans(result[4,])
    res2[5]=count2/sim
    result2 <- cbind(result2,res2)
  }
  final<-rbind(final,result2)
}

write.csv(final, file = "table1.csv")


#create dataset to store all regression results
final2 <- data.frame(var=1,res=1,res=1,res=1,res=1)
colnames(final2)=c("var","res2","res2","res2","res2")

#Table-3: Std errors with time fixed effects
for(i in 1:4){
  
  #create dataset to store average regression results from simulations
  result2 <- data.frame(var=1:5)
  
  for(j in 1:4){
    
    count1=0
    count2=0
    
    #create dataset to store regression results
    result <- data.frame(var=1:5)
    
    #run 5000 simulations
    for(k in 1:50){
      sim=50
      
      # Let's say: x = mu + nu and e = gamma + eta
      x.sd  <- 1 # Specify the total standard deviation of x
      e.sd <- 2 #Specify the total standard deviation of error
      mu.sd <- (i-1)*0.25 # Spefify the standard deviation of the fixed effect in x; Change this variablility from 0 to 75 at intervals of 25
      gamma.sd <-(j-1)*0.25 # Spefify the standard deviation of the fixed effect in x; Change this variablility from 0 to 75 at intervals of 25
      
      #generate data using the time invariant effects for x and e
      constantdata <- data.frame(id=1:nfirms, mu=rnorm(nfirms))
      constantdata$gamma <- rnorm(nfirms)
      
      # expand data by nobs
      fulldata <- constantdata[rep(1:nfirms, each=nobs),]
      
      # Using the generalized apply function coded above
      fulldata$t <- gapply(rep(1,length(fulldata$id)), 
                           group=fulldata$id, 
                           fun=cumsum)
      
      # Now caculate the time varying component of x and e
      fulldata$x <- fulldata$mu + rnorm(nobs*nfirms)
      fulldata$e <- fulldata$gamma + rnorm(nobs*nfirms)
      
      # simulate y variables
      fulldata$y <- 1*fulldata$x + fulldata$e
      
      #simple OLS, and OLS with clustered std errors at firm level
      sum1=summary(lm(formula = y ~ x, data = fulldata))
      sum2=summary(felm(y ~ x | 0 | 0 | t, data=fulldata)) # cluster by firm only
      
      #store the results
      res=matrix(NA,5,1)
      res[1]=sum1$coefficients[2]
      res[2]=sum1$coefficients[4]
      res[3]=sum1$coefficients[6]
      res[4]=sum2$coefficients[4]
      res[5]=sum2$coefficients[6]
      
      if(abs(res[3])>2.58){count1=count1+1}
      if(abs(res[5])>2.58){count2=count2+1}
      
      
      result <- cbind(result,res)
    }
    
    res2=matrix(NA,5,1)
    res2[1]=rowMeans(result[1,])
    res2[2]=rowMeans(result[2,])
    res2[3]=count1/sim
    res2[4]=rowMeans(result[4,])
    res2[5]=count2/sim
    result2 <- cbind(result2,res2)
  }
  final2<-rbind(final2,result2)
}

write.csv(final2, file = "table3.csv")