#total = read.table(file.choose(),header = FALSE)
cutpoints <- seq( 1 , 2886 , by = 3 )
categories <- findInterval( 1:2886 , cutpoints )
#test.ind = sample(127,127*0.3)
#train.ind = -test.ind
####################################################################

Jan15x = read.table(file = "E:/Regression Analysis/Jan15x",header = FALSE)
Jan15y = read.table(file = "E:/Regression Analysis/Jan15y",header = FALSE)

Feb15x = read.table(file = "E:/Regression Analysis/Feb15x",header = FALSE)
Feb15y = read.table(file = "E:/Regression Analysis/Feb15y",header = FALSE)

Mar15x = read.table(file = "E:/Regression Analysis/Mar15x",header = FALSE)
Mar15y = read.table(file = "E:/Regression Analysis/Mar15y",header = FALSE)

Apr15x = read.table(file = "E:/Regression Analysis/Apr15x",header = FALSE)
Apr15y = read.table(file = "E:/Regression Analysis/Apr15y",header = FALSE)

May15x = read.table(file = "E:/Regression Analysis/May15x",header = FALSE)
May15y = read.table(file = "E:/Regression Analysis/May15y",header = FALSE)

Jun15x = read.table(file = "E:/Regression Analysis/Jun15x",header = FALSE)
Jun15y = read.table(file = "E:/Regression Analysis/Jun15y",header = FALSE)

Jul15x = read.table(file = "E:/Regression Analysis/Jul15x",header = FALSE)
Jul15y = read.table(file = "E:/Regression Analysis/Jul15y",header = FALSE)

Aug15x = read.table(file = "E:/Regression Analysis/Aug15x",header = FALSE)
Aug15y = read.table(file = "E:/Regression Analysis/Aug15y",header = FALSE)

Sep15x = read.table(file = "E:/Regression Analysis/Sep15x",header = FALSE)
Sep15y = read.table(file = "E:/Regression Analysis/Sep15y",header = FALSE)

Oct15x = read.table(file = "E:/Regression Analysis/Oct15x",header = FALSE)
Oct15y = read.table(file = "E:/Regression Analysis/Oct15y",header = FALSE)

Nov15x = read.table(file = "E:/Regression Analysis/Nov15x",header = FALSE)
Nov15y = read.table(file = "E:/Regression Analysis/Nov15y",header = FALSE)

Dec15x = read.table(file = "E:/Regression Analysis/Dec15x",header = FALSE)
Dec15y = read.table(file = "E:/Regression Analysis/Dec15y",header = FALSE)


names(Dec15x)[1] = "businessId"
names(Dec15x)[2] = "yymm"
names(Dec15x)[3] = "totalReview"
names(Dec15x)[4] = "fiveStar"
names(Dec15x)[5] = "fourStar"
names(Dec15x)[6] = "threeStar"
names(Dec15x)[7] = "twoStar"
names(Dec15x)[8] = "oneStar"
names(Dec15y)[1] = "businessId"
names(Dec15y)[2] = "yymm"
names(Dec15y)[3] = "totalReview"
names(Dec15y)[4] = "fiveStar"
names(Dec15y)[5] = "fourStar"
names(Dec15y)[6] = "threeStar"
names(Dec15y)[7] = "twoStar"
names(Dec15y)[8] = "oneStar"

Dec15One = tapply( Dec15x[,8] , categories , sum )
Dec15Two = tapply( Dec15x[,7] , categories , sum )
Dec15Three = tapply( Dec15x[,6], categories, sum)
Dec15Four = tapply( Dec15x[,5] , categories , sum )
Dec15Five = tapply( Dec15x[,4] , categories , sum )
Dec15Total = data.frame(Dec15One,Dec15Two,Dec15Three,Dec15Four,Dec15Five, Dec15y[,3])
names(Dec15Total)[6] = 'y'



## CV
N = nrow(Dec15Total)
K = 10
index = rep(K,N)
for (j in 1:(K-1)){
  index[sample((1:N)[index > (j-1)], N/K, replace = F)] <- j
}

Dec15mse = rep(0,K)
Dec15pre = rep(0,K)

Dec15adjrsqure = rep(0,K)
Dec15fstats = rep(0,K)
Dec15StarOne = rep(0,K)
Dec15StarTwo = rep(0,K)
Dec15StarThree = rep(0,K)
Dec15StarFour = rep(0,K)
Dec15StarFive = rep(0,K)
for (j in 1:K){
  test = index == j
  train = !test
  Dec15fit = lm( y ~ ., data = Dec15Total[train,])
  Dec15fitpre = predict(Dec15fit, newdata = Dec15Total[test,])
  Dec15mse[j] = mean((Dec15fitpre-Dec15Total[test,6])^2)
  Dec15pre[j] = sqrt(Dec15mse[j])/mean(Dec15Total[test,6])
  
  #Dec15adjrsqure[j] = sumDecy(Dec15fit)$adj.r.squared
  
  #Dec15StarOne[j] = sumDecy(Dec15fit)$coefficient[2]
  #Dec15StarTwo[j] = sumDecy(Dec15fit)$coefficient[3]
  #Dec15StarThree[j] = sumDecy(Dec15fit)$coefficient[4]
  #Dec15StarFour[j] = sumDecy(Dec15fit)$coefficient[5]
  #Dec15StarFive[j] = sumDecy(Dec15fit)$coefficient[6]
  
  #Dec15fstats[j] = sumDecy(Dec15fit)$fstatistic[1]
}
#Dec15mse.mean = mean(Dec15mse)
#Dec15mse.sd = sd(Dec15mse)
#Dec15adjrsqure.mean = mean(Dec15adjrsqure)
#Dec15fstats.mean = mean(Dec15fstats)

Dec15pre.mean = mean(Dec15pre)
Dec15pre.sd = sd(Dec15pre)

## dataframe
#Data = data.frame(Jan15mse,Jan15adjrsqure,Jan15StarOne,Jan15StarTwo,Jan15StarThree,Jan15StarFour,Jan15StarFive,Feb15mse,Feb15adjrsqure,Feb15StarOne,Feb15StarTwo,Feb15StarThree,Feb15StarFour,Feb15StarFive,Mar15mse,Mar15adjrsqure,Mar15StarOne,Mar15StarTwo,Mar15StarThree,Mar15StarFour,Mar15StarFive,Apr15mse,Apr15adjrsqure,Apr15StarOne,Apr15StarTwo,Apr15StarThree,Apr15StarFour,Apr15StarFive,May15mse,May15adjrsqure,May15StarOne,May15StarTwo,May15StarThree,May15StarFour,May15StarFive,Jun15mse,Jun15adjrsqure,Jun15StarOne,Jun15StarTwo,Jun15StarThree,Jun15StarFour,Jun15StarFive,Jul15mse,Jul15adjrsqure,Jul15StarOne,Jul15StarTwo,Jul15StarThree,Jul15StarFour,Jul15StarFive,Aug15mse,Aug15adjrsqure,Aug15StarOne,Aug15StarTwo,Aug15StarThree,Aug15StarFour,Aug15StarFive,Sep15mse,Sep15adjrsqure,Sep15StarOne,Sep15StarTwo,Sep15StarThree,Sep15StarFour,Sep15StarFive,Oct15mse,Oct15adjrsqure,Oct15StarOne,Oct15StarTwo,Oct15StarThree,Oct15StarFour,Oct15StarFive,Nov15mse,Nov15adjrsqure,Nov15StarOne,Nov15StarTwo,Nov15StarThree,Nov15StarFour,Nov15StarFive,Dec15mse,Dec15adjrsqure,Dec15StarOne,Dec15StarTwo,Dec15StarThree,Dec15StarFour,Dec15StarFive)
#write.csv(Data, "LRStatisticsData.csv", col.names=TRUE)
