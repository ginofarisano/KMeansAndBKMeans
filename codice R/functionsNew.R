printPlot1<- function(){
color <- c('red','blue','yellow','violet','orange','green','pink','cyan','purple')
par(mfcol=c(3,3))
label <- c('Lettere','Economia','Fisica','Ingegneria','Matematica','Medicina','Arte','Lingue','Biologia')
################
value <- c(19,7,74,706,279,2,14,0,12)
mymax<-max(value)
br<-barplot(value,ylim=c(0,max(value)),ylab='cluster1',col=color)
text(x=br,value+(mymax/20),labels=paste(value),xpd=NA)
text(x=br,-(mymax/3),labels=label,srt=90,xpd=NA)

############
value <- c(297,49,113,1391,1886,7,142,2,79)
mymax<-max(value)
br<-barplot(value,ylim=c(0,max(value)),ylab='cluster2',col=color)
text(x=br,value+(mymax/20),labels=paste(value),xpd=NA)
text(x=br,-(mymax/3),labels=label,srt=90,xpd=NA)

############
value <- c(11,13,30,256,4110,1,6,7,13)
mymax<-max(value)
br<-barplot(value,ylim=c(0,max(value)),ylab='cluster3',col=color)
text(x=br,value+(mymax/20),labels=paste(value),xpd=NA)
text(x=br,-(mymax/3),labels=label,srt=90,xpd=NA)

############
value <- c(0,1,0,64,0,1,0,0,1)
mymax<-max(value)
br<-barplot(value,ylim=c(0,max(value)),ylab='cluster4',col=color)
text(x=br,value+(mymax/20),labels=paste(value),xpd=NA)
text(x=br,-(mymax/3),labels=label,srt=90,xpd=NA)

############
value <- c(562,1,25,33,3,1,243,1,2)
mymax<-max(value)
br<-barplot(value,ylim=c(0,max(value)),ylab='cluster5',col=color)
text(x=br,value+(mymax/20),labels=paste(value),xpd=NA)
text(x=br,-(mymax/3),labels=label,srt=90,xpd=NA)

############
value <- c(69,9,500,1824,762,0,30,0,22)
mymax<-max(value)
br<-barplot(value,ylim=c(0,max(value)),ylab='cluster6',col=color)
text(x=br,value+(mymax/20),labels=paste(value),xpd=NA)
text(x=br,-(mymax/3),labels=label,srt=90,xpd=NA)

############
value <- c(69,13,0,1178,3,63,26,3,7)
mymax<-max(value)
br<-barplot(value,ylim=c(0,max(value)),ylab='cluster7',col=color)
text(x=br,value+(mymax/20),labels=paste(value),xpd=NA)
text(x=br,-(mymax/3),labels=label,srt=90,xpd=NA)

############
value <- c(2001,29,2,33,21,16,906,2,11)
mymax<-max(value)
br<-barplot(value,ylim=c(0,max(value)),ylab='cluster8',col=color)
text(x=br,value+(mymax/20),labels=paste(value),xpd=NA)
text(x=br,-(mymax/3),labels=label,srt=90,xpd=NA)

############
value <- c(3724,643,6735,4256,992,2882,1967,1382,1969)
mymax<-max(value)
br<-barplot(value,ylim=c(0,max(value)),ylab='cluster9',col=color)
text(x=br,value+(mymax/10),labels=paste(value),xpd=NA)
text(x=br,-(mymax/3),labels=label,srt=90,xpd=NA)

############ 
}

printPlot2<- function(){
color <- c('red','blue','yellow','violet','orange','green','pink','cyan','purple')
par(mfcol=c(3,3))
label <- c('Lettere','Economia','Fisica','Ingegneria','Matematica','Medicina','Arte','Lingue','Biologia')
################
value <- c(1876,3,16,53,12,5,861,8,15)
mymax<-max(value)
br<-barplot(value,ylim=c(0,max(value)),ylab='cluster1',col=color)
text(x=br,value+(mymax/20),labels=paste(value),xpd=NA)
text(x=br,-(mymax/3),labels=label,srt=90,xpd=NA)

############
value <- c(155,146,633,2815,2358,59,89,16,289)
mymax<-max(value)
br<-barplot(value,ylim=c(0,max(value)),ylab='cluster2',col=color)
text(x=br,value+(mymax/20),labels=paste(value),xpd=NA)
text(x=br,-(mymax/3),labels=label,srt=90,xpd=NA)

############
value <- c(355,276,3,1955,44,1643,186,0,140)
mymax<-max(value)
br<-barplot(value,ylim=c(0,max(value)),ylab='cluster3',col=color)
text(x=br,value+(mymax/20),labels=paste(value),xpd=NA)
text(x=br,-(mymax/3),labels=label,srt=90,xpd=NA)

############
value <- c(23,49,250,577,4669,8,11,239,19)
mymax<-max(value)
br<-barplot(value,ylim=c(0,max(value)),ylab='cluster4',col=color)
text(x=br,value+(mymax/20),labels=paste(value),xpd=NA)
text(x=br,-(mymax/3),labels=label,srt=90,xpd=NA)

############
value <- c(39,20,3806,544,41,1198,43,53,874)
mymax<-max(value)
br<-barplot(value,ylim=c(0,max(value)),ylab='cluster5',col=color)
text(x=br,value+(mymax/20),labels=paste(value),xpd=NA)
text(x=br,-(mymax/3),labels=label,srt=90,xpd=NA)

############
value <- c(3461,225,31,392,89,17,1737,2,207)
mymax<-max(value)
br<-barplot(value,ylim=c(0,max(value)),ylab='cluster6',col=color)
text(x=br,value+(mymax/20),labels=paste(value),xpd=NA)
text(x=br,-(mymax/3),labels=label,srt=90,xpd=NA)

############
value <- c(734,10,3,41,22,9,352,1074,10)
mymax<-max(value)
br<-barplot(value,ylim=c(0,max(value)),ylab='cluster7',col=color)
text(x=br,value+(mymax/20),labels=paste(value),xpd=NA)
text(x=br,-(mymax/3),labels=label,srt=90,xpd=NA)

############
value <- c(22,2,645,1070,285,1,8,0,14)
mymax<-max(value)
br<-barplot(value,ylim=c(0,max(value)),ylab='cluster8',col=color)
text(x=br,value+(mymax/20),labels=paste(value),xpd=NA)
text(x=br,-(mymax/3),labels=label,srt=90,xpd=NA)

############
value <- c(87,34,2092,2294,536,33,47,5,548)
mymax<-max(value)
br<-barplot(value,ylim=c(0,max(value)),ylab='cluster9',col=color)
text(x=br,value+(mymax/14),labels=paste(value),xpd=NA)
text(x=br,-(mymax/3),labels=label,srt=90,xpd=NA)

############ 
}

printPlot3<- function(){
color <- c('red','blue','yellow','violet','orange','green','pink','cyan','purple')
par(mfcol=c(3,3))
label <- c('Lettere','Economia','Fisica','Ingegneria','Matematica','Medicina','Arte','Lingue','Biologia')
################
value <- c(1,0,82,28,8,0,0,0,80)
mymax<-max(value)
br<-barplot(value,ylim=c(0,max(value)),ylab='cluster1',col=color)
text(x=br,value+(mymax/20),labels=paste(value),xpd=NA)
text(x=br,-(mymax/3),labels=label,srt=90,xpd=NA)

############
value <- c(0,0,0,10,0,0,0,0,0)
mymax<-max(value)
br<-barplot(value,ylim=c(0,max(value)),ylab='cluster2',col=color)
text(x=br,value+(mymax/20),labels=paste(value),xpd=NA)
text(x=br,-(mymax/3),labels=label,srt=90,xpd=NA)

############
value <- c(1,0,1,84,484,1,1,0,2)
mymax<-max(value)
br<-barplot(value,ylim=c(0,max(value)),ylab='cluster3',col=color)
text(x=br,value+(mymax/20),labels=paste(value),xpd=NA)
text(x=br,-(mymax/3),labels=label,srt=90,xpd=NA)

############
value <- c(0,1,0,0,0,0,0,0,0)
mymax<-max(value)
br<-barplot(value,ylim=c(0,max(value)),ylab='cluster4',col=color)
text(x=br,value+(mymax/20),labels=paste(value),xpd=NA)
text(x=br,-(mymax/3),labels=label,srt=90,xpd=NA)

############
value <- c(0,0,41,17,38,1,0,3,1)
mymax<-max(value)
br<-barplot(value,ylim=c(0,max(value)),ylab='cluster5',col=color)
text(x=br,value+(mymax/20),labels=paste(value),xpd=NA)
text(x=br,-(mymax/3),labels=label,srt=90,xpd=NA)

############
value <- c(1,0,0,6,312,0,0,0,0)
mymax<-max(value)
br<-barplot(value,ylim=c(0,max(value)),ylab='cluster6',col=color)
text(x=br,value+(mymax/20),labels=paste(value),xpd=NA)
text(x=br,-(mymax/3),labels=label,srt=90,xpd=NA)

############
value <- c(9,0,0,500,0,0,2,1,0)
mymax<-max(value)
br<-barplot(value,ylim=c(0,max(value)),ylab='cluster7',col=color)
text(x=br,value+(mymax/20),labels=paste(value),xpd=NA)
text(x=br,-(mymax/3),labels=label,srt=90,xpd=NA)

############
value <- c(6740,763,7349,9084,7213,2962,3331,1393,2032)
mymax<-max(value)
br<-barplot(value,ylim=c(0,max(value)),ylab='cluster8',col=color)
text(x=br,value+(mymax/20),labels=paste(value),xpd=NA)
text(x=br,-(mymax/3),labels=label,srt=90,xpd=NA)

############
value <- c(0,1,6,12,1,9,0,0,1)
mymax<-max(value)
br<-barplot(value,ylim=c(0,max(value)),ylab='cluster9',col=color)
text(x=br,value+(mymax/20),labels=paste(value),xpd=NA)
text(x=br,-(mymax/3),labels=label,srt=90,xpd=NA)

############ 
}

printPlot4<- function(){
color <- c('red','blue','yellow','violet','orange','green','pink','cyan','purple')
par(mfcol=c(3,3))
label <- c('Lettere','Economia','Fisica','Ingegneria','Matematica','Medicina','Arte','Lingue','Biologia')
################
value <- c(259,29,16,164,9,2633,161,1086,41)
mymax<-max(value)
br<-barplot(value,ylim=c(0,max(value)),ylab='cluster1',col=color)
text(x=br,value+(mymax/20),labels=paste(value),xpd=NA)
text(x=br,-(mymax/3),labels=label,srt=90,xpd=NA)

############
value <- c(58,11,670,2299,1335,6,26,88,44)
mymax<-max(value)
br<-barplot(value,ylim=c(0,max(value)),ylab='cluster2',col=color)
text(x=br,value+(mymax/20),labels=paste(value),xpd=NA)
text(x=br,-(mymax/3),labels=label,srt=90,xpd=NA)

############
value <- c(717,61,2259,443,49,236,317,0,1298)
mymax<-max(value)
br<-barplot(value,ylim=c(0,max(value)),ylab='cluster3',col=color)
text(x=br,value+(mymax/20),labels=paste(value),xpd=NA)
text(x=br,-(mymax/3),labels=label,srt=90,xpd=NA)

############
value <- c(404,183,3,2472,25,65,176,1,47)
mymax<-max(value)
br<-barplot(value,ylim=c(0,max(value)),ylab='cluster4',col=color)
text(x=br,value+(mymax/20),labels=paste(value),xpd=NA)
text(x=br,-(mymax/3),labels=label,srt=90,xpd=NA)

############
value <- c(3750,199,7,331,84,29,1803,137,96)
mymax<-max(value)
br<-barplot(value,ylim=c(0,max(value)),ylab='cluster5',col=color)
text(x=br,value+(mymax/20),labels=paste(value),xpd=NA)
text(x=br,-(mymax/3),labels=label,srt=90,xpd=NA)

############
value <- c(38,43,91,426,3940,0,20,51,28)
mymax<-max(value)
br<-barplot(value,ylim=c(0,max(value)),ylab='cluster6',col=color)
text(x=br,value+(mymax/20),labels=paste(value),xpd=NA)
text(x=br,-(mymax/3),labels=label,srt=90,xpd=NA)

############
value <- c(285,101,104,2477,2282,0,154,30,64)
mymax<-max(value)
br<-barplot(value,ylim=c(0,max(value)),ylab='cluster7',col=color)
text(x=br,value+(mymax/20),labels=paste(value),xpd=NA)
text(x=br,-(mymax/3),labels=label,srt=90,xpd=NA)

############
value <- c(1225,134,813,577,90,2,664,1,470)
mymax<-max(value)
br<-barplot(value,ylim=c(0,max(value)),ylab='cluster8',col=color)
text(x=br,value+(mymax/20),labels=paste(value),xpd=NA)
text(x=br,-(mymax/3),labels=label,srt=90,xpd=NA)

############
value <- c(16,4,3516,552,242,2,13,3,28)
mymax<-max(value)
br<-barplot(value,ylim=c(0,max(value)),ylab='cluster9',col=color)
text(x=br,value+(mymax/20),labels=paste(value),xpd=NA)
text(x=br,-(mymax/3),labels=label,srt=90,xpd=NA)

############ 
}

###### new

printPlot5<- function(){
color <- c('red','blue','yellow','violet','orange','green','pink','cyan','purple')
par(mfcol=c(3,3))
label <- c('Lettere','Economia','Fisica','Ingegneria','Matematica','Medicina','Arte','Lingue','Biologia')
################
value <- c(4142,256,637,3609,3131,253,2088,1390,1465)
mymax<-max(value)
br<-barplot(value,ylim=c(0,max(value)),ylab='cluster1',col=color)
text(x=br,value+(mymax/20),labels=paste(value),xpd=NA)
text(x=br,-(mymax/3),labels=label,srt=90,xpd=NA)

############
value <- c(26,2,85,698,382,0,4,0,28)
mymax<-max(value)
br<-barplot(value,ylim=c(0,max(value)),ylab='cluster2',col=color)
text(x=br,value+(mymax/20),labels=paste(value),xpd=NA)
text(x=br,-(mymax/3),labels=label,srt=90,xpd=NA)

############
value <- c(285,91,189,1725,4357,4,145,3,136)
mymax<-max(value)
br<-barplot(value,ylim=c(0,max(value)),ylab='cluster3',col=color)
text(x=br,value+(mymax/20),labels=paste(value),xpd=NA)
text(x=br,-(mymax/3),labels=label,srt=90,xpd=NA)

############
value <- c(31,14,6538,1039,146,3,23,3,346)
mymax<-max(value)
br<-barplot(value,ylim=c(0,max(value)),ylab='cluster4',col=color)
text(x=br,value+(mymax/20),labels=paste(value),xpd=NA)
text(x=br,-(mymax/3),labels=label,srt=90,xpd=NA)

############
value <- c(3,0,0,606,10,0,3,0,1)
mymax<-max(value)
br<-barplot(value,ylim=c(0,max(value)),ylab='cluster5',col=color)
text(x=br,value+(mymax/20),labels=paste(value),xpd=NA)
text(x=br,-(mymax/3),labels=label,srt=90,xpd=NA)

############
value <- c(1819,331,2,115,15,3,877,1,61)
mymax<-max(value)
br<-barplot(value,ylim=c(0,max(value)),ylab='cluster6',col=color)
text(x=br,value+(mymax/20),labels=paste(value),xpd=NA)
text(x=br,-(mymax/3),labels=label,srt=90,xpd=NA)

############
value <- c(125,71,28,1949,15,2477,69,0,79)
mymax<-max(value)
br<-barplot(value,ylim=c(0,max(value)),ylab='cluster7',col=color)
text(x=br,value+(mymax/20),labels=paste(value),xpd=NA)
text(x=br,-(mymax/3),labels=label,srt=90,xpd=NA)

############
value <- c(0,0,0,0,0,233,0,0,0)
mymax<-max(value)
br<-barplot(value,ylim=c(0,max(value)),ylab='cluster8',col=color)
text(x=br,value+(mymax/20),labels=paste(value),xpd=NA)
text(x=br,-(mymax/3),labels=label,srt=90,xpd=NA)

############
value <- c(321,0,0,0,0,0,125,0,0)
mymax<-max(value)
br<-barplot(value,ylim=c(0,max(value)),ylab='cluster9',col=color)
text(x=br,value+(mymax/10),labels=paste(value),xpd=NA)
text(x=br,-(mymax/3),labels=label,srt=90,xpd=NA)

############ 
}

printPlot6<- function(){
color <- c('red','blue','yellow','violet','orange','green','pink','cyan','purple')
par(mfcol=c(3,3))
label <- c('Lettere','Economia','Fisica','Ingegneria','Matematica','Medicina','Arte','Lingue','Biologia')
################
value <- c(79,19,6878,1323,88,21,38,1,880)
mymax<-max(value)
br<-barplot(value,ylim=c(0,max(value)),ylab='cluster1',col=color)
text(x=br,value+(mymax/20),labels=paste(value),xpd=NA)
text(x=br,-(mymax/3),labels=label,srt=90,xpd=NA)

############
value <- c(2954,46,11,56,10,8,1369,13,18)
mymax<-max(value)
br<-barplot(value,ylim=c(0,max(value)),ylab='cluster2',col=color)
text(x=br,value+(mymax/20),labels=paste(value),xpd=NA)
text(x=br,-(mymax/3),labels=label,srt=90,xpd=NA)

############
value <- c(451,143,421,4928,3314,12,217,8,238)
mymax<-max(value)
br<-barplot(value,ylim=c(0,max(value)),ylab='cluster3',col=color)
text(x=br,value+(mymax/20),labels=paste(value),xpd=NA)
text(x=br,-(mymax/3),labels=label,srt=90,xpd=NA)

############
value <- c(443,0,3,31,1,1,190,2,1)
mymax<-max(value)
br<-barplot(value,ylim=c(0,max(value)),ylab='cluster4',col=color)
text(x=br,value+(mymax/20),labels=paste(value),xpd=NA)
text(x=br,-(mymax/3),labels=label,srt=90,xpd=NA)

############
value <- c(2742,493,71,1357,104,161,1485,1349,904)
mymax<-max(value)
br<-barplot(value,ylim=c(0,max(value)),ylab='cluster5',col=color)
text(x=br,value+(mymax/20),labels=paste(value),xpd=NA)
text(x=br,-(mymax/3),labels=label,srt=90,xpd=NA)

############
value <- c(4,18,13,29,1643,8,0,23,3)
mymax<-max(value)
br<-barplot(value,ylim=c(0,max(value)),ylab='cluster6',col=color)
text(x=br,value+(mymax/20),labels=paste(value),xpd=NA)
text(x=br,-(mymax/3),labels=label,srt=90,xpd=NA)

############
value <- c(6,6,39,276,2893,0,4,0,11)
mymax<-max(value)
br<-barplot(value,ylim=c(0,max(value)),ylab='cluster7',col=color)
text(x=br,value+(mymax/20),labels=paste(value),xpd=NA)
text(x=br,-(mymax/3),labels=label,srt=90,xpd=NA)

############
value <- c(41,39,43,44,2,2752,19,0,59)
mymax<-max(value)
br<-barplot(value,ylim=c(0,max(value)),ylab='cluster8',col=color)
text(x=br,value+(mymax/20),labels=paste(value),xpd=NA)
text(x=br,-(mymax/3),labels=label,srt=90,xpd=NA)

############
value <- c(32,1,0,1697,1,10,12,1,2)
mymax<-max(value)
br<-barplot(value,ylim=c(0,max(value)),ylab='cluster9',col=color)
text(x=br,value+(mymax/14),labels=paste(value),xpd=NA)
text(x=br,-(mymax/3),labels=label,srt=90,xpd=NA)

############ 
}

printPlot7<- function(){
color <- c('red','blue','yellow','violet','orange','green','pink','cyan','purple')
par(mfcol=c(3,3))
label <- c('Lettere','Economia','Fisica','Ingegneria','Matematica','Medicina','Arte','Lingue','Biologia')
################
value <- c(6023,745,245,5284,6819,2950,3016,1394,1511)
mymax<-max(value)
br<-barplot(value,ylim=c(0,max(value)),ylab='cluster1',col=color)
text(x=br,value+(mymax/20),labels=paste(value),xpd=NA)
text(x=br,-(mymax/3),labels=label,srt=90,xpd=NA)

############
value <- c(79,13,7111,2202,1086,8,35,2,453)
mymax<-max(value)
br<-barplot(value,ylim=c(0,max(value)),ylab='cluster2',col=color)
text(x=br,value+(mymax/20),labels=paste(value),xpd=NA)
text(x=br,-(mymax/3),labels=label,srt=90,xpd=NA)

############
value <- c(3,0,0,50,22,0,2,0,1)
mymax<-max(value)
br<-barplot(value,ylim=c(0,max(value)),ylab='cluster3',col=color)
text(x=br,value+(mymax/20),labels=paste(value),xpd=NA)
text(x=br,-(mymax/3),labels=label,srt=90,xpd=NA)

############
value <- c(302,6,117,1619,22,4,146,0,140)
mymax<-max(value)
br<-barplot(value,ylim=c(0,max(value)),ylab='cluster4',col=color)
text(x=br,value+(mymax/20),labels=paste(value),xpd=NA)
text(x=br,-(mymax/3),labels=label,srt=90,xpd=NA)

############
value <- c(10,0,0,55,106,0,5,0,3)
mymax<-max(value)
br<-barplot(value,ylim=c(0,max(value)),ylab='cluster5',col=color)
text(x=br,value+(mymax/20),labels=paste(value),xpd=NA)
text(x=br,-(mymax/3),labels=label,srt=90,xpd=NA)

############
value <- c(1,0,0,0,0,0,1,0,0)
mymax<-max(value)
br<-barplot(value,ylim=c(0,max(value)),ylab='cluster6',col=color)
text(x=br,value+(mymax/20),labels=paste(value),xpd=NA)
text(x=br,-(mymax/3),labels=label,srt=90,xpd=NA)

############
value <- c(324,0,0,1,0,0,126,0,0)
mymax<-max(value)
br<-barplot(value,ylim=c(0,max(value)),ylab='cluster7',col=color)
text(x=br,value+(mymax/20),labels=paste(value),xpd=NA)
text(x=br,-(mymax/3),labels=label,srt=90,xpd=NA)

############
value <- c(1,1,6,79,1,11,1,0,8)
mymax<-max(value)
br<-barplot(value,ylim=c(0,max(value)),ylab='cluster8',col=color)
text(x=br,value+(mymax/20),labels=paste(value),xpd=NA)
text(x=br,-(mymax/3),labels=label,srt=90,xpd=NA)

############
value <- c(9,0,0,451,0,0,2,1,0)
mymax<-max(value)
br<-barplot(value,ylim=c(0,max(value)),ylab='cluster9',col=color)
text(x=br,value+(mymax/20),labels=paste(value),xpd=NA)
text(x=br,-(mymax/3),labels=label,srt=90,xpd=NA)

############ 
}

printPlot8<- function(){
color <- c('red','blue','yellow','violet','orange','green','pink','cyan','purple')
par(mfcol=c(3,3))
label <- c('Lettere','Economia','Fisica','Ingegneria','Matematica','Medicina','Arte','Lingue','Biologia')
################
value <- c(0,0,0,0,8,0,0,0,1)
mymax<-max(value)
br<-barplot(value,ylim=c(0,max(value)),ylab='cluster1',col=color)
text(x=br,value+(mymax/20),labels=paste(value),xpd=NA)
text(x=br,-(mymax/3),labels=label,srt=90,xpd=NA)

############
value <- c(2353,580,16,2134,840,18,1325,12,252)
mymax<-max(value)
br<-barplot(value,ylim=c(0,max(value)),ylab='cluster2',col=color)
text(x=br,value+(mymax/20),labels=paste(value),xpd=NA)
text(x=br,-(mymax/3),labels=label,srt=90,xpd=NA)

############
value <- c(7,4,2905,116,16,77,7,0,44)
mymax<-max(value)
br<-barplot(value,ylim=c(0,max(value)),ylab='cluster3',col=color)
text(x=br,value+(mymax/20),labels=paste(value),xpd=NA)
text(x=br,-(mymax/3),labels=label,srt=90,xpd=NA)

############
value <- c(90,10,3410,2615,1305,3,42,1,100)
mymax<-max(value)
br<-barplot(value,ylim=c(0,max(value)),ylab='cluster4',col=color)
text(x=br,value+(mymax/20),labels=paste(value),xpd=NA)
text(x=br,-(mymax/3),labels=label,srt=90,xpd=NA)

############
value <- c(84,11,1046,691,63,23,53,0,1576)
mymax<-max(value)
br<-barplot(value,ylim=c(0,max(value)),ylab='cluster5',col=color)
text(x=br,value+(mymax/20),labels=paste(value),xpd=NA)
text(x=br,-(mymax/3),labels=label,srt=90,xpd=NA)

############
value <- c(2053,75,2,2309,79,58,936,15,46)
mymax<-max(value)
br<-barplot(value,ylim=c(0,max(value)),ylab='cluster6',col=color)
text(x=br,value+(mymax/20),labels=paste(value),xpd=NA)
text(x=br,-(mymax/3),labels=label,srt=90,xpd=NA)

############
value <- c(89,28,47,313,4649,0,58,1359,29)
mymax<-max(value)
br<-barplot(value,ylim=c(0,max(value)),ylab='cluster7',col=color)
text(x=br,value+(mymax/20),labels=paste(value),xpd=NA)
text(x=br,-(mymax/3),labels=label,srt=90,xpd=NA)

############
value <- c(42,7,43,1475,1086,2,20,0,19)
mymax<-max(value)
br<-barplot(value,ylim=c(0,max(value)),ylab='cluster8',col=color)
text(x=br,value+(mymax/20),labels=paste(value),xpd=NA)
text(x=br,-(mymax/3),labels=label,srt=90,xpd=NA)

############
value <- c(2034,50,10,88,10,2792,893,10,49)
mymax<-max(value)
br<-barplot(value,ylim=c(0,max(value)),ylab='cluster9',col=color)
text(x=br,value+(mymax/20),labels=paste(value),xpd=NA)
text(x=br,-(mymax/3),labels=label,srt=90,xpd=NA)

############ 
}

#200 parole 6 iterazioni
printPlot10<- function(){
color <- c('red','blue','yellow','violet','orange','green','pink','cyan','purple')
par(mfcol=c(3,3))
label <- c('Lettere','Economia','Fisica','Ingegneria','Matematica','Medicina','Arte','Lingue','Biologia')
################
value <- c(2267,61,8,167,68,28,1017,6,56)
mymax<-max(value)
br<-barplot(value,ylim=c(0,max(value)),ylab='cluster1',col=color)
text(x=br,value+(mymax/20),labels=paste(value),xpd=NA)
text(x=br,-(mymax/3),labels=label,srt=90,xpd=NA)

############
value <- c(45,7,1738,426,59,135,49,0,1506)
mymax<-max(value)
br<-barplot(value,ylim=c(0,max(value)),ylab='cluster2',col=color)
text(x=br,value+(mymax/20),labels=paste(value),xpd=NA)
text(x=br,-(mymax/3),labels=label,srt=90,xpd=NA)

############
value <- c(1933,555,9,402,329,3,988,0,142)
mymax<-max(value)
br<-barplot(value,ylim=c(0,max(value)),ylab='cluster3',col=color)
text(x=br,value+(mymax/20),labels=paste(value),xpd=NA)
text(x=br,-(mymax/3),labels=label,srt=90,xpd=NA)

############
value <- c(91,9,3779,1995,1127,11,36,0,176)
mymax<-max(value)
br<-barplot(value,ylim=c(0,max(value)),ylab='cluster4',col=color)
text(x=br,value+(mymax/20),labels=paste(value),xpd=NA)
text(x=br,-(mymax/3),labels=label,srt=90,xpd=NA)

############
value <- c(85,15,1875,3546,1021,13,37,0,85)
mymax<-max(value)
br<-barplot(value,ylim=c(0,max(value)),ylab='cluster5',col=color)
text(x=br,value+(mymax/20),labels=paste(value),xpd=NA)
text(x=br,-(mymax/3),labels=label,srt=90,xpd=NA)

############
value <- c(1824,16,2,73,22,14,943,1,26)
mymax<-max(value)
br<-barplot(value,ylim=c(0,max(value)),ylab='cluster6',col=color)
text(x=br,value+(mymax/20),labels=paste(value),xpd=NA)
text(x=br,-(mymax/3),labels=label,srt=90,xpd=NA)

############
value <- c(303,3,1,100,15,4,170,1390,9)
mymax<-max(value)
br<-barplot(value,ylim=c(0,max(value)),ylab='cluster7',col=color)
text(x=br,value+(mymax/20),labels=paste(value),xpd=NA)
text(x=br,-(mymax/3),labels=label,srt=90,xpd=NA)

############
value <- c(30,45,57,647,5389,5,16,0,54)
mymax<-max(value)
br<-barplot(value,ylim=c(0,max(value)),ylab='cluster8',col=color)
text(x=br,value+(mymax/20),labels=paste(value),xpd=NA)
text(x=br,-(mymax/3),labels=label,srt=90,xpd=NA)

############
value <- c(174,54,10,2385,26,2760,78,0,62)
mymax<-max(value)
br<-barplot(value,ylim=c(0,max(value)),ylab='cluster9',col=color)
text(x=br,value+(mymax/20),labels=paste(value),xpd=NA)
text(x=br,-(mymax/3),labels=label,srt=90,xpd=NA)

############ 
}

#200 parole 60 iterazioni
printPlot11<- function(){
color <- c('red','blue','yellow','violet','orange','green','pink','cyan','purple')
par(mfcol=c(3,3))
label <- c('Lettere','Economia','Fisica','Ingegneria','Matematica','Medicina','Arte','Lingue','Biologia')
################
value <- c(2301,63,8,165,67,28,1034,6,58)
mymax<-max(value)
br<-barplot(value,ylim=c(0,max(value)),ylab='cluster1',col=color)
text(x=br,value+(mymax/20),labels=paste(value),xpd=NA)
text(x=br,-(mymax/3),labels=label,srt=90,xpd=NA)

############
value <- c(45,8,1803,421,59,137,48,0,1496)
mymax<-max(value)
br<-barplot(value,ylim=c(0,max(value)),ylab='cluster2',col=color)
text(x=br,value+(mymax/20),labels=paste(value),xpd=NA)
text(x=br,-(mymax/3),labels=label,srt=90,xpd=NA)

############
value <- c(2126,559,10,429,317,4,1051,0,148)
mymax<-max(value)
br<-barplot(value,ylim=c(0,max(value)),ylab='cluster3',col=color)
text(x=br,value+(mymax/20),labels=paste(value),xpd=NA)
text(x=br,-(mymax/3),labels=label,srt=90,xpd=NA)

############
value <- c(91,10,3802,1959,1122,10,37,0,185)
mymax<-max(value)
br<-barplot(value,ylim=c(0,max(value)),ylab='cluster4',col=color)
text(x=br,value+(mymax/20),labels=paste(value),xpd=NA)
text(x=br,-(mymax/3),labels=label,srt=90,xpd=NA)

############
value <- c(86,16,1787,3596,1023,12,39,0,85)
mymax<-max(value)
br<-barplot(value,ylim=c(0,max(value)),ylab='cluster5',col=color)
text(x=br,value+(mymax/20),labels=paste(value),xpd=NA)
text(x=br,-(mymax/3),labels=label,srt=90,xpd=NA)

############
value <- c(1593,7,1,52,22,13,863,1,19)
mymax<-max(value)
br<-barplot(value,ylim=c(0,max(value)),ylab='cluster6',col=color)
text(x=br,value+(mymax/20),labels=paste(value),xpd=NA)
text(x=br,-(mymax/3),labels=label,srt=90,xpd=NA)

############
value <- c(303,3,1,97,15,4,163,1390,9)
mymax<-max(value)
br<-barplot(value,ylim=c(0,max(value)),ylab='cluster7',col=color)
text(x=br,value+(mymax/20),labels=paste(value),xpd=NA)
text(x=br,-(mymax/3),labels=label,srt=90,xpd=NA)

############
value <- c(33,45,58,650,5405,5,19,0,54)
mymax<-max(value)
br<-barplot(value,ylim=c(0,max(value)),ylab='cluster8',col=color)
text(x=br,value+(mymax/20),labels=paste(value),xpd=NA)
text(x=br,-(mymax/3),labels=label,srt=90,xpd=NA)

############
value <- c(174,54,9,2372,26,2760,80,0,62)
mymax<-max(value)
br<-barplot(value,ylim=c(0,max(value)),ylab='cluster9',col=color)
text(x=br,value+(mymax/20),labels=paste(value),xpd=NA)
text(x=br,-(mymax/3),labels=label,srt=90,xpd=NA)

############ 
}


#no random
printPlot9<- function(){
color <- c('red','blue','yellow','violet','orange','green','pink','cyan','purple')
par(mfcol=c(3,3))
label <- c('Lettere','Economia','Fisica','Ingegneria','Matematica','Medicina','Arte','Lingue','Biologia')
################
value <- c(2270,42,8,81,57,21,1041,2,35)
mymax<-max(value)
br<-barplot(value,ylim=c(0,max(value)),ylab='cluster1',col=color)
text(x=br,value+(mymax/20),labels=paste(value),xpd=NA)
text(x=br,-(mymax/3),labels=label,srt=90,xpd=NA)

############
value <- c(40,4,1883,402,50,140,35,0,1619)
mymax<-max(value)
br<-barplot(value,ylim=c(0,max(value)),ylab='cluster2',col=color)
text(x=br,value+(mymax/20),labels=paste(value),xpd=NA)
text(x=br,-(mymax/3),labels=label,srt=90,xpd=NA)

############
value <- c(2904,621,15,680,370,18,1396,0,208)
mymax<-max(value)
br<-barplot(value,ylim=c(0,max(value)),ylab='cluster3',col=color)
text(x=br,value+(mymax/20),labels=paste(value),xpd=NA)
text(x=br,-(mymax/3),labels=label,srt=90,xpd=NA)

############
value <- c(81,8,3813,1914,1183,3,38,0,113)
mymax<-max(value)
br<-barplot(value,ylim=c(0,max(value)),ylab='cluster4',col=color)
text(x=br,value+(mymax/20),labels=paste(value),xpd=NA)
text(x=br,-(mymax/3),labels=label,srt=90,xpd=NA)

############
value <- c(120,16,1692,3959,1328,6,54,1,66)
mymax<-max(value)
br<-barplot(value,ylim=c(0,max(value)),ylab='cluster5',col=color)
text(x=br,value+(mymax/20),labels=paste(value),xpd=NA)
text(x=br,-(mymax/3),labels=label,srt=90,xpd=NA)

############
value <- c(879,2,0,35,8,1,534,1,5)
mymax<-max(value)
br<-barplot(value,ylim=c(0,max(value)),ylab='cluster6',col=color)
text(x=br,value+(mymax/20),labels=paste(value),xpd=NA)
text(x=br,-(mymax/3),labels=label,srt=90,xpd=NA)

############
value <- c(322,2,9,59,13,1,174,1393,5)
mymax<-max(value)
br<-barplot(value,ylim=c(0,max(value)),ylab='cluster7',col=color)
text(x=br,value+(mymax/20),labels=paste(value),xpd=NA)
text(x=br,-(mymax/3),labels=label,srt=90,xpd=NA)

############
value <- c(14,32,51,338,5032,4,8,0,28)
mymax<-max(value)
br<-barplot(value,ylim=c(0,max(value)),ylab='cluster8',col=color)
text(x=br,value+(mymax/20),labels=paste(value),xpd=NA)
text(x=br,-(mymax/3),labels=label,srt=90,xpd=NA)

############
value <- c(122,38,8,2273,15,2779,54,0,37)
mymax<-max(value)
br<-barplot(value,ylim=c(0,max(value)),ylab='cluster9',col=color)
text(x=br,value+(mymax/20),labels=paste(value),xpd=NA)
text(x=br,-(mymax/3),labels=label,srt=90,xpd=NA)

############ 
}
