import pandas as pd
import matplotlib  #pip scipy, matplotlib
from matplotlib import pyplot as plt
from matplotlib import style

style.use("ggplot")

plt.subplot(211) # 2 = 2 graph, 1 horizontal, 1 the first graph)
x1 = [5,8,10]
y1 = [12,16,6]
label1 = "5D Avg Cross Correlation"
plt.plot(x1,y1,'g',label = label1, linewidth =3)  #g is color

x2 = [5,8,10]
y2 = [12,10,6]
label2 = "10D Avg Cross Correlation"
plt.plot(x2,y2,'r',label = label2, linewidth =3)  #r is color
title_str = "HK Avg Single Stocks Cross-Correlation 5-10-30 days "
plt.title(title_str)
plt.ylabel("")
plt.xlabel("Date")
plt.legend()



plt.subplot(212) # 2 = 2 graph, 1 horizontal, 1 the first graph)
x1 = [5,8,10]
y1 = [12,16,6]
label1 = "5D Avg Cross Correlation"
plt.plot(x1,y1,'g',label = label1, linewidth =3)  #g is color


title_str = "HK Avg Single Stocks Cross-Correlation 5-10-30 days "
plt.title(title_str)
plt.ylabel("")
plt.xlabel("Date")
plt.legend()



plt.show()


#plt.hist(ages, bins, histtype = "bar", rwidth = 0.8)