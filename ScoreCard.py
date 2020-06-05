'''
Created on 2020年6月2日
@author: Joke

Thesis:评分卡制作
Describe:基于电商用户交易数据的评分卡开发，本篇主要为特征构造
'''

import pandas as pd
import numpy as np

TraData=pd.read_csv('F:\workspace\python\TransactionalData.csv',encoding='gb18030')

'''预处理'''
#收货地址处理
address=TraData['收货地址'].str.split(',' ,expand=True)
TraData=TraData.join(address)
TraData.rename(columns={0:'省'}, inplace=True)
TraData.rename(columns={1:'市'}, inplace=True)
TraData.rename(columns={2:'区'}, inplace=True)
TraData.rename(columns={3:'街道'}, inplace=True)
TraData['三级地址']=TraData['省']+TraData['市']+TraData['区']
 
#IP地址处理
ip=TraData['IP地址'].str.split('.' ,expand=True)
TraData=TraData.join(ip)
TraData.rename(columns={0:'IP1'}, inplace=True)
TraData.rename(columns={1:'IP2'}, inplace=True)
TraData.rename(columns={2:'IP3'}, inplace=True)
TraData.rename(columns={3:'IP4'}, inplace=True)
TraData['前IP3']=TraData['IP1']+TraData['IP2']+TraData['IP3']

#使用重采样的方式进行时间序列处理
TraData['下单时间']=pd.to_datetime(TraData['下单时间'])
TraData=TraData.set_index(TraData['下单时间'])

'''关联情况'''
#注册手机关联多个收货手机
rela1=TraData.groupby('注册手机')['收货手机'].nunique()
  
#注册手机关联多个收货人
rela2=TraData.groupby('注册手机')['收货人'].nunique()
  
#注册手机关联多个IP地址
rela3=TraData.groupby('注册手机')['IP地址'].nunique()
  
#收货手机关联多个注册手机
rela4=TraData.groupby('收货手机')['注册手机'].nunique()
  
#收货手机关联多个收货人
rela5=TraData.groupby('收货手机')['收货人'].nunique()
  
#收货手机关联多个IP地址
rela6=TraData.groupby('收货手机')['IP地址'].nunique()
  
#收货手机关联多个收货地址
rela7=TraData.groupby('收货手机')['收货地址'].nunique()
  
#收货手机关联多个一级收货地址
rela8=TraData.groupby('收货手机')['省'].nunique()
  
#注册手机关联多个订单来源
rela9=TraData.groupby('注册手机')['订单来源'].nunique()


'''交易行为情况'''
len_=len(TraData)
#商品一天内购买数量
list_=[]
for i in range(len_):
    #筛选对应的时间
    end_time=TraData.index[i]
    start_time=end_time-pd.Timedelta(days=1)
    TraData_=TraData.loc[str(start_time):str(end_time)]
    #筛选对应的商品
    TraData_=TraData_[TraData_['商品编号']==TraData['商品编号'][i]]
    #计算
    value=TraData_['购买数量'].sum()
    list_.append(value)
TraData['商品一天内购买数量']=list_

#三级收货地址一天内交易相同商品数量过多
list_=[]
for i in range(len_):
    #筛选对应的时间
    end_time=TraData.index[i]
    start_time=end_time-pd.Timedelta(days=1)
    TraData_=TraData.loc[str(start_time):str(end_time)]
    #筛选对应的三级地址
    TraData_=TraData_[TraData_['三级地址']==TraData['三级地址'][i]]
    #筛选对应的商品
    TraData_=TraData_[TraData_['商品编号']==TraData['商品编号'][i]]
    #计算
    value=TraData_['购买数量'].sum()
    list_.append(value)
TraData['三级收货地址一天内交易相同商品数量']=list_

#三级收货地址一天内交易金额过大
list_=[]
for i in range(len_):
    #筛选对应的时间
    end_time=TraData.index[i]
    start_time=end_time-pd.Timedelta(days=1)
    TraData_=TraData.loc[str(start_time):str(end_time)]
    #筛选对应的三级地址
    TraData_=TraData_[TraData_['三级地址']==TraData['三级地址'][i]]
    #计算
    value=TraData_['付款总额'].sum()
    list_.append(value)
TraData['三级收货地址一天内交易金额']=list_

# #同一账号短时间内下单次数过多
# tra5=TraData.groupby(['注册手机'])['订单号']
# tra5=tra5.resample('5min').nunique()

#IP地址短时间内下单次数过多
list_=[]
for i in range(len_):
    #筛选对应的时间
    end_time=TraData.index[i]
    start_time=end_time-pd.Timedelta(5,'m')
    TraData_=TraData.loc[str(start_time):str(end_time)]
    #筛选对应的IP地址
    TraData_=TraData_[TraData_['IP地址']==TraData['IP地址'][i]]
    #计算
    value=TraData_['订单号'].nunique()
    list_.append(value)
TraData['IP地址短时间内下单次数']=list_

#前三段IP地址短时间内使用相同优惠券过多
list_=[]
for i in range(len_):
    #筛选对应的时间
    end_time=TraData.index[i]
    start_time=end_time-pd.Timedelta(5,'m')
    TraData_=TraData.loc[str(start_time):str(end_time)]
    #筛选对应的IP地址
    TraData_=TraData_[TraData_['前IP3']==TraData['前IP3'][i]]
    #筛选对应的使用优惠券情况
    TraData_=TraData_[TraData_['使用优惠券']==TraData['使用优惠券'][i]]
    #计算
    value=TraData_['订单号'].nunique()
    list_.append(value)
TraData['前三段IP地址短时间内使用相同优惠券数量']=list_

#前三段IP地址一天内使用相同优惠券过多
list_=[]
for i in range(len_):
    #筛选对应的时间
    end_time=TraData.index[i]
    start_time=end_time-pd.Timedelta(days=1)
    TraData_=TraData.loc[str(start_time):str(end_time)]
    #筛选对应的IP地址
    TraData_=TraData_[TraData_['前IP3']==TraData['前IP3'][i]]
    #筛选对应的使用优惠券情况
    TraData_=TraData_[TraData_['使用优惠券']==TraData['使用优惠券'][i]]
    #计算
    value=TraData_['订单号'].nunique()
    list_.append(value)
TraData['前三段IP地址一天内使用相同优惠券数量']=list_


# '''汇总'''
TraData=TraData.merge(rela1,on='注册手机',suffixes=("", "_注册手机&"))
TraData=TraData.merge(rela2,on='注册手机',suffixes=("", "_注册手机&"))
TraData=TraData.merge(rela3,on='注册手机',suffixes=("", "_注册手机&"))
TraData=TraData.merge(rela4,on='收货手机',suffixes=("", "_收货手机&"))
TraData=TraData.merge(rela5,on='收货手机',suffixes=("", "_收货手机&"))
TraData=TraData.merge(rela6,on='收货手机',suffixes=("", "_收货手机&"))
TraData=TraData.merge(rela7,on='收货手机',suffixes=("", "_收货手机&"))
TraData=TraData.merge(rela8,on='收货手机',suffixes=("", "_收货手机&"))
TraData=TraData.merge(rela9,on='注册手机',suffixes=("", "_注册手机&"))
TraData.to_csv('./TraData.csv',encoding='gb18030')
