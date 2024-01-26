#-*- coding: utf-8 -*-
import requests
import csv
import time
import datetime
import random
import os
import re

import cb_Sum

from bs4 import BeautifulSoup
from collections import defaultdict
from collections import Counter
from pandas_datareader import data as pder

def pure_num(line):#保留數字
	rule = re.compile(r"[^0-9.-]")
	line = rule.sub('',line)
	return line

def pure_chinese(line):#保留中文
	rule = re.compile(r"[^\u4e00-\u9fa5]")
	line = rule.sub('',line)
	return line

def cbdetail(url):
	res = requests.get(url)
	soup = BeautifulSoup(res.text, 'html.parser')
	cb_data = defaultdict(list)
	for i in soup.find_all('tr'):
		#print (i.find_all('td')[0].text, i.find_all('td')[1].text)
		if '可轉債名稱' in i.find_all('td')[0].text:
			CBname = i.find_all('td')[1].text
			cb_data[url.split('/')[-1]].append(CBname)
		if '擔保銀行' in i.find_all('td')[0].text:
			CBcol = i.find_all('td')[1].text
			cb_data[url.split('/')[-1]].append(CBcol)
		if 'CB 收盤價' in i.find_all('td')[0].text:
			CBclose = i.find_all('td')[1].text
			if '(' in CBclose:
				CBclose = str(i.find_all('td')[1].text).split('(')[0]
			cb_data[url.split('/')[-1]].append(CBclose)
		if '轉換價值' in i.find_all('td')[0].text:
			CBvalue = i.find_all('td')[1].text
			cb_data[url.split('/')[-1]].append(CBvalue)
		if '百元報價' in i.find_all('td')[0].text:
			CBAS = i.find_all('td')[1].text
			cb_data[url.split('/')[-1]].append(CBAS)
		if '股票收盤價' in i.find_all('td')[0].text :
			Sclose = i.find_all('td')[1].text
			if '(' in Sclose:
				Sclose = str(i.find_all('td')[1].text).split('(')[0]#只剩純收盤價，無漲跌幅
			cb_data[url.split('/')[-1]].append(Sclose)
		if '目前轉換價' in i.find_all('td')[0].text:
			CBconvPrice = i.find_all('td')[1].text
			cb_data[url.split('/')[-1]].append(CBconvPrice)
		if '轉換比例' in i.find_all('td')[0].text:
			CBconvPercentage = str(i.find_all('td')[1].text).replace('%', '')
			cb_data[url.split('/')[-1]].append(CBconvPercentage)
		if '發行日' in i.find_all('td')[0].text:
			CBstartDate = i.find_all('td')[1].text
			cb_data[url.split('/')[-1]].append(CBstartDate)
		if '到期日' in i.find_all('td')[0].text:
			CBendDate = i.find_all('td')[1].text
			cb_data[url.split('/')[-1]].append(CBendDate)
		if '到期賣回價格' in i.find_all('td')[0].text:
			CBsellBack = i.find_all('td')[1].text
			cb_data[url.split('/')[-1]].append(CBsellBack)
		if '下次提前賣回價格' in i.find_all('td')[0].text:
			CBnextSellBack = i.find_all('td')[1].text
			cb_data[url.split('/')[-1]].append(CBnextSellBack)
		if '發行總額(百萬)' in i.find_all('td')[0].text:
			CBtotalm = i.find_all('td')[1].text
			cb_data[url.split('/')[-1]].append(CBtotalm)
		#cb_data[url.split('/')[-1]]=[CBname, CBcol, CBclose, CBAS, CBvalue, Sclose, CBconvPrice, CBconvPercentage, CBstartDate, CBendDate, CBsellBack, CBnextSellBack, CBtotalm]
	#print (url.split('/')[-1], cb_data[url.split('/')[-1]])
	time.sleep(random.uniform(5.12,10.24))
	if cb_data[url.split('/')[-1]] == []:
		cb_data[url.split('/')[-1]] = ['查無無資料']
	return cb_data[url.split('/')[-1]]
	
def cbList(today_yyyy_mm_dd):
	#可轉債名單
	cbList_dic = {}
	cbList_dic = cbList_tpex()

	#cb資料
	cb_data_all = []#所有可轉債資料
	print('可轉債紀錄日期 : ', today_yyyy_mm_dd, '。 本日共'+str(len(cbList_dic))+'筆可轉債資料。')
	#待補欄位: 折/溢價、樂透cbas、新可轉債是否有高殖利率(未來)
	cbData_title = ['標的代碼','CB名稱', '擔保銀行/信評等級', 'CB收盤價', '轉換市值(PR)', 'CBAS百元報價', '股票收盤價', '轉換價', '發行總額(百萬)','已轉換(%)', '發行日', '到期日', '到期賣回價', '提前賣回價', '剩餘到期天數']
	print (cbData_title)
	cb_data_all.append(cbData_title)
	cbCnt = 1#進度條
	for cb_i in cbList_dic:
		cbdetail_list = cbdetail('https://thefew.tw/quote/'+cb_i)
		if len(cbdetail_list) >= 3 and '無' not in cbdetail_list[10]:#有資料
			#剩餘到期日
			endd = datetime.datetime.strptime(cbdetail_list[10], '%Y-%m-%d')-datetime.datetime.strptime(today_yyyy_mm_dd, '%Y-%m-%d')
			#統計可轉債分布
			show_cbData = [cb_i[:4]] + cbdetail_list + [endd.days]
			if show_cbData not in cb_data_all:
				cb_data_all.append(show_cbData)
			ProgressBar = str(cbCnt)+'/'+str(len(cbList_dic))
			print(ProgressBar, show_cbData)
		cbCnt = cbCnt + 1
		# if cbCnt >= 5:
		# 	break
		time.sleep(random.uniform(5.12,10.24))
	
	#先存檔(可轉債資料)
	savepath = 'CBData'
	if not os.path.exists(savepath):#HistoryData資料夾沒有存在嗎?
		os.makedirs(savepath)#建立HistoryData資料夾
	with open(savepath+'\\cbInfo_'+today_yyyy_mm_dd+'_v4.csv', 'w', newline='', encoding='utf-8') as csvfile:
		writer  = csv.writer(csvfile)
		for cbInfo_ in cb_data_all:
			writer.writerow(cbInfo_)

	recentCB_list = []#近期上市的可轉債
	expire_list = []#n即將到期+還沒轉+接近轉換價
	closeConvPrice_list = []#接近轉換價的可轉債
	willGoUp = []#CB潛力股 還沒過轉換價 容易拉抬的標的
	countCB = defaultdict(list)#發行2檔以上可轉債
	#['標的代碼','CB名稱', '擔保銀行/信評等級', 'CB收盤價', '轉換市值', 'CBAS百元報價', '股票收盤價', '轉換價', '發行總額(百萬)','已轉換(%)', '發行日', '到期日', '到期賣回價', '提前賣回價', '剩餘到期天數']
	sort_cb_data_all = sorted(cb_data_all, key = lambda ixdate : ixdate[10])
	for cb_recent in sort_cb_data_all:
		#近期上市的可轉債
		try:
			if cb_recent[10] not in '發行日':#不要標題列
				RecentCB_days = (datetime.datetime.strptime(today_yyyy_mm_dd, '%Y-%m-%d')-datetime.datetime.strptime(cb_recent[10], '%Y-%m-%d')).days
				if RecentCB_days <= 30:
					show_cb_info = '◆ 近期發行的可轉債:'+cb_recent[1]+', 標的代碼:'+cb_recent[0]+', 發債日:'+cb_recent[10]+', 發行額(百萬):'+cb_recent[8]
					recentCB_list.append(show_cb_info)
				#快到期還沒轉換的 <120日 && 80<市值<110
				if cb_recent[0] not in '標的代碼' and cb_recent[4] != 'NaN' and cb_recent[9] != 'NaN': #不要標題列
					if float(cb_recent[4]) >= 80 and float(cb_recent[4]) <= 110 and float(cb_recent[9]) < 30.0 and cb_recent[-1] <= 120:
						expire_list.append('⊕ 即將到期(近轉換價):'+cb_recent[1]+', 標的代碼:'+cb_recent[0]+', 剩餘天數:'+str(cb_recent[-1])+', CB收盤/市值 : '+str(cb_recent[3])+'/'+cb_recent[4]+', 已轉換(%):'+cb_recent[9]+', 百元報價:'+cb_recent[5])
				#股價接近可轉換價的可轉債  95<市值< 105 且已轉換<10(%)
				if cb_recent[0] not in '標的代碼' and cb_recent[4] != 'NaN' and cb_recent[9] != 'NaN': #不要標題列
					if float(cb_recent[4]) >= 95 and float(cb_recent[4]) <= 106 and float(cb_recent[9]) < 10.0:
						closeConvPrice_list.append('■ 接近轉換價:'+cb_recent[1]+', 標的代碼:'+cb_recent[0]+', CB收盤價:'+cb_recent[3]+', 股票收盤價(轉換價):'+cb_recent[6]+'('+cb_recent[7]+')'+', 已轉換(%):'+cb_recent[9])
				#cb收盤>市值 且無擔保(有人願意出更高價買可轉債，看好會拉) #問小哥 cb價格>市值就可以買? 還是剛發債都獲這樣?還是有擔保品的不適用?
				if cb_recent[0] not in '標的代碼' and cb_recent[3] !='NaN'  and cb_recent[4] !='NaN': #不要標題列
					if float(cb_recent[3]) > 120 and float(cb_recent[4]) <= 110 and float(cb_recent[9]) <= 30.0: #(沒擔保&市值低&可轉債價格高)更好好
						willGoUp.append('▲ CB潛力股:'+cb_recent[0]+', 可轉債:'+cb_recent[1]+', CB收盤價:'+cb_recent[3]+', 可轉債市值(PR):'+str(cb_recent[4])+', 股票收盤價(轉換價):'+cb_recent[6]+'('+cb_recent[7]+')'+', 已轉換(%):'+cb_recent[9]+', 擔保情況:'+cb_recent[2])
				#同一標的發行CB
				countCB[cb_recent[0]].append(cb_recent[1])
				

		except Exception as e:
			print ('Error Message : ', e)
			print ('Error Data : ', cb_recent)

		
	#統整結果&存檔

	# 後續新增功能
	#補 目前價格 < 賣回價 , 例如:現在買100，到期賣回101
	#補 股息穿越轉換價:股利穩定發放、波棟小、高填息，例如大聯大
	#補 cb價格小於110 && 高配息 = 勝算高。高配息可以在第三季或第四季預估出來，找出明天高配息、本益比被低估的好股票，在可轉債價格<100或附近買進。 
	#補 cb價格小於100， 找能放到到期的標的
	#補 95>cb轉換價值>70，找便宜cb，看分點有沒有在買超
	#股價暴跌，融券不減反增(信用交易才行) 更要小心，例如樂陞



	cb_Sum.CBSummary(sort_cb_data_all, today_yyyy_mm_dd)


def cbList_tpex():
	cbList_dic = {}
	url = 'https://www.tpex.org.tw/web/bond/publish/convertible_bond_search/memo.php?l=zh-tw'
	res = requests.get(url)
	soup = BeautifulSoup(res.text, 'html.parser')
	soup = soup.find('tbody')
	c = 1
	for cb_l in soup.find_all('tr'):
		#print(c, cb_l.find_all('td')[0].text, cb_l.find_all('td')[1].text)
		#c += 1
		cbList_dic[cb_l.find_all('td')[0].text] = cb_l.find_all('td')[1].text
	#print (cbList_dic)
	return cbList_dic

def main():
	today_yyyy_mm_dd = str(datetime.date.today())
	if datetime.datetime.strptime(today_yyyy_mm_dd, '%Y-%m-%d').weekday()==5 or datetime.datetime.strptime(today_yyyy_mm_dd, '%Y-%m-%d').weekday()==6:
		while datetime.datetime.strptime(today_yyyy_mm_dd, '%Y-%m-%d').weekday()!=4:
			today_yyyy_mm_dd = (datetime.datetime.strptime(today_yyyy_mm_dd,'%Y-%m-%d') + datetime.timedelta(days=-1)).strftime('%Y-%m-%d')
	#是否已存在
	if not os.path.exists('CBData\\cbInfo_'+today_yyyy_mm_dd+'_v4.csv'):
		cbList(today_yyyy_mm_dd)
		#print ('yes')
	else:
		print('◎ 可轉債資料已存在，無須重複執行。')

if __name__ == '__main__':
	main()
	#cbList()
	#cbdata()
	#print(cbList_tpex())
	#print (cbdetail('https://thefew.tw/quote/43069'))

	#結合籌碼分點資料 找出主一 主五 主十
