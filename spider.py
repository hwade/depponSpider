# -*- coding: utf-8 -*-
from gevent import monkey; monkey.patch_all()
import os
import time
import json
import pickle
import gevent
import urllib
import urllib2
import numpy as np
import pandas as pd
from bs4 import BeautifulSoup
from threading import Thread

def request(url, openers=None, data=None, timeout=3, max_try=5):
    if max_try < 0:
        #print('Request exceed max try')
        return None
    try:
        opener = None
        if openers is None:
            opener = urllib2.build_opener()
            opener.addheaders = [ 
                ('User-Agent', 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:63.0) Gecko/20100101 Firefox/63.0')
            ]
        else:
            opener = np.random.choice(openers)
        req = urllib2.Request(url, data=data)
        res = opener.open(req, timeout=timeout)
        #data = res.read()
        return res
    except urllib2.URLError, e:
        return request(url, openers=openers, data=data, timeout=timeout, max_try=max_try-1)
    except Exception, e:
        print('Request Exception: %s' % str(e))
        return request(url, openers=openers, data=data, timeout=timeout, max_try=max_try-1)

def geventReqRecurve(openers, urls):
    lists = []
    reget_list = []
    gs = [gevent.spawn(request, url, openers, None, 3, 2) for url in urls]
    res = gevent.joinall(gs)
    for j in range(len(gs)):
        d = gs[j].get()
        if d != None and d.msg == 'OK':
            data = d.read()
            lists.extend(json.loads(data)['result'])
        else:
            reget_list.append(urls[j])
    if len(reget_list) > 0:
        lists.extend(geventReqRecurve(openers, reget_list))
    return lists
    
def geventReq(openers, urls):
    lists = []
    reget_list = []
    gs = [gevent.spawn(request, url, openers, None, 3, 1) for url in urls]
    res = gevent.joinall(gs)
    for j in range(len(gs)):
        d = gs[j].get()
        if d != None and d.msg == 'OK':
            data = d.read()
            lists.extend(json.loads(data)['result'])
        else:
            reget_list.append(urls[j])
    return lists, reget_list
            
def getNation(openers, nation_url, data=None, to_file='nation.pkl'):
    try:
        if os.path.exists(to_file): 
            with open(to_file, 'rb') as fp: 
                data = pickle.load(fp)
                print('Nations length: %d' % len(data))
                return data
        res = request(nation_url, openers=openers, data=data)
        #print(data)
        while res is None or res.msg != 'OK':
            res = request(nation_url, openers=openers, data=data)
        data = json.loads(res.read())['result']
        #data = [d['nationCode'] for d in data]
        with open(to_file, 'wb') as fp: pickle.dump(data, fp)
        print('Nations length: %d' % len(data))
        return data
    except Exception, e:
        print('GetNation Exception: %s' % str(e))
    return None

def getHMT(openers, hmt_url, data=None, to_file='hmt.pkl'):
    try:
        if os.path.exists(to_file): 
            with open(to_file, 'rb') as fp: 
                data = pickle.load(fp)
                print('HMT length: %d' % len(data))
                return data
        res = request(hmt_url, openers=openers, data=data)
        #print(data)
        while res is None or res.msg != 'OK':
            res = request(hmt_url, openers=openers, data=data)
        data = json.loads(res.read())['result']
        #data = [d['nationCode'] for d in data]
        with open(to_file, 'wb') as fp: pickle.dump(data, fp)
        print('HMT length: %d' % len(data))
        return data
    except Exception, e:
        print('GetHMT Exception: %s' % str(e))
    return None
    
def getProvince(openers, nations_code, province_url, max_num=50, to_file='province.pkl'):
    try:
        if os.path.exists(to_file): 
            with open(to_file, 'rb') as fp: 
                provinces = pickle.load(fp)
                print('Provinces length: %d' % len(provinces))
                return provinces
        
        i = 0
        provinces = []
        urls = [os.path.join(province_url, nation) for nation in nations_code]
        while i * max_num < len(urls):
            url_batch = urls[i*max_num:(i+1)*max_num] 
            provinces.extend(geventReqRecurve(openers, url_batch))
            i = i + 1
        with open(to_file, 'wb') as fp: pickle.dump(provinces, fp)
        print('Provinces length: %d' % len(provinces))
        return provinces
    except Exception, e:
        print('GetProvince Exception: %s' % str(e))
    return None
    
def getCity(openers, provinces_code, city_url, max_num=50, to_file='city.pkl'):
    try:
        if os.path.exists(to_file): 
            with open(to_file, 'rb') as fp: 
                cities = pickle.load(fp)
                print('Cities length: %d' % len(cities))
                return cities
        
        i = 0
        cities = []
        urls = [os.path.join(city_url, province) for province in provinces_code]
        while i * max_num < len(urls):
            url_batch = urls[i*max_num:(i+1)*max_num] 
            cities.extend(geventReqRecurve(openers, url_batch))
            i = i + 1
        with open(to_file, 'wb') as fp: pickle.dump(cities, fp)
        print('Cities length: %d' % len(cities))
        return cities
    except Exception, e:
        print('GetCities Exception: %s' % str(e))
    return None
    
def getCounty(openers, cities_code, county_url, max_num=50, to_file='county.pkl'):
    try:
        if os.path.exists(to_file): 
            with open(to_file, 'rb') as fp: 
                counties = pickle.load(fp)
                print('Counties length: %d' % len(counties))
                return counties
        
        i = 0
        counties = []
        reget_list = []
        urls = [os.path.join(county_url, city) for city in cities_code]
        while i * max_num < len(urls):
            url_batch = urls[i*max_num:(i+1)*max_num] 
            cs, rl = geventReq(openers, url_batch)
            counties.extend(cs)
            reget_list.extend(rl)
            i = i + 1
        print(len(counties))
        print(len(reget_list))
        while len(reget_list) > 0:
            l = len(reget_list)
            url_batch = [reget_list.pop() for _ in range(min(l, max_num))]
            counties.extend(geventReqRecurve(openers, url_batch))
            
        with open(to_file, 'wb') as fp: pickle.dump(counties, fp)
        print('Counties length: %d' % len(counties))
        return counties
    except Exception, e:
        print('GetCounties Exception: %s' % str(e))
    return None
    
def getProxy(proxy_url, num=5):
    try: 
        ip_list = []
        for i in range(1, num+1):
            print('getting proxy page %d' % i)
            data = request(proxy_url + str(i)).read().decode('utf-8')
            html = BeautifulSoup(data, features='html.parser')
            ip_list_soup = html.find(id='ip_list')
            for ip in ip_list_soup.find_all('tr'):
                tds = ip.find_all('td')
                if len(tds) > 0:
                    ip_list.append(tds[1].text + ':' + tds[2].text)
            time.sleep(0.2)
        print('ip_list length: %d' % (len(ip_list)))
        return ip_list
    except Exception, e:
        print('GetProxy Exception: %s' % str(e))
    return None
    
def transferOpener(ip_list):
    openers = []
    headers = {
        'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:63.0) Gecko/20100101 Firefox/63.0',
        'Content-Type': 'application/json'
    }
    raw_opener = urllib2.build_opener()
    raw_opener.addheaders = [('User-agent', headers['User-Agent']), ('Content-Type', headers['Content-Type'])]
    openers.append(raw_opener)
    for ip_proxy in ip_list:
        proxies = {'http': ip_proxy, 'https': ip_proxy}  
        proxy_handler = urllib2.ProxyHandler(proxies)     
        opener = urllib2.build_opener(proxy_handler)
        opener.addheaders = [('User-Agent', headers['User-Agent']), ('Content-Type', headers['Content-Type'])]
        openers.append(opener)
    print('Transfer %d openers.' % len(openers))
    return openers
    
def validData(data):
    try:
        data = json.loads(data.read())
        if data['status'] == 'success': return data['result']
        return None
    except:
        return None
        
def getValidProxyOpener(ip_list, valid_url, to_file='valid_proxy_list.pkl'):
    if os.path.exists(to_file):
        with open(to_file, 'rb') as fp:
            valid_openers = transferOpener(pickle.load(fp))
            print('Loaded %d valid openers.' % len(valid_openers))
            return valid_openers
    valid_openers = []
    valid_proxy = []
    openers = transferOpener(ip_list)
    opener_len = len(openers)
    max_opener_num = 50
    for i in range(opener_len//max_opener_num + 1):
        batch_openers = openers[i*max_opener_num:(i+1)*max_opener_num]
        gs = [gevent.spawn(request, valid_url, [opener], None, 3, 1) for opener in batch_openers]
        res = gevent.joinall(gs)
        for j in range(len(gs)):
            d = gs[j].get()
            d = validData(d)
            if d is not None: 
                valid_openers.append(batch_openers[j])
                valid_proxy.append(ip_list[i*max_opener_num+j])
    with open(to_file, 'wb') as fp: pickle.dump(valid_proxy, fp)
    print('valid openers num: %d' % len(valid_openers))
    return valid_openers

def getEntireCounty(counties, search_time='2018-12-01 00:00:00', to_file='entire_address.csv'):
    if os.path.exists(to_file):
        county_df = pd.read_csv(to_file, encoding='utf-8')
        print('df_county shape: (%d, %d)' % (county_df.shape[0], county_df.shape[1]))
        return county_df
    entire_counties = ['%s-%s-%s'%(county['provinceName'], county['cityName'], county['countyName']) for county in counties]
    county_df = pd.DataFrame({'fromAddr': entire_counties})
    county_df.loc[:,'done'] = 0
    county_df = pd.merge(county_df, county_df, on='done', how='left')
    print(county_df.columns)
    county_df.columns = ['fromAddr','done','toAddr']
    county_df.loc[:,'time'] = search_time
    print(county_df.columns)
    print('df_county shape: (%d, %d)' % (county_df.shape[0], county_df.shape[1]))
    county_df.drop_duplicates(['fromAddr','toAddr'], inplace=True) # does not work
    county_df.to_csv(to_file, index=None ,encoding='utf-8')
    return county_df
    
def geventReqByData(openers, url, datas=None):
    lists = []
    reget_list = []
    print(datas[0])
    gs = [gevent.spawn(request, url, openers, urllib.urlencode(data), 3, 1) for data in datas]
    res = gevent.joinall(gs)
    for j in range(len(gs)):
        d = gs[j].get()
        if d != None and d.msg == 'OK':
            data = d.read()
            print(data)
            lists.extend(json.loads(data)['result'])
        else:
            reget_list.append(datas[j])
    return lists, reget_list
    
def getNewPrice(openers, county_df, search_q, max_num=50, to_file='price.pkl'):
    record_file = 'mark.csv'
    if os.path.exists(record_file):
        mark = pd.read_csv(record_file)
    else:
        mark = county_df[['done']]
        mark.to_csv(record_file, index=None)
        #county_df.loc[:,'done'] = mark.done
    #try:
    while len(mark[mark.done<1]) > 0:
        s_t = time.time()
        head_index = mark[mark.done<1].head(max_num).index
        data = county_df.loc[head_index, ['fromAddr','toAddr','time']].values
        data = [{'departureCity': d[0].encode('gbk'), 'destinationCity': d[1].encode('gbk'), 'sendTime': d[2].encode('gbk')} \
                for d in data]
        res, reget_list = geventReqByData(openers, search_q, data)
        mark.loc[head_index, 'done'] = 1
        print('result: %d vs %d, cost: %.4fs' % (len(res), len(reget_list), time.time()-s_t))
    #except Exception, e:
    #    print('getNewPrice Exception: %s' % str(e))
    #    mark.to_csv(record_file, index=None)

if __name__ == '__main__':

    #nation_path = 'nations.pkl'
    #province_path = 'provinces.pkl'
    
    proxy_url = 'http://www.xicidaili.com/nn/'
    base_url = 'https://www.deppon.com/'
    search_q = 'phonerest/pricetime/searchNewPrice'
    nation_q = 'phonerest/citycontrol/queryNations'
    province_q = 'phonerest/citycontrol/queryProviecn/'
    hmt_q = 'phonerest/citycontrol/queryHMT'
    city_q = 'phonerest/citycontrol/queryCity/'
    county_q = 'phonerest/citycontrol/queryCounty/'
    main_lane_code = '100000'
    
    s_t = time.time()
    ip_list = getProxy(proxy_url)
    valid_openers = getValidProxyOpener(ip_list, os.path.join(base_url, nation_q))
    
    nations = getNation([valid_openers[0]], os.path.join(base_url, nation_q))
    nations_code = [nation['nationCode'] for nation in nations]
    nations_code.append(main_lane_code)
    
    provinces = getProvince(valid_openers, nations_code, os.path.join(base_url, province_q), max_num=50)
    hmt = getHMT([valid_openers[0]], os.path.join(base_url, hmt_q))
    provinces_code = [province['provinceCode'] for province in provinces]
    provinces_code.extend([province['provinceCode'] for province in hmt])
    
    cities = getCity(valid_openers, provinces_code, os.path.join(base_url, city_q), max_num=50)
    cities_code = [city['cityCode'] for city in cities]
    
    counties = getCounty(valid_openers, cities_code, os.path.join(base_url, county_q), max_num=100)
    #counties_name = [county['countyName'] for county in counties]
    #print(','.join(counties_name))
    county_df = getEntireCounty(counties, '2018-12-01 00:00:00')
    print('Ready data cost: %.4fs' % (time.time() - s_t))
    
    getNewPrice(valid_openers, county_df, os.path.join(base_url, search_q), max_num=10)
