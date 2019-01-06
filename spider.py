# -*- coding: utf-8 -*-
#from __future__ import division
from gevent import monkey; monkey.patch_all()
import os
import time
import json
import Queue
import pickle
import gevent
import urllib
import urllib2
import numpy as np
import pandas as pd
from bs4 import BeautifulSoup
from threading import Thread, Lock

MUTEX = Lock()

def request(url, openers=None, data=None, timeout=3, max_try=5):
    if max_try < 0:
        #print('Request exceed max try')
        return None
    try:
        opener = None
        if openers is None:
            headers = {'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:63.0) Gecko/20100101 Firefox/63.0'}
            req = urllib2.Request(url, data=data, headers=headers)
            res = urllib2.urlopen(req, timeout=timeout)
            data = res.read()
            return data
        else:
            opener = np.random.choice(openers)
            headers = {
                'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:63.0) Gecko/20100101 Firefox/63.0',
                'Content-Type': 'application/json'
            }
            req = urllib2.Request(url, data=data, headers=headers)
            res = opener.open(req, timeout=timeout)
            #print(res.msg)
            data = res.read()
            return data
    except urllib2.URLError, e:
        return request(url, openers=openers, data=data, timeout=timeout, max_try=max_try-1)
    except urllib2.HTTPError, e:
        return request(url, openers=openers, data=data, timeout=timeout, max_try=max_try-1)
    except Exception, e:
        #print('Request Exception: %s' % str(e))
        return request(url, openers=openers, data=data, timeout=timeout, max_try=max_try-1)

def geventReqRecurve(openers, urls):
    lists = []
    reget_list = []
    gs = [gevent.spawn(request, url, openers, None, 3, 2) for url in urls]
    res = gevent.joinall(gs)
    for j in range(len(gs)):
        d = gs[j].get()
        if d is None:
            reget_list.append(urls[j])
            continue
        data = json.loads(d)
        if data['status'] == 'fail':
            reget_list.append(urls[j])
        else:
            lists.extend(data['result'])
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
        if d is None:
            reget_list.append(urls[j])
            continue
        data = json.loads(d)
        if data['status'] == 'fail': reget_list.append(urls[j])
        else: lists.extend(data['result'])
    return lists, reget_list
            
def getNation(openers, nation_url, data=None, to_file='cache/nation.pkl'):
    try:
        if os.path.exists(to_file): 
            with open(to_file, 'rb') as fp: 
                data = pickle.load(fp)
                print('Nations length: %d' % len(data))
                return data
        res = request(nation_url, openers=openers, data=data)
        #print(data)
        while res is None or json.loads(res)['status'] != 'success':
            res = request(nation_url, openers=openers, data=data)
        res = json.loads(res)['result']
        #res = [d['nationCode'] for d in res]
        with open(to_file, 'wb') as fp: pickle.dump(res, fp)
        print('Nations length: %d' % len(res))
        return res
    except Exception, e:
        print('GetNation Exception: %s' % str(e))
    return None

def getHMT(openers, hmt_url, data=None, to_file='cache/hmt.pkl'):
    try:
        if os.path.exists(to_file): 
            with open(to_file, 'rb') as fp: 
                data = pickle.load(fp)
                print('HMT length: %d' % len(data))
                return data
        res = request(hmt_url, openers=openers, data=data)
        #print(data)
        while res is None:
            res = request(hmt_url, openers=openers, data=data)
        data = validData(res)
        #data = [d['nationCode'] for d in data]
        with open(to_file, 'wb') as fp: pickle.dump(data, fp)
        print('HMT length: %d' % len(data))
        return data
    except Exception, e:
        print('GetHMT Exception: %s' % str(e))
    return None
    
def getProvince(openers, nations_code, province_url, max_num=50, to_file='cache/province.pkl'):
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
    
def getCity(openers, provinces_code, city_url, max_num=50, to_file='cache/city.pkl'):
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
    
def getCounty(openers, cities_code, county_url, max_num=50, to_file='cache/county.pkl'):
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
            data = request(proxy_url + str(i))
            if data == None: continue
            data = data.decode('utf-8')
            html = BeautifulSoup(data, features='html.parser')
            ip_list_soup = html.find(id='ip_list')
            for ip in ip_list_soup.find_all('tr'):
                tds = ip.find_all('td')
                if len(tds) > 0 and tds[5].text == 'HTTPS':
                    ip_list.append(tds[1].text + ':' + tds[2].text)
            time.sleep(0.5)
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
    # double local requester
    #openers.append(raw_opener)
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
        data = json.loads(data)
        if data['status'] == 'success': return data['result']
        return None
    except:
        return None
        
def getValidProxyOpener(ip_list, valid_url, to_file='cache/valid_proxy_list.pkl'):
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
        gs = [gevent.spawn(request, valid_url, [opener], None, 2, 1) for opener in batch_openers]
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

def getEntireCounty(counties, search_time='2018-12-08 00:00:00', to_file='cache/entire_address.csv'):
    if os.path.exists(to_file):
        county_df = pd.read_csv(to_file, encoding='utf-8')
        print('df_county shape: (%d, %d)' % (county_df.shape[0], county_df.shape[1]))
        return county_df
    entire_counties = ['%s-%s-%s'%(county['provinceName'], county['cityName'], county['countyName']) for county in counties]
    province = ['%s'%(county['provinceName']) for county in counties]
    city = ['%s'%(county['cityName']) for county in counties]
    county_df = pd.DataFrame({'fromAddr': entire_counties})
    county_df.loc[:,'province'] = province
    county_df.loc[:,'city'] = city
    county_df.loc[:,'temp'] = 0
    cols = [col for col in county_df.columns if col not in ['province','city']]
    county_df2 = county_df[cols]
    county_df2.columns = ['toAddr', 'temp']
    county_df = pd.merge(county_df, county_df2, on='temp', how='left')
    del county_df['temp']
    print(county_df.columns)
    #county_df.columns = ['fromAddr','province','city','toAddr']
    county_df.loc[:,'time'] = search_time
    #county_df.drop_duplicates(['fromAddr','toAddr'], inplace=True) # does not work
    print('Origin df_county shape: (%d, %d)' % (county_df.shape[0], county_df.shape[1]))
    county_df = county_df[county_df.fromAddr!=county_df.toAddr]
    print(county_df.columns)
    print('df_county shape: (%d, %d)' % (county_df.shape[0], county_df.shape[1]))
    county_df.to_csv(to_file, index=None, encoding='utf-8')
    return county_df
    
def getEntireCountyByMix(counties1, counties2, search_time='2018-12-08 00:00:00', to_file='cache/entire_address.csv'):
    if os.path.exists(to_file):
        county_df = pd.read_csv(to_file, encoding='utf-8')
        print('df_county shape: (%d, %d)' % (county_df.shape[0], county_df.shape[1]))
        return county_df
    
    entire_counties1 = ['%s-%s-%s'%(county['provinceName'], county['cityName'], county['countyName']) for county in counties1]
    entire_counties2 = ['%s-%s-%s'%(county['provinceName'], county['cityName'], county['countyName']) for county in counties2]
    province1 = ['%s'%(county['provinceName']) for county in counties1]
    province2 = ['%s'%(county['provinceName']) for county in counties2]
    city1 = ['%s'%(county['cityName']) for county in counties1]
    city2 = ['%s'%(county['cityName']) for county in counties2]
    county_df1 = pd.DataFrame({'fromAddr': entire_counties1})
    county_df2 = pd.DataFrame({'toAddr': entire_counties2})
    county_df1.loc[:,'province'] = province1
    county_df2.loc[:,'province'] = province2
    county_df1.loc[:,'city'] = city1
    county_df2.loc[:,'city'] = city2
    county_df1.loc[:,'temp'] = 0
    county_df2.loc[:,'temp'] = 0
    
    cols1 = [col for col in county_df1.columns if col not in ['province','city']]
    cols2 = [col for col in county_df2.columns if col not in ['province','city']]
    
    county_df_new1 = pd.merge(county_df1[cols1], county_df2, on='temp', how='left')
    county_df_new2 = pd.merge(county_df1, county_df2[cols2], on='temp', how='left')
    del county_df_new1['temp'], county_df_new2['temp']
    
    print(county_df_new1.columns, county_df_new2.columns)
    county_df_new2.rename(index=str, columns={'fromAddr': 'toAddr', 'toAddr': 'fromAddr'})
    
    county_df = pd.concat([county_df_new1, county_df_new2], axis=0)
    county_df.loc[:,'time'] = search_time
    
    print('Origin df_county shape: (%d, %d)' % (county_df.shape[0], county_df.shape[1]))
    county_df = county_df[county_df.fromAddr!=county_df.toAddr]
    print('df_county shape: (%d, %d)' % (county_df.shape[0], county_df.shape[1]))
    print(county_df.columns)
    county_df.to_csv(to_file, index=None, encoding='utf-8')
    return county_df
    
def geventReqByData(queue, task_queue, openers, url, datas=None):
    ds = [{
        'departureCity': d[0].encode('utf8'), 
        'destinationCity': d[1].encode('utf8'), 
        'sendTime': d[2].encode('utf8')
    } for d in datas]
    
    s_t = time.time()
    MUTEX.acquire()
    gs = [gevent.spawn(request, url, openers, json.dumps(data), 2, 1) for data in ds]
    res = gevent.joinall(gs, timeout=2)
    MUTEX.release()
    
    print('length of request: %d, gevent cost: %.2fs' % (len(gs), time.time() - s_t))
    for j in range(len(gs)):
        d = gs[j].get()
        fdata = validData(d)
        if fdata is None:
            task_queue.put(datas[j])
        else:
            #print(datas[j])
            #fdata['check_route'] = '%s - %s' % (datas[j][0], datas[j][1])
            fdata['fromAddr'] = datas[j][0]
            fdata['toAddr'] = datas[j][1]
            queue.put(fdata)
    
def getNewPrice(openers, county_df, search_q, max_num=50, thread_num=8, file_path='data/'):
    #record_file = 'cache/mark.csv'
    #if os.path.exists(record_file):
    #    mark = pd.read_csv(record_file)
    #else:
    #    mark = county_df[['done']]
    #    mark.to_csv(record_file, index=None)
    #    #county_df.loc[:,'done'] = mark.done
    try:
        queue = Queue.Queue()
        task_queue = Queue.Queue()
        clip_size = max_num / thread_num
        for gpn, gp in county_df.groupby('province'):
            prov_dir = os.path.join(file_path,gpn)
            if not os.path.exists(prov_dir): os.mkdir(prov_dir)
            for gcn, gc in gp.groupby('city'):
                s_t = time.time()
                result = []
                to_file = os.path.join(prov_dir, '%s.pkl'%(gcn))
                if os.path.exists(to_file): continue
                print('[-] Crawling %s-%s with length %d...' % (gpn, gcn, len(gc)))
                data = gc[['fromAddr','toAddr','time']].values
                for i in range(len(data)): task_queue.put(data[i])
                num = 0
                while not task_queue.empty():
                    s_t2 = time.time()
                    threads = []
                    total_num = 0
                    for tid in range(thread_num):
                        d_batch = []
                        for _ in range(max_num): 
                            if not task_queue.empty(): d_batch.append(task_queue.get())
                        total_num += len(d_batch)
                        if len(d_batch) == 0:
                            break
                        elif len(d_batch) < 5:
                            thread = Thread(target=geventReqByData, args=(queue, task_queue, [openers[0]], search_q, d_batch, ))
                            threads.append(thread)
                            break
                        else:
                            thread = Thread(target=geventReqByData, args=(queue, task_queue, openers, search_q, d_batch, ))
                            threads.append(thread)
                    #print('q size %d'%task_queue.qsize())
                    for thread in threads:
                        thread.setDaemon(True)
                        thread.start()
                    for thread in threads:
                        thread.join()
                    #print('q size %d'%task_queue.qsize())
                    count = 0
                    while not queue.empty():
                        result.append(queue.get())
                        count += 1
                    num += count
                    dur2 = time.time() - s_t2
                    #total_num = float(thread_num * max_num)
                    total_num = float(total_num)
                    print('[%6d/%6d] time: %.4fs, process rate: %.2f/s, true rate: %.2f/s, success rate: %.3f' %\
                          (num, len(gc), dur2, total_num / dur2, count / dur2, count / total_num))
                dur = time.time() - s_t
                print('[*] %s-%s with length %d cost %.2fs, process rate: %.3f/s' % (gpn, gcn, len(gc), dur, len(gc) / dur))
                with open(to_file, 'wb') as fp: pickle.dump(result, fp)
            print('[**] Province %s Done!' % (gpn))
    #except KeyboardInterrupt:
    #    save = raw_input('Save %s and %s? [y/n]' % (record_file, to_file))
    #    if save == 'y':
    #        mark.to_csv(record_file, index=None)
    except Exception, e:
        print('getNewPrice Exception: %s' % str(e))
        #mark.to_csv(record_file, index=None)

if __name__ == '__main__':

    #nation_path = 'nations.pkl'
    #province_path = 'provinces.pkl'
    
    proxy_url = 'http://www.xicidaili.com/wn/'
    base_url = 'https://www.deppon.com/'
    search_q = 'phonerest/pricetime/searchNewPrice'
    nation_q = 'phonerest/citycontrol/queryNations'
    province_q = 'phonerest/citycontrol/queryProviecn/'
    hmt_q = 'phonerest/citycontrol/queryHMT'
    city_q = 'phonerest/citycontrol/queryCity/'
    county_q = 'phonerest/citycontrol/queryCounty/'
    main_lane_code = '100000'
    
    s_t = time.time()
    ip_list = getProxy(proxy_url, num=1)
    valid_openers = getValidProxyOpener(ip_list, os.path.join(base_url, nation_q))
    
    # exclude the national express route
    #nations = getNation([valid_openers[0]], os.path.join(base_url, nation_q))
    #nations_code = [nation['nationCode'] for nation in nations]
    nations_code = []
    nations_code.append(main_lane_code)
    
    provinces = getProvince(valid_openers, nations_code, os.path.join(base_url, province_q), max_num=50)
    provinces_code = [province['provinceCode'] for province in provinces]
    hmt = getHMT([valid_openers[0]], os.path.join(base_url, hmt_q))
    
    ig_code = [province['provinceCode'] for province in hmt]
    print('Raw Provinces_code length: %d' % len(provinces_code))
    provinces_code = [code for code in provinces_code if code not in ig_code]
    print('Reduced HMT Provinces_code length: %d' % len(provinces_code))
    
    # ------ MainLand -------
    mainland_cities = getCity(valid_openers, provinces_code, os.path.join(base_url, city_q), max_num=50, to_file='cache/mainland_city.pkl')
    mainland_cities_code = [city['cityCode'] for city in mainland_cities]
    mainland_counties = getCounty(valid_openers, mainland_cities_code, os.path.join(base_url, county_q), max_num=100, 
                                  to_file='cache/mainland_county.pkl')
    
    # ------    HMT   -------
    # HMT provinces, HongKong and Macow, TaiWan - 710000
    print(hmt)
    HMT_provinces_code = [province['provinceCode'] for province in hmt if province['provinceCode'] != u'710000']
    print(HMT_provinces_code)
    
    HMT_cities = getCity(valid_openers, HMT_provinces_code, os.path.join(base_url, city_q), max_num=50, to_file='cache/HMT_city.pkl')
    HMT_cities_code = [city['cityCode'] for city in HMT_cities]
    HMT_counties = getCounty(valid_openers, HMT_cities_code, os.path.join(base_url, county_q), max_num=100, to_file='cache/HMT_county.pkl')
    
    # ------ Merge Mix Counties ------
    all_counties = getEntireCountyByMix(mainland_counties, HMT_counties, '2018-12-08 00:00:00')
    
    #counties_name = [county['countyName'] for county in counties]
    #print(','.join(counties_name))
    #county_df = getEntireCounty(counties, '2018-12-08 00:00:00')
    print('Ready data cost: %.4fs' % (time.time() - s_t))
    getNewPrice(valid_openers, all_counties, os.path.join(base_url, search_q), max_num=50, thread_num=4)
    
