# -*- coding: utf-8 -*-
from gevent import monkey; monkey.patch_all()
import os
import json
import gevent
import urllib2
from bs4 import BeautifulSoup
from threading import Thread

def request(url, opener=None, data=None, timeout=3, max_try=5):
    if max_try < 0:
        print('Request exceed max try')
        return None
    try:
        if opener is None:
            opener = urllib2.build_opener()
            opener.addheaders = [ 
                ('User-Agent', 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:63.0) Gecko/20100101 Firefox/63.0')
            ]
        res = opener.open(url, data, timeout=timeout)
        data = res.read()
        print(res.msg)
        return data
    except Exception, e:
        #print('Request Exception: %s' % str(e))
        return request(url, opener=opener, data=data, timeout=timeout, max_try=max_try-1)
    
def getNation(opener, nation_url, data=None):
    try:
        data = request(nation_url, opener=opener, data=data)
        print(data)
    except Exception, e:
        print('GetNation Exception: %s' % str(e))
    return None
    
def getProvince():
    pass
    
def getCity():
    pass
    
def getCounty():
    pass
    
def getProxy(proxy_url):
    try: 
        ip_list = []
        data = request(proxy_url).decode('utf-8')
        html = BeautifulSoup(data, features='html.parser')
        ip_list_soup = html.find(id='ip_list')
        for ip in ip_list_soup.find_all('tr'):
            tds = ip.find_all('td')
            if len(tds) > 0:
                ip_list.append(tds[1].text + ':' + tds[2].text)
        print('ip_list: \n%s\n%d length' % (str(ip_list), len(ip_list)))
        return ip_list
    except Exception, e:
        print('GetProxy Exception: %s' % str(e))
    return None
    
def transferOpener(ip_list):
    openers = []
    headers = {
        'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:63.0) Gecko/20100101 Firefox/63.0',
        'Accept': 'application/json'
    }
    raw_opener = urllib2.build_opener()
    raw_opener.addheaders = [('User-agent', headers['User-Agent']), ('Accept', headers['Accept'])]
    openers.append(raw_opener)
    for ip_proxy in ip_list:
        proxies = {'http': ip_proxy, 'https': ip_proxy}  
        proxy_handler = urllib2.ProxyHandler(proxies)     
        opener = urllib2.build_opener(proxy_handler)
        opener.addheaders = [('User-agent', headers['User-Agent']), ('Accept', headers['Accept'])]
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
        
def getValidProxyOpener(ip_list, valid_url):
    valid_openers = []
    openers = transferOpener(ip_list)
    opener_len = len(openers)
    max_opener_num = 50
    for i in range(opener_len//max_opener_num + 1):
        batch_openers = openers[i*max_opener_num:(i+1)*max_opener_num]
        gs = [gevent.spawn(request, valid_url, opener, None, 1, 3) for opener in batch_openers]
        res = gevent.joinall(gs)
        for j in range(len(gs)):
            d = gs[j].get()
            d = validData(d)
            if d is not None: valid_openers.append(batch_openers[j])
    print('valid openers num: %d' % len(valid_openers))
    return valid_openers

if __name__ == '__main__':
    
    proxy_url = 'http://www.xicidaili.com/nn/'
    base_url = 'https://www.deppon.com/'
    search_q = 'phonerest/pricetime/searchNewPrice'
    nation_q = 'phonerest/citycontrol/queryNations'
    province_q = 'phonerest/citycontrol/queryProviecn/'
    hmt_q = 'phonerest/citycontrol/queryHMT'
    city_q = 'phonerest/citycontrol/queryCity/'
    county_q = '/phonerest/citycontrol/queryCounty/'
    main_lane_code = '100000'
    
    ip_list = getProxy(proxy_url)
    valid_openers = getValidProxyOpener(ip_list, os.path.join(base_url, nation_q))
