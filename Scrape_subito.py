#!/usr/bin/env python
# coding: utf-8

# In[9]:


from bs4 import BeautifulSoup as bs
import pandas as pd
import requests
from tqdm import tqdm
import time
import concurrent.futures
import pickle


# In[10]:


import pickle

# In[11]:


page = requests.get('https://www.subito.it/annunci-italia/vendita/moto-e-scooter').text
html = bs(page, features="html.parser")
print('---------------')
print(html)
print('---------------')
last_page = html.findAll('div', {'class' : 'pagination_pagination-button-wrapper__czWc4 unselected-page'})[1].text


# In[12]:


lista_marche = []

for index in list(range(205))[1:]:
    if len(str(index))==1:
        url = 'https://www.subito.it/annunci-italia/vendita/moto-e-scooter/?bb=00000' + str(index)
    elif len(str(index))==2:
        url = 'https://www.subito.it/annunci-italia/vendita/moto-e-scooter/?bb=0000' + str(index)
    elif len(str(index))==3:
        url = 'https://www.subito.it/annunci-italia/vendita/moto-e-scooter/?bb=000' + str(index)
    
    lista_marche.append(url)

lista_pagine = []

for elem in tqdm(lista_marche):
    page = requests.get(elem).text
    html = bs(page, features="html.parser")
    try:
        last_page = html.findAll('span', {'class' : 'index-module_sbt-text-atom__ed5J9 index-module_token-button__eMeQT size-normal index-module_weight-semibold__MWtJJ index-module_button-text__VZcja'})[-1].text
        if int(last_page):
            for num in list(range(int(last_page)+1)):
                lista_pagine.append('https://www.subito.it/annunci-italia/vendita/moto-e-scooter/?o=' + str(num) + '&&' + elem.split('?')[1])
    except:
        last_page= str(1)
        lista_pagine.append(elem)
        continue


# In[13]:


lista = pd.DataFrame(lista_pagine)

with open('subito/lista_pagine.pkl', 'wb') as f:
    pickle.dump(lista_pagine, f)

lista


# In[14]:


lista_pagine = list(lista[0].values)


# In[15]:

global lista_moto

lista_moto = []


def get_pages(url):
    
    global lista_moto
    
    try:
        page = requests.get(url, timeout=10).text
        html = bs(page, features="html.parser")
        for elem in html.findAll('div', {'class' : 'items__item item-card item-card--small'}):
            try:
                for cc in elem:
                    lista_moto.append(cc['href'])
            except Exception as e:
                print(e)
                continue
    except Exception as e:
        pass

start_time = time.time()
futures = []

with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
    with tqdm(total=len(lista_pagine)-1) as pbar:
        for page in lista_pagine:
            
            future = executor.submit(get_pages, page)
            future.add_done_callback(lambda p: pbar.update())
            futures.append(future)

        for future in concurrent.futures.as_completed(futures):
            result = future.result()
            pbar.update(1)

    
end_time = time.time()
print(f'Total time to run multithreads: {end_time - start_time:2f}s')


# In[16]:


lista_moto = pd.DataFrame(lista_moto)
#lista_moto = lista_moto.drop_duplicates(subset=None, keep="first", inplace=False).reset_index(drop=True)

with open('subito/lista_moto.pkl', 'wb') as f:
    pickle.dump(lista_moto, f)


# ---------------------------------------------------------------

# In[ ]:




