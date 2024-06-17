#!/usr/bin/env python
# coding: utf-8

# In[8]:


from bs4 import BeautifulSoup as bs
import pandas as pd
import requests
from tqdm import tqdm
import time
import concurrent.futures
import pickle


# In[9]:




# ## Otteniamo le marche disponibili con i realtivi link alla pagina dedicata

# In[10]:


page = requests.get('https://www.moto.it/moto-usate').text
html = bs(page)

link = []
label = []

marche = html.findAll('aside', {'class' : 'smart-navigation-aside'})[0].findAll('li')
for elem in marche:
    #for x in elem:
    a = elem.find(href=True)
    link.append('https://www.moto.it' + a['href'])
    label.append(a.text)

marche = dict(zip(label, link))


# ## Per motivi di parallelizzazione, otteniamo tutti i modelli disponibili per ogni marca

# In[11]:


sub_link = []
sub_label = []

for marca in tqdm(link):

    page = requests.get(marca).text
    html = bs(page, features="html.parser")

    modello = html.findAll('aside', {'class' : 'smart-navigation-aside'})[0].findAll('li')
    
    for elem in modello:
        #for x in elem:
        a = elem.find(href=True)
        sub_link.append('https://www.moto.it' + a['href'])
        sub_label.append(a.text)


modelli = dict(zip(sub_label, sub_link))


# In[12]:


sub_link = list(modelli.values())


# ## Per ogni modello, ottengo i link di tutti gli annunci pubblicati




global moto

moto = []

def scrape_links(url):
       
    global moto

    links = []
    page = requests.get(url).text
    html = bs(page)

    annunci = html.findAll('ul', {'class' : 'ad-list list'})[0].findAll('a', href=True)

    for annuncio in annunci:
        moto.append('https://www.moto.it' + annuncio['href'])
        

def scrape_modelli(url):
    
    try:
        scrape_links(url)
    
        html = bs(requests.get(url).text)
    
        next_page = html.findAll('a', {'id' : 'cpContent_ucList_ucPager_aNext'}, href=True)

        while len(next_page)>0:

            scrape_links('https://www.moto.it' + next_page[0]['href'])

            try:
                html = bs(requests.get('https://www.moto.it' + next_page[0]['href']).text, features="html.parser")
                next_page = html.findAll('a', {'id' : 'cpContent_ucList_ucPager_aNext'}, href=True)
                if next_page[0]['href'].endswith('#'):
                    next_page = []
            except:
                next_page = []
                break
    except:
        pass




start_time = time.time()

futures = []

with tqdm(total=len(sub_link)-1) as pbar:
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        for pag in sub_link[1:]:
            futures.append(executor.submit(scrape_modelli, pag))

        for future in concurrent.futures.as_completed(futures):
            result = future.result()
            pbar.update(1)
    
end_time = time.time()
print(f'Total time to run multithreads: {end_time - start_time:2f}s')
        
moto = list(dict.fromkeys(moto))


# In[14]:


import pickle

with open('motoit/marche.pkl', 'wb') as f:
    pickle.dump(marche, f)

with open('motoit/modelli.pkl', 'wb') as f:
    pickle.dump(modelli, f)    
    
with open('motoit/moto.pkl', 'wb') as f:
    pickle.dump(moto, f)




