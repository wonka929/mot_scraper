#!/usr/bin/env python
# coding: utf-8

# In[48]:


import sqlalchemy as sql
import pandas as pd
import pickle
import re
import concurrent.futures
from tqdm import tqdm
import time
import requests
from bs4 import BeautifulSoup as bs
import warnings
import datetime
warnings.filterwarnings("ignore")


# In[49]:


engine = sql.create_engine('mysql+mysqldb://tgbot:tgbot%40123@51.254.115.58/tgbot?charset=utf8mb4')


# In[50]:


data = pd.read_sql('SELECT * FROM DATA', engine)


# In[51]:


data


# In[52]:


with open('subito/lista_moto.pkl', 'rb') as f:
    nuova_lista_subito = pickle.load(f)
    nuova_lista_subito = list(nuova_lista_subito[0].values)

with open('motoit/moto.pkl', 'rb') as f:
    nuova_lista_motoit = pickle.load(f)

lista_moto_database = list(data['Link'])

import math
lista_moto_database = [str(x) for x in lista_moto_database]


# ## Moto.it

# In[53]:


database_motoit = list(filter(lambda k: 'moto.it' in k, lista_moto_database))


# In[54]:


nuove_motoit = (list(set(nuova_lista_motoit) - set(database_motoit)))
print(len(nuove_motoit))


# In[55]:


to_be_dropped_motoit = (list(set(database_motoit) - set(nuova_lista_motoit)))
print(len(to_be_dropped_motoit))


# ## Subito.it

# In[56]:


database_subito = list(filter(lambda k: 'subito.it' in k, lista_moto_database))


# In[57]:


nuove_subito = (list(set(nuova_lista_subito) - set(database_subito)))
print(len(nuove_subito))


# In[58]:


to_be_dropped_subito = (list(set(database_subito) - set(nuova_lista_subito)))
print(len(to_be_dropped_subito))


# ## Dati su database epurati dei link vecchi di annunci scomparsi

# In[59]:


data = data[~data['Link'].isin(to_be_dropped_motoit)]
data = data[~data['Link'].isin(to_be_dropped_subito)]
data.reset_index(inplace=True, drop=True)
data


# In[60]:


data.to_sql(con=engine, schema="tgbot", name="DATA", if_exists="replace", index=False, chunksize=1000, method='multi')


# ## Scrape delle nuove moto su moto.it

# In[61]:


try:
    del final
except:
    pass

#global final

final = pd.DataFrame()

def scrape_moto(address):
    try:
        global final
        
        mesi = {"gennaio":"january", "febbraio":"february", "marzo":"march", "aprile":"april", "maggio":"may", 
        "giugno":"june", "luglio":"july", "agosto":"august", "settembre":"september", "ottobre":"october", 
        "novembre":"november", "dicembre":"december"}
        
        label = []
        values = []
        html = bs(requests.get(address, timeout=10).text)
        
        x = html.findAll('span', {'class':'info'})
        mese = x[0].text.split('inserito il ')[1].split(' ore')[0]
        for key in mesi:
            if mese.replace(key, mesi[key]) != mese:
                data = mese.replace(key, mesi[key])
                data = datetime.datetime.strptime(data, '%d %B %Y')

        a = html.findAll('div', {'class' : 'panel list-info active'})
        b = a[0].findAll('li')

        for li in b:
            label.append(li.findAll('span', {'class' : 'label'})[0].text)
            values.append(li.findAll('span', {'class' : 'value'})[0].text)


        a = html.findAll('div', {'class' : 'panel list-engine'})
        b = a[0].findAll('li')

        for li in b:
            label.append(li.findAll('span', {'class' : 'label'})[0].text)
            values.append(li.findAll('span', {'class' : 'value'})[0].text)


        for li in html.findAll('aside', {'class' : 'ucrecap'})[0]:
            a = html.findAll('aside', {'class' : 'ucrecap'})[0].findAll('span', {'class' : 'key'})
            b = html.findAll('aside', {'class' : 'ucrecap'})[0].findAll('span', {'class' : 'value'})

            for elem in a:
                label.append(elem.text)
            for elem in b:
                values.append(elem.text)
        
        label.append('link')
        values.append(address)

        label.append('Data')
        values.append(data)

        aa = dict(zip(label, values))
        final = pd.concat([final, pd.DataFrame.from_dict(aa, orient='index').T], axis=0, ignore_index=True, sort=False)
    except Exception as e:
        #print(e)
        pass


start_time = time.time()

futures = []

with tqdm(total=len(nuove_motoit)) as pbar:
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        for pag in nuove_motoit:
            futures.append(executor.submit(scrape_moto, pag))

        for future in concurrent.futures.as_completed(futures):
            result = future.result()
            pbar.update(1)
    
end_time = time.time()
print(f'Total time to run multithreads: {end_time - start_time:2f}s')

for column in final.columns:
    try:
        final[column] = final[column].str.replace('[\n\r]', '').str.rstrip()
    except:
        continue

with open('motoit/nuovi_motoit.pkl', 'wb') as f:
    pickle.dump(final, f)


# # Pulisci risultati di moto.it e caricali su DataBase

# In[62]:


with open('motoit/nuovi_motoit.pkl', 'rb') as f:
    motoit = pickle.load(f)

motoit['Città'] = 'civitavecchia'
for i in range(0, len(motoit.index)):
    try:
        motoit['Città'].iloc[i] = motoit[motoit.columns & ['Emilia Romagna', 'Toscana', 'Lazio', 'Lombardia', 'Piemonte', 'Veneto',
           'Molise', 'Marche', 'Puglia', 'Liguria', 'Abruzzo', 'Campania',
           'Sicilia', 'Umbria', 'Trentino Alto Adige', 'Calabria', "Valle d'Aosta",
           'Basilicata', 'Friuli Venezia Giulia', 'Sardegna',
           'Repubblica di San Marino']].iloc[i].dropna()[0]
    except:
        motoit['Città'].iloc[i] = '0'
        continue

print(motoit)
motoit = motoit[['Marca', 'Modello', 'Cilindrata', 'Prezzo da', 'Città', 'link', 'Chilometri', 'Data']]
motoit['Cilindrata'] = motoit['Cilindrata'].str.replace('cc', '').str.strip().str.replace('.', '').str.replace(r'\,.*', '', regex=True).str.replace('nd', '')
motoit['Chilometri'] = motoit['Chilometri'].str.replace(r'\D', '', regex=True)
motoit['Prezzo da'] = motoit['Prezzo da'].str.replace(r'\D', '', regex=True)
motoit.rename(columns = {'Marca':'Manufacturer', 'Prezzo da' : 'Price', 'Modello' : 'Model', 'Chilometri' : 'KM', 'Cilindrata' : 'CC', 'Città' : 'Location', 'link' : 'Link'}, inplace=True)
motoit.replace('', 0, inplace=True)

engine = sql.create_engine('mysql+mysqldb://tgbot:tgbot%40123@51.254.115.58/tgbot?charset=utf8mb4')

start_time = time.time()
motoit.to_sql(con=engine, schema="tgbot", name="DATA", if_exists="append", index=False, chunksize=1000, method='multi')
end_time = time.time()
print(f'Total time to insert values: {end_time - start_time:2f}s')


# ## Scrape delle nuove moto su Subito.it

# In[63]:


try:
    del final
except:
    pass


final = pd.DataFrame()

def screpa(url):
    global final
    
    mesi = {"gen":"january", "feb":"february", "mar":"march", "apr":"april", "mag":"may", 
    "giu":"june", "lug":"july", "ago":"august", "set":"september", "ott":"october", 
    "nov":"november", "dic":"december", 'Oggi': datetime.datetime.now().strftime('%e %B %Y').strip(), 
    'Ieri' : (datetime.datetime.now() - datetime.timedelta(1)).strftime('%e %B %Y').strip()}
    
    try:
        page = requests.get(url, timeout=10).text
        html = bs(page)

        features = []
        values = []
        
        features.append('Location')
        values.append(html.findAll('span', {'class' : 'index-module_sbt-text-atom__ed5J9 index-module_token-overline__ESoEk index-module_size-small__XFVFl AdInfo_ad-info__location__text__ZBFdn'})[0].text)
        
        mese = html.findAll('span', {'class':'index-module_sbt-text-atom__ed5J9 index-module_token-caption__TaQWv size-normal index-module_weight-book__WdOfA index-module_insertion-date__MU4AZ'})
        mese = mese[0].text
        
        if 'Oggi' not in mese and 'Ieri' not in mese:
            mese = mese.split(' alle')[0] + ' ' + str(datetime.datetime.now().year)
        else:
            mese = mese.split(' alle')[0]
        
        for key in mesi:
            if mese.replace(key, mesi[key]) != mese:
                data = mese.replace(key, mesi[key])
                data = datetime.datetime.strptime(data, '%d %B %Y')

        
        a = html.findAll('ul', {'class':'feature-list_feature-list__RDCLn undefined'})
        b = a[0].findAll('li', {'class' : 'feature-list_feature__8a4rn'})
        
        for el in b:
            features.append(el.findAll('span')[0].text)
            values.append(el.findAll('span')[1].text)
            features.append('Prezzo')
            price = html.findAll('p', {'class' : "index-module_price__N7M2x AdInfo_ad-info__price__tGg9h index-module_large__SUacX"})[0].text.replace('\xa0€', '').replace('.', '')
            values.append(price)
            features.append('Link')
            values.append(url)
        
        features.append('Data')
        values.append(data)

        moto = dict(zip(features, values))
        final = pd.concat([final, pd.DataFrame.from_dict(moto, orient='index').T], axis=0, ignore_index=True, sort=False)
    except Exception as e:
        #print(e)
        pass


        
start_time = time.time()

futures = []
with tqdm(total=len(nuove_subito)-1) as pbar:
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        for pag in nuove_subito:
            futures.append(executor.submit(screpa, pag))

        for future in concurrent.futures.as_completed(futures):
            result = future.result()
            pbar.update(1)


end_time = time.time()
print(f'Total time to run multithreads: {end_time - start_time:2f}s')

final = final.drop_duplicates(subset=None, keep="first", inplace=False).reset_index(drop=True)

with open('subito/nuovi_subito.pkl', 'wb') as f:
    pickle.dump(final, f)


# # Pulisci risultati di Subito.it e caricali su DataBase

# In[64]:


with open('subito/nuovi_subito.pkl', 'rb') as f:
    subito = pickle.load(f)

subito = subito[['Marca', 'Modello', 'Cilindrata', 'Prezzo', 'Link', 'Km', 'Location', 'Data']]
subito['Cilindrata'] = subito['Cilindrata'].str.replace(r'\D', '', regex=True)
subito['Km'] = subito['Km'].str.replace(r'\D', '', regex=True)


for col in subito.columns:
    try:
        subito[col] = subito[col].str.replace('\r\n                    ', '')
    except:
        continue

subito.rename(columns = {'Marca':'Manufacturer', 'Prezzo' : 'Price', 'Modello' : 'Model', 'Km' : 'KM', 'Cilindrata' : 'CC', 'link' : 'Link'}, inplace=True)
subito.Price.replace('',0, inplace=True)


# In[65]:


engine = sql.create_engine('mysql+mysqldb://tgbot:tgbot%40123@51.254.115.58/tgbot?charset=utf8mb4')

start_time = time.time()
subito.to_sql(con=engine, schema="tgbot", name="DATA", if_exists="append", index=False, chunksize=1000, method='multi')
end_time = time.time()
print(f'Total time to insert values: {end_time - start_time:2f}s')


# ## Backup database

# In[66]:


engine = sql.create_engine('mysql+mysqldb://tgbot:tgbot%40123@51.254.115.58/tgbot?charset=utf8mb4')
data = pd.read_sql('SELECT * FROM DATA', engine)

with open('backup_data.pickle', 'wb') as f:
    pickle.dump(data, f)
