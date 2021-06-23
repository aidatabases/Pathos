#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pathos_config import *


# In[2]:

from dotenv import load_dotenv
load_dotenv()
import os
current_schema = os.environ.get("schema")


# # load data from local system

# In[3]:

cf7 = pd.read_excel("cf7.xlsx",engine='openpyxl')

#cf7 = pd.read_excel('cf7.xlsx')
cf7_verbatim = pd.read_excel("cf7verbatim.xlsx",engine='openpyxl')


# In[4]:


cf8 = pd.read_excel("cf8.xlsx",engine='openpyxl')
cf8_verbatim = pd.read_excel("cf8verbatim.xlsx",engine='openpyxl')


# In[5]:


cf20 = pd.read_excel("ICCS_CF2020.xlsx",engine='openpyxl')


# In[6]:


# #renaming locations
cf7['reg1'].replace(['PEEL', 'YORK'], ['Peel', 'York'], inplace=True)
# cf8_verbatim.jurisdiction.replace(['Region of Peel', 'Toronto'], ['Peel', 'City of Toronto'], inplace=True)


# In[7]:


#delete useless column which creating trouble while concat
del cf7_verbatim['id']


# In[ ]:





# In[8]:


try:
    truncate_table(table_name= 'cf7', schema = current_schema)
except:
    print("not available")


# In[9]:


try:
    truncate_table(table_name= 'cf7_verbatim', schema = current_schema)
except:
    print("not available")


# In[10]:


#engine.execute('TRUNCATE generic.initial_data RESTART IDENTITY;')
cf7.to_sql('cf7', con=engine, index=False, if_exists= 'append', schema=current_schema)
cf7_verbatim.to_sql('cf7_verbatim', con=engine, index=False, if_exists= 'append', schema=current_schema)


# In[11]:


try:
    truncate_table(table_name= 'cf8', schema = current_schema)
except:
    print("not available")


# In[12]:


try:
    truncate_table(table_name= 'cf8_verbatim', schema = current_schema)
except:
    print("not available")


# In[13]:


try:
    truncate_table(table_name= 'cf20', schema = current_schema)
except:
    print("not available")


# In[14]:


#engine.execute('TRUNCATE generic.from_ml_team RESTART IDENTITY;')
cf8.to_sql('cf8', con=engine, index=False, if_exists= 'append', schema=current_schema)
cf8_verbatim.to_sql('cf8_verbatim', con=engine, index=False, if_exists= 'append', schema=current_schema)


# In[15]:


cf20.to_sql('cf20', con=engine, index=False, if_exists= 'append', schema=current_schema)


# In[1]:


#import os
#os.system('jupyter nbconvert --to python load_iccs_client_sheet.ipynb')


# In[ ]:




