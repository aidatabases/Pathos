#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pathos_config import *


# # load data from local system

# In[4]:


#RB_Final_File_Model_Processed = pd.read_excel('RB_Final_File_Model_Processed.xlsx')
initial_data = pd.read_excel('RB_Final_File_Model_Processed.xlsx', sheet_name= "initial_data")
from_ml_team = pd.read_excel('RB_Final_File_Model_Processed.xlsx', sheet_name= "from_ml_team")


# # Pushing (All Dimension + Reference + RB_Final_File_Model_Processed) DATA to postgres

# In[5]:


engine = create_engine('postgresql://aidatabases:Aidatabases#@65.1.96.15:5432/pathos_db')


# ## Generic tables

# In[9]:


engine = create_engine('postgresql://aidatabases:Aidatabases#@65.1.96.15:5432/pathos_db')


# In[10]:


# try:
#     engine.execute('TRUNCATE pathos_rb_schema.rb_initial_data RESTART IDENTITY;')
# except:
#     print('not available')


# In[11]:


initial_data.to_sql('rb_initial_data', con=engine, index=False, if_exists= 'append', schema='pathos_rb_schema')


# In[12]:


# try:
#     engine.execute('TRUNCATE pathos_rb_schema.rb_from_ml_team RESTART IDENTITY;')
# except:
#     print('not available')


# In[13]:


from_ml_team.to_sql('rb_from_ml_team', con=engine, index=False, if_exists= 'append', schema='pathos_rb_schema')


# In[2]:


import os
os.system('jupyter nbconvert --to python load_rb_client_sheet.ipynb')


# In[ ]:




