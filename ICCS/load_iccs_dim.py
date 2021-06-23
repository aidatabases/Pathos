#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pathos_config import *
from dotenv import load_dotenv
load_dotenv()
import os
current_schema = os.environ.get("schema")

# In[2]:


# # Query from Google Sheet

# ## Dimension google sheet

# In[3]:


for i in range(len(dim_table_list_iccs)):
    sample_var1 = dim_table_list_iccs[i]
    sample_var2 = dim_table_list_iccs[i]
    print("Reference DataFrame: {}".format(sample_var1, sample_var2))
    vars()[sample_var2]= load_gsheet(dim_table_list_iccs[i], dim_table_list_iccs[i], sheet_id_iccs)


# # Pushing (All Dimension + Reference + RB_Final_File_Model_Processed) DATA to postgres

# ## Dimension tables

# In[4]:


# Truncate
try:
    for i in dim_table_list_iccs:
        truncate_table(i, current_schema)
        print(i)
except:
    print('not available:{}'.format(i))


# In[5]:


# Pushing
for i, j in zip(range(len(dim_table_list_iccs)), dim_table_list_iccs):
    sample_var = dim_table_list_iccs[i]
    print(sample_var)
    push_table_pgres(df = vars()[sample_var],df_name = j, schema= current_schema)


# In[8]:


#import os
#os.system('jupyter nbconvert --to python load_iccs_dim.ipynb')


# In[ ]:




