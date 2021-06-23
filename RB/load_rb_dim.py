#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pathos_config import *


# # Query from Google Sheet

# ## Dimension google sheet

# In[2]:


for i in range(len(dim_table_list_rb)):
    sample_var1 = dim_table_list_rb[i]
    sample_var2 = dim_table_list_rb[i]
    print("Reference DataFrame: {}".format(sample_var1, sample_var2))
    vars()[sample_var2]= load_gsheet(dim_table_list_rb[i], dim_table_list_rb[i], sheet_id_rb)


# ## Dimension tables

# In[6]:


# # Truncate
# try:
#     for i in dim_table_list_rb:
#         print(i)
#         truncate_table(i, 'pathos_rb_schema')
# except:
#     print('not available')


# In[8]:


# Push
for i, j in zip(range(len(dim_table_list_rb)), dim_table_list_rb):
    sample_var = dim_table_list_rb[i]
    print(sample_var)
    push_table_pgres(df = vars()[sample_var],df_name = j, schema= 'pathos_rb_schema')


# In[1]:


import os
os.system('jupyter nbconvert --to python load_rb_dim.ipynb')


# In[ ]:




