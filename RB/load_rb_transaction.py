#!/usr/bin/env python
# coding: utf-8

# # Querying Postgres

# In[1]:


from pathos_config import *


# ## Selecting Relevant Dimensions based on Master Dim Table + Filtering based on (Active for reporting)

# ### reference schema

# In[2]:


for i in range(len(ref_table_list)):
    sample_var = ref_table_list[i]
    print(sample_var)
    vars()[sample_var]= load_data(ref_table_list[i], 'pathos_reference_common_schema', connection)


# In[3]:


# Applying Filter
pathos_ref_drivers_sectors_mapping = pathos_ref_drivers_sectors_mapping.loc[pathos_ref_drivers_sectors_mapping['sector_reporting_flag']=='Y']


# ### dimension schema

# In[4]:


for i in range(len(dim_table_list_rb)):
    sample_var = dim_table_list_rb[i]
    print(sample_var)
    vars()[sample_var]= load_data(dim_table_list_rb[i], 'pathos_rb_schema', connection)


# In[5]:


pathos_cl_master_prd_ver


# In[6]:


# Applying Filter
try:
    pathos_cl_master_brand = active_filter(pathos_cl_master_brand)
except:
    print('not available')
try:
    pathos_cl_master_manufacturer = active_filter(pathos_cl_master_manufacturer)
except:
    print('not available')
try:
    pathos_cl_master_prd_ver = active_filter(pathos_cl_master_prd_ver)
except:
    print('not available')

try:
    pathos_cl_master_dim_mapping = active_filter(pathos_cl_master_dim_mapping)
except:
    print('not available')
        
try:
    pathos_cl_master_time = active_filter(pathos_cl_master_time)
except:
    print('not available')
        
try:
    pathos_cl_master_product = active_filter(pathos_cl_master_product)
except:
    print('not available')
        
try:
    pathos_cl_master_gmo = active_filter(pathos_cl_master_gmo)
except:
    print('not available')
        
try:
    pathos_cl_master_source = active_filter(pathos_cl_master_source)
except:
    print('not available')
    
try:
    pathos_cl_master_country = active_filter(pathos_cl_master_country)
except:
    print('not available')
    
try:
    pathos_cl_master_channel = active_filter(pathos_cl_master_channel)
except:
    print('not available')


# In[7]:


pathos_cl_master_prd_ver


# In[8]:


# Initializing version_id and Version
try:
    version_id = pathos_cl_master_prd_ver['dim_val_id'].iloc[0]
    version    = pathos_cl_master_prd_ver['dim_value_name'].iloc[0]
except:
    print('not available')


# In[9]:


version_id


# ### generic schema

# In[10]:


connection = pg.connect('postgresql://aidatabases:Aidatabases#@65.1.96.15:5432/pathos_db')


# In[11]:


initial_data = psql.read_sql('SELECT * FROM pathos_rb_schema.rb_initial_data', connection)

try:
    initial_data['MANUFACTURER'].replace({'rb Health': 'RB Health','Rb Health': 'RB Health', 'rB Health': 'RB Health'}, inplace=True)
except:
    print('not available')


# In[12]:


initial_data


# ## Dimensions based on filters

# In[13]:


try:    
    pathos_cl_master_dim_mapping = pathos_cl_master_dim_mapping.loc[pathos_cl_master_dim_mapping['dim_active_reporting']=='Y']
    k=[]
    l=[]
    for i, j in zip(pathos_cl_master_dim_mapping['dim_sql'], pathos_cl_master_dim_mapping['dimension_desc']):
        k.append(j)
        l.append(i)
        print(k)
        print(l)

    for i in range(len(k)):
        sample_var = k[i]
        #print(sample_var)
        vars()[sample_var] = psql.read_sql(l[i], connection)
        vars()[sample_var] = vars()[sample_var].loc[vars()[sample_var]['dim_active_reporting']=='Y']
        
except:
    print('not available')


# # Transaction Table

# # Adding Relevant Dimension ID'

# In[14]:


try:
    initial_data['year'] = pd.DatetimeIndex(initial_data['Date']).year
    #initial_data['year'] = initial_data.year.apply(str)
except:
    print('not available')


# In[15]:


try:
    initial_data['year'] = initial_data.year.apply(str)
    s = initial_data.year.replace(pathos_cl_master_time.set_index('dim_value_name')['dim_val_id'])
    initial_data['time_id'] = s
except:
    print('not available')


# In[16]:


initial_data


# In[17]:


#REPLACING BRAND NAME WITH BRAND ID
try:
    s = initial_data.BRAND.replace(pathos_cl_master_brand.set_index('dim_value_name')['dim_val_id'])
    initial_data['brand_id'] = s
except:
    print('not available')


# In[18]:


#REPLACING MANUFACTURER NAME WITH MANUFACTURER ID
try:
    s = initial_data.MANUFACTURER.replace(pathos_cl_master_manufacturer.set_index('dim_value_name')['dim_val_id'])
    initial_data['manufacturer_id'] = s
except:
    print('not available')


# In[19]:


#REPLACING PRODUCT NAME WITH PRODUCT ID
try:
    s = initial_data.Product.replace(pathos_cl_master_product.set_index('dim_value_name')['dim_val_id'])
    initial_data['product_id'] = s
except:
    print('not available')


# In[20]:


#REPLACING GMO NAME WITH GMO ID
try:
    initial_data.GMO.replace('with a Non GMO Claim','With a Non GMO Claim', inplace = True )
    s = initial_data.GMO.replace(pathos_cl_master_gmo.set_index('dim_value_name')['dim_val_id'])
    initial_data['gmo_id'] = s
except:
    print('not available')


# In[21]:


#REPLACING SOURCE NAME WITH SOURCE ID
try:
    s = initial_data.Source.replace(pathos_cl_master_source.set_index('dim_value_name')['dim_val_id'])
    initial_data['source_id'] = s
except:
    print('not available')


# In[22]:


#REPLACING CHANNEL NAME WITH CHANNEL ID
try:
    s = initial_data.Channel.replace(pathos_cl_master_channel.set_index('dim_value_name')['dim_val_id'])
    initial_data['channel_id'] = s
except:
    print('not available')


# In[23]:


#REPLACING CHANNEL NAME WITH CHANNEL ID
try:
    s = initial_data.Country.replace(pathos_cl_master_country.set_index('dim_value_name')['dim_val_id'])
    initial_data['country_id'] = s
except:
    print('not available')


# # Applying filters to Main DataFrame

# In[24]:


try:
    available_manufacturer = list(pathos_cl_master_manufacturer['dim_val_id'])
except:
    print('not available')
        
try:
    available_brand = list(pathos_cl_master_brand['dim_val_id'])
except:
    print('not available')
        
try:
    available_product = list(pathos_cl_master_product['dim_val_id'])
except:
    print('not available')
        
try:
    available_gmo = list(pathos_cl_master_gmo['dim_val_id'])
except:
    print('not available')
        
try:
    available_source = list(pathos_cl_master_source['dim_val_id'])
except:
    print('not available')
        
try:
    available_time_id = list(pathos_cl_master_time['dim_val_id'])
except:
    print('not available')
        
try:
    available_channel_id = list(pathos_cl_master_channel['dim_val_id'])
except:
    print('not available')


# ### Applying Filter

# In[25]:


try:
    initial_data = initial_data.loc[initial_data['manufacturer_id'].isin (available_manufacturer)]
except:
    print('not available')
        
try:
    initial_data = initial_data.loc[initial_data['brand_id'].isin (available_brand)]
except:
    print('not available')
        
try:
    initial_data = initial_data.loc[initial_data['product_id'].isin (available_product)]
except:
    print('not available')

try:
    initial_data = initial_data.loc[initial_data['gmo_id'].isin (available_gmo)]
except:
    print('not available')

try:
    initial_data = initial_data.loc[initial_data['source_id'].isin (available_source)]
except:
    print('not available')

try:
    initial_data = initial_data.loc[initial_data['time_id'].isin (available_time_id)]
except:
    print('not available')

try:
    initial_data = initial_data.loc[initial_data['channel_id'].isin (available_channel_id)]
except:
    print('not available')


# In[ ]:





# In[26]:


initial_data


# ## Adding Version ID in main table

# In[27]:


try:    
    initial_data['version_id'] = version_id
    initial_data['version'] = version
except:
        print('not available')


# ## Dropping unnecessary columns

# In[28]:


initial_data.drop(['version','Channel', 'Country','MANUFACTURER', 'BRAND', 'SUBBRAND', 'Product', 'CATEGORY', 'FORM', 
                   'GMO', 'MILK SOURCE', 'ORGANIC CLAIM', 'Rating', 'SEGMENT', 'Source', 'Source type', 
                   'SUB-SEGMENT'], axis=1, inplace=True, errors='ignore')


# In[29]:


initial_data.columns


# # Renaming Column names to match with Schema Syntax

# In[30]:


rename_dict = {'Id':'pathos_transaction_id', 
                'Date':'date'
                }


# In[31]:


initial_data.rename(columns=rename_dict, inplace=True)


# In[32]:


initial_data.columns


# # Transaction table

# In[33]:


connection = pg.connect('postgresql://aidatabases:Aidatabases#@65.1.96.15:5432/pathos_db')

engine = create_engine('postgresql://aidatabases:Aidatabases#@65.1.96.15:5432/pathos_db')


# In[34]:


try:
    truncate_table(table_name= 'rb_transaction_table', schema = 'pathos_rb_schema')
except:
    print("not available")


# In[35]:


push_table_pgres(initial_data, df_name = 'rb_transaction_table', schema= 'pathos_rb_schema')


# In[36]:


initial_data


# In[ ]:


import os
os.system('jupyter nbconvert --to python load_rb_transaction.ipynb')

