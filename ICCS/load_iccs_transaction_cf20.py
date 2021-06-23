#!/usr/bin/env python
# coding: utf-8

# # Querying Postgres

# In[83]:


from pathos_config import *


# In[84]:


from dotenv import load_dotenv
load_dotenv()
import os
dim_schema = os.environ.get("schema")

# ## Selecting Relevant Dimensions based on Master Dim Table + Filtering based on (Active for reporting)

# ### reference schema

# In[85]:


for i in range(len(ref_table_list)):
    sample_var = ref_table_list[i]
    print(sample_var)
    vars()[sample_var]= load_data(ref_table_list[i], 'pathos_reference_common_schema', connection)


# In[ ]:


# Applying Filter
pathos_ref_drivers_sectors_mapping = pathos_ref_drivers_sectors_mapping.loc[pathos_ref_drivers_sectors_mapping['sector_reporting_flag']=='Y']


# ### dimension schema

# In[ ]:


for i in range(len(dim_table_list_iccs)):
    sample_var = dim_table_list_iccs[i]
    print(sample_var)
    vars()[sample_var]= load_data(dim_table_list_iccs[i], dim_schema, connection)


# In[ ]:


# Applying Filter
try:
    pathos_cl_master_dim_mapping = active_filter(pathos_cl_master_dim_mapping)
except:
    print('not available: ' + i)
try:
    pathos_cl_master_time = active_filter(pathos_cl_master_time)
except:
    print('not available: ' + i)
    
try:
    pathos_cl_master_time_new = active_filter(pathos_cl_master_time_new)
except:
    print('not available: ' + i)
    
try:
    pathos_cl_master_prod_serv_cf8 = active_filter(pathos_cl_master_prod_serv_cf8)
except:
    print('not available: ' + i)
    
try:
    pathos_cl_master_prod_serv_cf7 = active_filter(pathos_cl_master_prod_serv_cf7)
except:
    print('not available: ' + i)
    
try:
    pathos_cl_master_prod_serv_cf20 = active_filter(pathos_cl_master_prod_serv_cf20)
except:
    print('not available: ' + i)


try:
    pathos_cl_master_channel = active_filter(pathos_cl_master_channel)
except:
    print('not available: ' + i)
        
try:
    pathos_cl_master_age = active_filter(pathos_cl_master_age)
except:
    print('not available: ' + i)
        
try:
    pathos_cl_master_gender = active_filter(pathos_cl_master_gender)
except:
    print('not available: ' + i)
        
try:
    pathos_cl_master_income = active_filter(pathos_cl_master_income)
except:
    print('not available: ' + i)
        
try:
    pathos_cl_master_occupation = active_filter(pathos_cl_master_occupation)
except:
    print('not available: ' + i)
    
try:
    pathos_cl_master_education = active_filter(pathos_cl_master_education)
except:
    print('not available: ' + i)
    
try:
    pathos_cl_master_country = active_filter(pathos_cl_master_country)
except:
    print('not available: ' + i)
    
# try:
#     pathos_cl_master_personas = active_filter(pathos_cl_master_personas)
# except:
#     print('not available: ' + i)
    
try:
    pathos_cl_master_prod_ver = active_filter(pathos_cl_master_prod_ver)
except:
    print('not available: ' + i)


# # CF20

# In[86]:


connection = pg.connect('postgresql://aidatabases:Aidatabases#@34.93.95.15:5432/pathos_db')

engine = create_engine('postgresql://aidatabases:Aidatabases#@34.93.95.15:5432/pathos_db')


# In[87]:


cf20 = psql.read_sql('SELECT * FROM {}.cf20'.format(dim_schema), connection)


# In[10]:


##CF20
d = pathos_cl_master_prod_serv_cf20.set_index('dim_value_name').to_dict()

cf20_ques_id_list = pathos_cl_master_prod_serv_cf20['dim_val_id'].tolist()
cf20_ques_list = pathos_cl_master_prod_serv_cf20['dim_value_name'].tolist()

#copy of dataframe
cf20_temp = cf20.iloc[:,[cf20.columns.get_loc(c) for c in cf20_ques_list if c in cf20][0]:[cf20.columns.get_loc(c) for c in cf20_ques_list if c in cf20][-1]+1].copy()

#rename columns in cf8_temp
cf20_temp.columns = cf20_temp.columns.to_series().map(d['dim_val_id'])

#concatenation
cf20 = pd.concat([cf20, cf20_temp], axis=1)


# In[11]:


# rename id
cf20 = cf20.rename(columns={'id': 'respondent_id',
                           'marketgroup':'jurisdiction',
                           'mainchannel':'channel',
                           'quotagerange': 'age',
                           'q21a': 'reviews'})


# ## Adding Dimension ID

# ### education dimension

# In[15]:


cf20['education'].replace({'education':'-1','None':'-1', '7':'-1',
np.nan:'-1', '8': '-1'},inplace=True)


# In[16]:


try:
    s = cf20.education.replace(pathos_cl_master_education.set_index('dim_value_name')['dim_val_id'])
    cf20['education_id'] = s
except:
    print('not available')


# In[ ]:





# ### Adding Age dimension to cf20

# In[20]:


cf20['age'].replace({'quotagerange':'-1','None':'-1', 'REF':'-1',
np.nan:'-1', '18_24': '1', '25_34':'2',
       '35_44': '3','45_54':'4', '55_65': '5',
       'Over_65':'6'},inplace=True)


# In[21]:


try:
    s = cf20.age.replace(pathos_cl_master_age.set_index('dim_value_name')['dim_val_id'])
    cf20['age_id'] = s
except:
    print('not available')


# ### Adding channel dimension

# In[23]:


cf20['channel'].replace({'3': 'Online', '4':'Online',
       '6': 'Online','7':'Online', '2': 'Telephone','5': 'Telephone','9':'Telephone', '8': 'Mail',
       '1':'In-person','10': 'In-person','11':'In-person', None: 'unknown', np.nan:'unknown','mainchannel': 'unknown' },inplace=True)


# In[24]:


# cf20['channel']= cf20.channel.map({'3': 'Online', '4':'Online',
#        '6': 'Online','7':'Online', '2': 'Telephone','5': 'Telephone','9':'Telephone', '8': 'Mail',
#        '1':'In-person','10': 'In-person','11':'In-person'})


# In[25]:


try:
    s = cf20.channel.replace(pathos_cl_master_channel.set_index('dim_value_name')['dim_val_id'])
    cf20['channel_id'] = s
except:
    print('not available')


# ### Demographic reference

# In[26]:


cf20['gender'].replace({'1': '1', '2':'2',
       '3': '3','4': '3', 'gender':'3',
       None: '3',np.nan:'3','mainchannel': '3' },inplace=True)


# In[27]:


# int to string
cf20.gender = cf20.gender.astype(str)

#REPLACING BRAND NAME WITH BRAND ID
try:
    s = cf20.gender.replace(pathos_cl_master_gender.set_index('dim_value_name')['dim_val_id'])
    cf20['gender_id'] = s
except:
    print('not available')


# ### location dimension

# In[29]:


cf20['jurisdiction']= cf20["jurisdiction"].replace({'BC': 'British Columbia', 'NT': 'Northwest Territories',
                                                 'FG': 'Federal Government', 'PE': 'Prince Edward Island',
                                                 'RP': 'Peel', 'VI': 'City of Victoria',
                                                 'TO': 'City of Toronto', 'YT':'Yukon',
                                                 'marketgroup': 'unknown','NATIONAL':'National Survey', 
                                                    None:'unknown',np.nan:'unknown' })


# In[31]:


#REPLACING BRAND NAME WITH BRAND ID
try:
    s = cf20.jurisdiction.replace(pathos_cl_master_country.set_index('dim_value_name')['dim_val_id'])
    cf20['location_id'] = s
except:
    print('not available')


# ### time dimension

# In[32]:


cf20['year'] = 2020

cf20['year']= cf20['year'].astype(str)
try:
    s = cf20.year.replace(pathos_cl_master_time.set_index('dim_value_name')['dim_val_id'])
    cf20['time_id'] = s
except:
    print('not available')


# In[34]:


cf20 = cf20.rename(columns={'respondent_id': 'id'})    #rename
pathos_cl_master_prod_serv_cf20_list = pathos_cl_master_prod_serv_cf20['dim_val_id'].tolist()
pathos_cl_master_prod_serv_cf20_list = [float(i) for i in pathos_cl_master_prod_serv_cf20_list]
common_prod_list = list(set(pathos_cl_master_prod_serv_cf20_list).intersection(cf20.columns))
table_list = ['id', 'gender_id', 'location_id', 'channel_id', 'age_id', 'reviews', 'time_id', 'education_id']
final_list = table_list + common_prod_list
cf20 = cf20[final_list]


# # Applying filters to Main DataFrame

# In[37]:


try:
    available_time = list(pathos_cl_master_time['dim_val_id'])
except:
    print('not available')

try:
    available_time_new = list(pathos_cl_master_time_new['dim_val_id'])
except:
    print('not available')
        
        
try:
    available_age = list(pathos_cl_master_age['dim_val_id'])
except:
    print('not available')
        
try:
    available_occupation = list(pathos_cl_master_occupation['dim_val_id'])
except:
    print('not available')
        
try:
    available_gender = list(pathos_cl_master_gender['dim_val_id'])
except:
    print('not available')
        
try:
    available_income = list(pathos_cl_master_income['dim_val_id'])
except:
    print('not available')
        
try:
    available_channel = list(pathos_cl_master_channel['dim_val_id'])
except:
    print('not available')
    
try:
    available_location = list(pathos_cl_master_country['dim_val_id'])
except:
    print('not available')
    
try:
    available_education = list(pathos_cl_master_education['dim_val_id'])
except:
    print('not available')


# In[38]:


available_channel = available_channel + [np.NaN]
available_age = available_age + [np.NaN]
available_occupation = available_occupation + [np.NaN]
available_gender = available_gender + [np.NaN]
available_income = available_income + [np.NaN]
available_channel = available_channel + [np.NaN]
available_location = available_location + [np.NaN] + ['nan', 'NaN', 'None']
available_education = available_education + [np.NaN] + ['nan', 'NaN', 'None']


# ### Applying Filter

# In[40]:


try:
    cf20 = cf20.loc[cf20['age_id'].isin (available_age)]
except:
    print('not available')
print(cf20.shape)
        
try:
    cf20 = cf20.loc[cf20['gender_id'].isin (available_gender)]
except:
    print('not available')
print(cf20.shape)

try:
    cf20 = cf20.loc[cf20['income_id'].isin (available_income)]
except:
    print('not available')
print(cf20.shape)

try:
    cf20 = cf20.loc[cf20['occupation_id'].isin (available_occupation)]
except:
    print('not available')
print(cf20.shape)

try:
    cf20 = cf20.loc[cf20['channel_id'].isin (available_channel)]
except:
    print('not available')
print(cf20.shape)

try:
    cf20 = cf20.loc[cf20['location_id'].isin (available_location)]
except:
    print('not available')
print(cf20.shape)

try:
    cf20 = cf20.loc[cf20['education_id'].isin (available_education)]
except:
    print('not available')
print(cf20.shape)


# In[41]:


ques_dict = pd.Series(pathos_cl_master_prod_serv_cf20.dim_value_name.values,index=pathos_cl_master_prod_serv_cf20.dim_val_id).to_dict()


# In[42]:


cf20.rename(columns=ques_dict, inplace=True)


# In[44]:


# removing null id rows and null reviews


# In[47]:


cf20 = cf20[cf20['id'].notna()]


# In[48]:


cf_reviews = pd.DataFrame(cf20['reviews'].value_counts())


# In[49]:


cf20['reviews']= cf20['reviews'].replace({'none':np.nan,'None':np.nan, 'none':np.nan,
'None':np.nan,
'na':np.nan, 
'Na':np.nan,
'N/a':np.nan,
'NONE':np.nan,
'non':np.nan,
'nan': np.nan,
'NULL': np.nan,
'Null': np.nan,
'null': np.nan,})


# In[50]:


cf20 = cf20[cf20['reviews'].notna()]


# In[52]:


cf20['reviews']= cf20['reviews'].dropna(axis=0)


# In[56]:


df = cf20


# # Adding Version

# In[58]:


current_version = pathos_cl_master_prod_ver['pathos_ref_prd_rel_unique_id'].to_list()
version_index = pathos_cl_master_prod_ver.index
df['version_id'] = current_version[0]


# # Transaction table

# In[59]:


connection = pg.connect('postgresql://aidatabases:Aidatabases#@34.93.95.15:5432/pathos_db')

engine = create_engine('postgresql://aidatabases:Aidatabases#@34.93.95.15:5432/pathos_db')


# In[60]:


try:
    truncate_table(table_name= 'load_iccs_transaction_cf20', schema = dim_schema)
except:
    print("not available")

# In[61]:


df['pathos_transaction_id']=list(range(1, df.shape[0]+1))


# In[62]:


df = df.astype(str)


# In[63]:


di = {'1 - Very poor':1, '5 - Very good':5, '(DK/NS)': None, 'Does not apply': None}


# In[64]:


for i in df.columns:
    df = df.replace({i: di})


# In[66]:


di = {'None':np.nan}
for i in df.columns:
    df = df.replace({i: di})


# In[67]:


df = df.fillna(value=np.nan)


# In[68]:


df.replace(to_replace=[None], value=np.nan, inplace=True)


# In[70]:


null_list = ['id','q6l' ,'q6k' ,'q6j' ,'q6g' ,'q6f' ,'q6e' ,'q6d' ,'q6c' ,'q5br' ,'q5bq' ,'q5bp' ,'q5bl' ,'q5bk' ,'q5bj' ,'q5ai' ,'q5ah' ,'q5ag' ,'q5af' ,'q5ae' ,'q5ad' ,'q5ac' ,'q5ab' ,'q5aa']


# In[71]:


df[null_list] = df[null_list].apply(pd.to_numeric, errors='coerce')


# # Values are not in proper format
# - like id is not in numeric and not unique,
# - Q6c is also not numeric

# In[ ]:





# In[73]:


df['reviews']= df['reviews'].replace({'none':np.nan,'None':np.nan, 'none':np.nan,
'None':np.nan,
'na':np.nan, 
'Na':np.nan,
'N/a':np.nan,
'NONE':np.nan,
'non':np.nan,
'nan': np.nan,
'NULL': np.nan,
'Null': np.nan,
'null': np.nan,})


# In[74]:


df = df[df['reviews'].notna()]


# In[76]:


df['reviews']= df['reviews'].dropna(axis=0)


# In[81]:


push_table_pgres(df, df_name = 'load_iccs_transaction_cf20', schema= dim_schema)

# In[82]:


#import os
#os.system('jupyter nbconvert --to python load_iccs_transaction_cf20.ipynb')


# In[ ]:





# In[ ]:





# In[ ]:




