#!/usr/bin/env python
# coding: utf-8

# # Querying Postgres

# In[1]:


from pathos_config import *


# In[2]:

from dotenv import load_dotenv
load_dotenv()
import os
dim_schema = os.environ.get("schema")


# ## Selecting Relevant Dimensions based on Master Dim Table + Filtering based on (Active for reporting)

# ### reference schema

# In[3]:


for i in range(len(ref_table_list)):
    sample_var = ref_table_list[i]
    print(sample_var)
    vars()[sample_var]= load_data(ref_table_list[i], 'pathos_reference_common_schema', connection)


# In[4]:


# Applying Filter
pathos_ref_drivers_sectors_mapping = pathos_ref_drivers_sectors_mapping.loc[pathos_ref_drivers_sectors_mapping['sector_reporting_flag']=='Y']


# ### dimension schema

# In[5]:


for i in range(len(dim_table_list_iccs)):
    sample_var = dim_table_list_iccs[i]
    print(sample_var)
    vars()[sample_var]= load_data(dim_table_list_iccs[i], dim_schema, connection)


# In[6]:


# Applying Filter
try:
    pathos_cl_master_dim_mapping = active_filter(pathos_cl_master_dim_mapping)
except:
    print('not available')
try:
    pathos_cl_master_time = active_filter(pathos_cl_master_time)
except:
    print('not available')
    
try:
    pathos_cl_master_time_new = active_filter(pathos_cl_master_time_new)
except:
    print('not available')
    
try:
    pathos_cl_master_prod_serv_cf8 = active_filter(pathos_cl_master_prod_serv_cf8)
except:
    print('not available')
    
try:
    pathos_cl_master_prod_serv_cf7 = active_filter(pathos_cl_master_prod_serv_cf7)
except:
    print('not available')
    
try:
    pathos_cl_master_prod_serv_cf20 = active_filter(pathos_cl_master_prod_serv_20)
except:
    print('not available')


try:
    pathos_cl_master_channel = active_filter(pathos_cl_master_channel)
except:
    print('not available')
        
try:
    pathos_cl_master_age = active_filter(pathos_cl_master_age)
except:
    print('not available')
        
try:
    pathos_cl_master_gender = active_filter(pathos_cl_master_gender)
except:
    print('not available')
        
try:
    pathos_cl_master_income = active_filter(pathos_cl_master_income)
except:
    print('not available')
        
try:
    pathos_cl_master_occupation = active_filter(pathos_cl_master_occupation)
except:
    print('not available')
    
try:
    pathos_cl_master_education = active_filter(pathos_cl_master_education)
except:
    print('not available')
    
try:
    pathos_cl_master_country = active_filter(pathos_cl_master_country)
except:
    print('not available')
    
try:
    pathos_cl_master_personas = active_filter(pathos_cl_master_personas)
except:
    print('not available')
    
try:
    pathos_cl_master_prod_ver = active_filter(pathos_cl_master_prod_ver)
except:
    print('not available')


# ### generic schema

# In[7]:


connection = pg.connect('postgresql://aidatabases:Aidatabases#@34.93.95.15:5432/pathos_db')
engine = create_engine('postgresql://aidatabases:Aidatabases#@34.93.95.15:5432/pathos_db')


# In[8]:


cf7 = psql.read_sql('SELECT * FROM {}.cf7'.format(dim_schema), connection)

cf8 = psql.read_sql('SELECT * FROM {}.cf8'.format(dim_schema), connection)


# ## Dimensions based on filters

# # CF8

# In[9]:


keys = ['Q5A_1','Q5A_2','Q5A_3','Q5A_4','Q5A_5','Q5A_6','Q5A_7','Q5A_8','Q5A_9','Q5A_10','Q5B_1','Q5B_2','Q5B_3','Q5B_4','Q5B_5',
     'Q5B_6','Q5B_7','Q5B_8','Q5B_9','Q6_1','Q6_2','Q6_3','Q6_4','Q6_5','Q6_6','Q6_7','Q6_8','Q6_9','Q6_10','Q6_11','Q8A_1',
     'Q8A_2','Q8A_3','Q8A_4','Q8A_5','Q8A_6','Q8A_7','Q8A_8','Q8A_9','Q8B_1','Q8B_2','Q8B_3','Q8B_4','Q8B_5','Q8B_6','Q8C_1',
     'Q8C_2','Q8C_3','Q8C_4','Q8C_5','Q8C_6','Q8D_1','Q8D_2','Q8D_3','Q8D_4','Q8D_5','Q8E_1','Q8E_2','Q8E_3','Q8E_4','Q8F_1',
     'Q8F_2','Q8F_3','Q8F_4','Q8F_5','Q8F_6','Q8F_7','Q8F_8','Q8G_1','Q8G_2','Q8G_3','Q8G_4','Q8G_5','Q8G_6','Q8G_7','Q8G_8', 
     'Q8G_9']
values = ['q5a_1','q5a_2','q5a_3','q5a_4','q5a_5','q5a_6','q5a_7','q5a_8','q5a_9','q5a_10','q5b_1','q5b_2','q5b_3','q5b_4','q5b_5',
     'q5b_6','q5b_7','q5b_8','q5b_9','q6_1','q6_2','q6_3','q6_4','q6_5','q6_6','q6_7','q6_8','q6_9','q6_10','q6_11','q8a_1',
     'q8a_2','q8a_3','q8a_4','q8a_5','q8a_6','q8a_7','q8a_8','q8a_9','q8b_1','q8b_2','q8b_3','q8b_4','q8b_5','q8b_6','q8c_1',
     'q8c_2','q8c_3','q8c_4','q8c_5','q8c_6','q8d_1','q8d_2','q8d_3','q8d_4','q8d_5','q8e_1','q8e_2','q8e_3','q8e_4','q8f_1',
     'q8f_2','q8f_3','q8f_4','q8f_5','q8f_6','q8f_7','q8f_8','q8g_1','q8g_2','q8g_3','q8g_4','q8g_5','q8g_6','q8g_7','q8g_8', 
     'q8g_9']
rename_dict = dict(zip(keys, values))


# In[ ]:





# In[10]:


cf8 = cf8.rename(columns=rename_dict)


# In[11]:


##CF8
# to dictionary
#d = cf8_ques_id.set_index('Questions').to_dict()['ques_id']
d = pathos_cl_master_prod_serv_cf8.set_index('dim_value_name').to_dict()
cf8 = cf8.rename(columns=rename_dict) 

cf8_ques_id_list = pathos_cl_master_prod_serv_cf8['dim_val_id'].tolist()
cf8_ques_list = pathos_cl_master_prod_serv_cf8['dim_value_name'].tolist()

#index of columns
[cf8.columns.get_loc(c) for c in cf8_ques_list if c in cf8]

#copy of dataframe
cf8_temp = cf8.iloc[:,[cf8.columns.get_loc(c) for c in cf8_ques_list if c in cf8][0]:[cf8.columns.get_loc(c) for c in cf8_ques_list if c in cf8][-1]+1].copy()

#rename columns in cf8_temp
cf8_temp.columns = cf8_temp.columns.to_series().map(d['dim_val_id'])

#concatenation
cf8 = pd.concat([cf8, cf8_temp], axis=1)

# rename id
cf8 = cf8.rename(columns={'id': 'respondent_id'})


# ## Adding Dimension ID

# ### Adding time dimension to cf8

# In[12]:


cf8.insert(0, 'TimeStamp', pd.to_datetime('2018-01-23 16:46:22').replace(microsecond=0))
cf8['Qtr'] = pd.to_datetime(cf8.TimeStamp).dt.quarter
cf8['year'] = cf8['TimeStamp'].dt.year
cf8['year']= cf8["year"].astype(str)
cf8['Qtr']=cf8["Qtr"].astype(str)
cf8['Period_id'] = cf8[['year', 'Qtr']].agg('_0'.join, axis=1)


# In[13]:


#REPLACING BRAND NAME WITH BRAND ID
try:
    s = cf8.year.replace(pathos_cl_master_time.set_index('dim_value_name')['dim_val_id'])
    cf8['time_id'] = s
except:
    print('not available')


# ### cf8 & channel dimension

# In[14]:


p = ['Q11_01','Q11_02','Q11_03','Q11_04','Q11_05','Q11_06','Q11_07','Q11_08','Q11_09','Q11_10','Q11_11','Q11_12','Q11_13','Q11_14','Q11_15']
s = cf8[['Q11_01','Q11_02','Q11_03','Q11_04','Q11_05','Q11_06','Q11_07','Q11_08','Q11_09','Q11_10','Q11_11','Q11_12','Q11_13','Q11_14','Q11_15']]

cf8['channel'] = s.idxmax(axis=1)


# In[15]:


cf8['channel'] = cf8['channel'].map({'Q11_02': 'Telephone', 'Q11_01':'In-person',
       'Q11_03': 'Online','Q11_04':'Online', 'Q11_08': 'Mail',
       'Q11_05':'Telephone', 'Q11_07':'Online', 'Q11_10':'In-person',
       'Q11_09':'Telephone', 'Q11_06':'Online', 'Q11_11':'In-person', 'Q11_12':'unknown', 'Q11_13':'unknown',
                                            'Q11_14':'unknown','Q11_15':'unknown', np.nan:'unknown'})


# In[16]:


#REPLACING BRAND NAME WITH BRAND ID
try:
    s = cf8.channel.replace(pathos_cl_master_channel.set_index('dim_value_name')['dim_val_id'])
    cf8['channel_id'] = s
except:
    print('not available')


# ### Demographic reference

# In[17]:


cf8.rename(columns={'Q29': 'gender', 'Q30':'age', 'Q35':'occupation','Q38':'income'}, inplace= True)


# In[18]:


# int to string
cf8.gender = cf8.gender.astype(str)


# In[19]:


cf8['gender'] = cf8['gender'].replace({'4':'3', np.nan:'3'})


# In[20]:


# int to string
cf8.gender = cf8.gender.astype(str)

#REPLACING BRAND NAME WITH BRAND ID
try:
    s = cf8.gender.replace(pathos_cl_master_gender.set_index('dim_value_name')['dim_val_id'])
    cf8['gender_id'] = s
except:
    print('not available')


# # age dimension

# In[21]:


# int to string
cf8.age = cf8.age.astype(str)


# In[22]:


cf8['age'] = cf8['age'].replace({'8':'-1','9':'-1', '10':'-1',
                    np.nan:'-1'})


# In[23]:


# int to string
cf8.age = cf8.age.astype(str)

#REPLACING BRAND NAME WITH BRAND ID
try:
    s = cf8.age.replace(pathos_cl_master_age.set_index('dim_value_name')['dim_val_id'])
    cf8['age_id'] = s
except:
    print('not available')


# # Occupation dimension

# In[24]:


# int to string
cf8.occupation = cf8.occupation.astype(str)


# In[25]:


cf8['occupation'] = cf8['occupation'].replace({'8':'-1', '9':'-1','10':'-1' , np.nan:'-1'})


# In[26]:


# int to string
cf8.occupation = cf8.occupation.astype(str)

#REPLACING BRAND NAME WITH BRAND ID
try:
    s = cf8.occupation.replace(pathos_cl_master_occupation.set_index('dim_value_name')['dim_val_id'])
    cf8['occupation_id'] = s
except:
    print('not available')


# # income dimension

# In[27]:


# int to string
cf8.income = cf8.income.astype(str)
cf8['income'] = cf8['income'].replace({'11':'-1','12':'-1', '13':'-1',
                     np.nan:'-1'})


# In[28]:


# int to string
cf8.income = cf8.income.astype(str)

#REPLACING BRAND NAME WITH BRAND ID
try:
    s = cf8.income.replace(pathos_cl_master_income.set_index('dim_value_name')['dim_val_id'])
    cf8['income_id'] = s
except:
    print('not available')


# # Jurisdiction

# In[29]:


# #REPLACING BRAND NAME WITH BRAND ID
try:
    s = cf8.jurisdiction.replace(pathos_cl_master_country.set_index('dim_value_name')['dim_val_id'])
    cf8['location_id'] = s
except:
    print('not available')


# In[30]:


cf8 = cf8.rename(columns={'Respondent_Serial': 'id'})    #rename
pathos_cl_master_prod_serv_cf8_list = pathos_cl_master_prod_serv_cf8['dim_val_id'].tolist()
table_list = ['id', 'gender_id', 'age_id', 'occupation_id', 'income_id', 'channel_id', 'time_id']
final_list = table_list + pathos_cl_master_prod_serv_cf8_list
cf8 = cf8[final_list]


# # Applying filters to Main DataFrame

# In[31]:


try:
    available_time = list(pathos_cl_master_time['dim_val_id'])
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


# ### Applying Filter

# In[32]:


try:
    cf8 = cf8.loc[cf8['time_id'].isin (available_time)]
except:
    print('not available')
        
try:
    cf8 = cf8.loc[cf8['age_id'].isin (available_age)]
except:
    print('not available')
        
try:
    cf8 = cf8.loc[cf8['gender_id'].isin (available_gender)]
except:
    print('not available')

try:
    cf8 = cf8.loc[cf8['income_id'].isin (available_income)]
except:
    print('not available')

try:
    cf8 = cf8.loc[cf8['occupation_id'].isin (available_occupation)]
except:
    print('not available')

try:
    cf8 = cf8.loc[cf8['channel_id'].isin (available_channel)]
except:
    print('not available')


# In[33]:


ques_dict = pd.Series(pathos_cl_master_prod_serv_cf8.dim_value_name.values,index=pathos_cl_master_prod_serv_cf8.dim_val_id).to_dict()


# In[34]:


cf8.rename(columns=ques_dict, inplace=True)


# # CF 7

# In[35]:


##CF8
# to dictionary
#d = cf8_ques_id.set_index('Questions').to_dict()['ques_id']
d = pathos_cl_master_prod_serv_cf7.set_index('dim_value_name').to_dict()

cf7_ques_id_list = pathos_cl_master_prod_serv_cf7['dim_val_id'].tolist()
cf7_ques_list = pathos_cl_master_prod_serv_cf7['dim_value_name'].tolist()

#index of columns
[cf7.columns.get_loc(c) for c in cf7_ques_list if c in cf7]

#copy of dataframe
cf7_temp = cf7.iloc[:,[cf7.columns.get_loc(c) for c in cf7_ques_list if c in cf7][0]:[cf7.columns.get_loc(c) for c in cf7_ques_list if c in cf7][-1]+1].copy()

#rename columns in cf8_temp
cf7_temp.columns = cf7_temp.columns.to_series().map(d['dim_val_id'])

#concatenation
cf7 = pd.concat([cf7, cf7_temp], axis=1)

# rename id
cf7 = cf7.rename(columns={'id': 'respondent_id'})


# ## Adding Dimension ID

# ### Adding time dimension to cf8

# In[36]:


cf7.insert(0, 'TimeStamp', pd.to_datetime('2014-01-23 16:46:22').replace(microsecond=0))
cf7['Qtr'] = pd.to_datetime(cf7.TimeStamp).dt.quarter
cf7['year'] = cf7['TimeStamp'].dt.year
cf7['year']= cf7["year"].astype(str)
cf7['Qtr']=cf7["Qtr"].astype(str)
cf7['Period_id'] = cf7[['year', 'Qtr']].agg('_0'.join, axis=1)


# In[37]:


#REPLACING BRAND NAME WITH BRAND ID
try:
    s = cf7.year.replace(pathos_cl_master_time.set_index('dim_value_name')['dim_val_id'])
    cf7['time_id'] = s
except:
    print('not available')


# ### cf8 & channel dimension

# In[38]:


cf7['channel']= cf7.q10.replace({'Telephone': 'Telephone', 'Visit an office or service counter':'In-person',
       'Online/website': 'Online','E-mail':'Online', 'Regular mail': 'Mail',
       'Text message (SMS)':'Telephone', 'Social media (Twitter, Facebook)':'Online', 'Kiosk':'In-person',
       'Fax':'Telephone', 'Mobile app':'Online', 'Visit from a government employee':'In-person', 'None of the above/No others':'unknown',
                                np.nan:'unknown'})


# In[39]:


#REPLACING BRAND NAME WITH BRAND ID
try:
    s = cf7.channel.replace(pathos_cl_master_channel.set_index('dim_value_name')['dim_val_id'])
    cf7['channel_id'] = s
except:
    print('not available')


# ### Demographic reference

# In[40]:


cf7 = cf7.rename(columns={'resp_gen': 'gender'})
cf7 = cf7.rename(columns = {'resp_age':'age'})
cf7 = cf7.rename(columns= {'q34':'occupation'})
cf7 = cf7.rename(columns={'q37':'income'})


# In[41]:


cf7['gender']= cf7.gender.replace({'(DK/NS)':'unknown',
                                np.nan:'unknown'})


# In[42]:


# int to string
cf7.gender = cf7.gender.astype(str)

#REPLACING BRAND NAME WITH BRAND ID
try:
    s = cf7.gender.replace(pathos_cl_master_gender.set_index('dim_value_desc')['dim_val_id'])
    cf7['gender_id'] = s
except:
    print('not available')


# # age dimension

# In[43]:


cf7['age']= cf7.age.replace({'(DK/NS)':'-1',
                                np.nan:'-1', '75+  years':'7', '35-44 years':'3', '45-54 years':'4', '25-34 years':'2', '18-24 years':'1',
       '55-64 years':'5', '65-74 years':'6'})


# In[44]:


# int to string
cf7.age = cf7.age.astype(str)

#REPLACING BRAND NAME WITH BRAND ID
# here we have used dim_value_desc instead of dim_value_name
try:
    s = cf7.age.replace(pathos_cl_master_age.set_index('dim_value_name')['dim_val_id'])
    cf7['age_id'] = s
except:
    print('not available')


# # occupation dimension

# In[45]:


cf7['occupation']= cf7.occupation.replace({'Paid employment, part time or full time':'1',
    'Paid employment, full or part-time':'1',
'Student, full or part time':'2',
'Looking for work':'3',
'Homemaker':'4',
'Retired':'5',
'Other':'6',
'Prefer not to answer':'7',
'(DK/NS)':'-1', np.nan:'-1'})


# In[46]:


# int to string
cf7.occupation = cf7.occupation.astype(str)

#REPLACING BRAND NAME WITH BRAND ID
try:
    s = cf7.occupation.replace(pathos_cl_master_occupation.set_index('dim_value_name')['dim_val_id'])
    cf7['occupation_id'] = s
except:
    print('not available')


# # income

# In[47]:


cf7['income']= cf7.income.replace({'(DK/NS)':'unknown', np.nan:'unknown'})


# In[48]:


# int to string
cf7.income = cf7.income.astype(str)

#REPLACING BRAND NAME WITH BRAND ID
try:
    s = cf7.income.replace(pathos_cl_master_income.set_index('dim_value_desc')['dim_val_id'])
    cf7['income_id'] = s
except:
    print('not available')


# # Jurisdiction

# In[49]:


#REPLACING BRAND NAME WITH BRAND ID
try:
    s = cf7.jurisdiction.replace(pathos_cl_master_country.set_index('dim_value_name')['dim_val_id'])
    cf7['location_id'] = s
except:
    print('not available')


# In[50]:


cf7 = cf7.rename(columns={'ID': 'id'})    #rename
pathos_cl_master_prod_serv_cf7_list = pathos_cl_master_prod_serv_cf7['dim_val_id'].tolist()
common = list(set(pathos_cl_master_prod_serv_cf7_list).intersection(cf7.columns))
table_list = ['id', 'gender_id', 'age_id', 'occupation_id', 'income_id', 'channel_id', 'time_id']
final_list = table_list + common
cf7 = cf7[final_list]


# # Applying filters to Main DataFrame

# In[51]:


try:
    available_time = list(pathos_cl_master_time['dim_val_id'])
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


# ### Applying Filter

# In[52]:


try:
    cf7 = cf7.loc[cf7['time_id'].isin (available_time)]
except:
    print('not available')
        
try:
    cf7 = cf7.loc[cf7['age_id'].isin (available_age)]
except:
    print('not available')
        
try:
    cf7 = cf7.loc[cf7['gender_id'].isin (available_gender)]
except:
    print('not available')

try:
    cf7 = cf7.loc[cf7['income_id'].isin (available_income)]
except:
    print('not available')

try:
    cf7 = cf7.loc[cf7['occupation_id'].isin (available_occupation)]
except:
    print('not available')

try:
    cf7 = cf7.loc[cf7['channel_id'].isin (available_channel)]
except:
    print('not available')


# In[53]:


ques_dict = pd.Series(pathos_cl_master_prod_serv_cf7.dim_value_name.values,index=pathos_cl_master_prod_serv_cf7.dim_val_id).to_dict()


# In[54]:


cf7.rename(columns=ques_dict, inplace=True)


# In[55]:


# removing first two integers from ID
cf7['id'] = cf7['id'].astype(str)
cf7['id'] = cf7['id'].str[2:]
cf7['id'] = pd.to_numeric(cf7['id'], errors='coerce')
cf7 = (cf7[cf7['id'].notnull()])
cf7['id'] = cf7['id'].astype(int)


# In[56]:


df = pd.concat([cf8,cf7], axis=0, ignore_index=True)


# # Adding Version

# In[57]:


current_version = list(pathos_cl_master_prod_ver['pathos_ref_prd_rel_unique_id'])


# In[58]:


df['version_id'] = current_version[0]


# In[59]:


df = df[df['id'].notna()]


# # Transaction table

# In[60]:


connection = pg.connect('postgresql://aidatabases:Aidatabases#@34.93.95.15:5432/pathos_db')

engine = create_engine('postgresql://aidatabases:Aidatabases#@34.93.95.15:5432/pathos_db')


# In[61]:


try:
    truncate_table(table_name= 'iccs_transaction_table', schema = dim_schema)
except:
    print("not available")


# In[62]:


df['pathos_transaction_id']=list(range(1, df.shape[0]+1))


# In[63]:


df = df.astype(str)


# In[64]:


di = {'1 - Very poor':1, '5 - Very good':5, '(DK/NS)': None, 'Does not apply': None}


# In[65]:


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


# In[69]:


push_table_pgres(df, df_name = 'load_iccs_transaction_cf7_cf8', schema= dim_schema)


# In[ ]:


#import os
#os.system('jupyter nbconvert --to python load_iccs_transaction_cf7_cf8.ipynb')


# In[ ]:





# In[ ]:





# In[ ]:




