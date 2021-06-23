#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pathos_config import *


# In[2]:


final_transaction_table = load_data('rb_transaction_table', 'pathos_rb_schema', connection)
final_transaction_table.dropna(axis=1, how='all', inplace=True)
from_ml_team = load_data('rb_from_ml_team', 'pathos_rb_schema', connection)


# In[3]:


final_summary = pd.merge(final_transaction_table, from_ml_team, on="review_id", how= 'left' )


# In[5]:


final_summary.head(1)


# # Filtering PERSONAS

# ### Loading Tables from Dimension and reference Schemas

# In[4]:


# try:
#     pathos_ref_personas = load_data('pathos_ref_personas','pathos_reference_common_schema', connection)
# except:
#     print('not available')


### dimension schema

for i in range(len(dim_table_list_rb)):
    sample_var = dim_table_list_rb[i]
    print(sample_var)
    vars()[sample_var]= load_data(dim_table_list_rb[i], 'pathos_rb_schema', connection)
for i in range(len(ref_table_list)):
    sample_var = ref_table_list[i]
    print(sample_var)
    vars()[sample_var]= load_data(ref_table_list[i], 'pathos_reference_common_schema', connection)
    
#reference schema# Applying Filter
try:
    pathos_ref_drivers_sectors_mapping = pathos_ref_drivers_sectors_mapping.loc[pathos_ref_drivers_sectors_mapping['sector_reporting_flag']=='Y']
except:
    print('not available')


# ### Pathos Persona Dimension Table Filter Logic

# In[5]:


try:
    pathos_cl_master_personas['new'] = pathos_cl_master_personas.apply(lambda x: x.index[x == "Y"].tolist(), axis=1)
except:
    print('not available')


# In[7]:


p=[]
q=[]
for i, j in zip(pathos_cl_master_personas['persona'], pathos_cl_master_personas['new']):
    p.append(i)
    q.append(j)


# ### ACTIVE PERSONA LIST

# In[8]:


active_persona = dict(zip(p, q))


# In[9]:


active_persona


# ## Client Filter Hard-Coded
# 

# In[10]:


client = 'RB'
client_id = pathos_ref_clients.loc[pathos_ref_clients['client_name'] == client].client_id.iloc[0]
client_dict = pd.Series(pathos_ref_clients.client_sector_desc.values,index=pathos_ref_clients.client_id).to_dict()


# ## Calculating Driver Score based on current Client Sector

# In[11]:


sector = client_dict.get(client_id)
sector_id = pathos_ref_sectors.loc[pathos_ref_sectors['sectors'] == sector].sector_id.iloc[0]
df_sector_weight = pathos_ref_drivers_sectors_mapping.loc[pathos_ref_drivers_sectors_mapping["sector_id"] == sector_id]
#dictionary of driver_id and sector_weight
sector_wt_dict = pd.Series(df_sector_weight.sector_weight.values,index=df_sector_weight.driver_id).to_dict()


# ## Emotion_score

# ### rename column name with emotion_ids

# In[12]:


final_summary.rename(columns=dict(zip(pathos_ref_emotions["emotions"], pathos_ref_emotions["emotions_id"])), inplace=True)


# In[13]:


#sector_id 
emotion_id_list = list(pathos_ref_emotions['emotions_id'])

#taking common driver_id from sector dataframe and main dataframe
common_emotions = list(set(emotion_id_list).intersection(final_summary.columns))


# In[14]:


# dataframe containing only common drivers
df_emotions = final_summary[common_emotions]


# In[15]:


#dictionary of driver_id and sector_weight
emotion_wt_dict = pd.Series(pathos_ref_emotions.emotion_weight.values,index=pathos_ref_emotions.emotions_id).to_dict()


# In[16]:


#multiplying emotions with weight and summation
emotion_score = (pd.Series(emotion_wt_dict)*df_emotions).sum(1)


# In[17]:


final_summary['emotion_score'] = pd.DataFrame(emotion_score)


# ## Driver Score

# ### rename column name with driver_ids

# In[18]:


final_summary.rename(columns=dict(zip(pathos_ref_drivers["drivers"], pathos_ref_drivers["driver_id"])), inplace=True)


# In[19]:


#sector_id 
sector_id_list = list(sector_wt_dict.keys())

#taking common driver_id from sector dataframe and main dataframe
common_driver = list(set(sector_id_list).intersection(final_summary.columns))


# In[20]:


# dataframe containing only common drivers
df_drivers = final_summary[common_driver]


# In[21]:


final_summary['drivers_summation']=df_drivers.sum(axis=1)


# In[22]:


df_drivers


# In[23]:


for i in df_drivers.columns:
    df_drivers[i]= (df_drivers[i]*final_summary['emotion_score'])/final_summary['drivers_summation']
#df_drivers[20009]= (df_drivers[20009]*final_summary['emotion_score'])/df_drivers['drivers_summation']


# In[24]:


df_drivers = (pd.Series(sector_wt_dict)*df_drivers)


# In[25]:


### Only common drivers (no null drivers)


# In[26]:


df_drivers = df_drivers[common_driver]


# In[27]:


df_drivers.head()


# In[28]:


final_summary.head(2)


# In[29]:


# for i, j in zip(df_drivers.columns,range(df_drivers.shape[0])):
#     df_drivers[i][j]= (df_drivers[i][j]/df_drivers.sum(axis=1)[j])*final_summary['emotion_score'][j]
#     print(df_drivers[i])   


# In[30]:


#multiplying drivers with weight and summation
driver_score = (pd.Series(sector_wt_dict)*df_drivers).sum(1)


# In[31]:


final_summary['driver_score'] = pd.DataFrame(driver_score)


# ### Dropping old drivers scores

# In[32]:


final_summary.columns


# In[33]:


final_summary.drop(['drivers_summation', 20001,20002,20003,20004,20005,20006,20007,20008,20009,
                    20010,20011,20012,20013,20014,20015,20016,20017,20018,20019,20020,20021,20022,
                    20023,20024,20025,20026,20027,20028,20029,20030], axis=1, inplace=True,errors='ignore' )


# ### Adding updated drivers value

# In[34]:


final_summary = pd.concat([final_summary, df_drivers], axis=1)


# In[35]:


final_summary.head(2)


# ## Emotional Engagement

# In[36]:


final_summary['emotional_engagement']= (final_summary.driver_score * final_summary.emotion_score * final_summary.pos_score_by_weights) + (final_summary.driver_score * final_summary.emotion_score * final_summary.neg_score_by_weights)


# # Customer_segment

# In[37]:


df = pathos_ref_drivers.groupby(['customer_segment', 'driver_id'], as_index=False).count()


# In[38]:


# list of driver_id on various customer segments
people_list = list(df.loc[df['customer_segment'] == "people"].driver_id)
process_list = list(df.loc[df['customer_segment'] == "process"].driver_id)
product_list = list(df.loc[df['customer_segment'] == "product"].driver_id)
product_service_list = list(df.loc[df['customer_segment'] == "product/service"].driver_id)


# In[39]:


product_service_list = product_list + product_service_list


# ## Adding customer segment

# In[40]:


#taking common driver_id from sector dataframe and main dataframe
common_people = list(set(people_list).intersection(final_summary.columns))
# dataframe containing only common drivers
df_people = final_summary[common_people]
#multiplying with weight and summation
people_score = (pd.Series(sector_wt_dict)*df_people).sum(1)
final_summary['people_score'] = pd.DataFrame(people_score)


# In[41]:


#taking common driver_id from sector dataframe and main dataframe
common_process = list(set(process_list).intersection(final_summary.columns))
# dataframe containing only common drivers
df_process = final_summary[common_process]
#multiplying with weight and summation
process_score = (pd.Series(sector_wt_dict)*df_process).sum(1)
final_summary['process_score'] = pd.DataFrame(process_score)


# In[42]:


#taking common driver_id from sector dataframe and main dataframe
common_product = list(set(product_service_list).intersection(final_summary.columns))
# dataframe containing only common drivers
df_product = final_summary[common_product]
#multiplying with weight and summation
product_score = (pd.Series(sector_wt_dict)*df_product).sum(1)
final_summary['product_score'] = pd.DataFrame(product_score)


# # Marketing_segment

# In[43]:


df = pathos_ref_drivers.groupby(['marketing_segment', 'driver_id'], as_index=False).count()


# In[44]:


# list of driver_id on various marketing segments
intent_list = list(df.loc[df['marketing_segment'] == "intent"].driver_id)
loyalty_list = list(df.loc[df['marketing_segment'] == "loyalty"].driver_id)
values_list = list(df.loc[df['marketing_segment'] == "values"].driver_id)


# ## Adding customer marketing segment

# In[45]:


#taking common driver_id from sector dataframe and main dataframe
common_intent = list(set(intent_list).intersection(final_summary.columns))
# dataframe containing only common drivers
df_intent = final_summary[common_intent]
#multiplying with weight and summation
intent_score = (pd.Series(sector_wt_dict)*df_intent).sum(1)
final_summary['intent_score'] = pd.DataFrame(intent_score)


# In[46]:


#taking common driver_id from sector dataframe and main dataframe
common_loyalty = list(set(loyalty_list).intersection(final_summary.columns))
# dataframe containing only common drivers
df_loyalty = final_summary[common_loyalty]
#multiplying with weight and summation
loyalty_score = (pd.Series(sector_wt_dict)*df_loyalty).sum(1)
final_summary['loyalty_score'] = pd.DataFrame(loyalty_score)


# In[47]:


#taking common driver_id from sector dataframe and main dataframe
common_values = list(set(values_list).intersection(final_summary.columns))
# dataframe containing only common drivers
df_values = final_summary[common_values]
#multiplying with weight and summation
values_score = (pd.Series(sector_wt_dict)*df_values).sum(1)
final_summary['values_score'] = pd.DataFrame(values_score)


# In[48]:


final_summary.shape


# In[49]:


final_summary.columns


# # Adding predict emotion first element

# In[50]:


# predicted_emotion_list = final_summary['predicted_emotion'].tolist()


# In[51]:


# predicted_emotion_list = map(str, predicted_emotion_list)


# In[52]:


# temp = [] 
  
# # Getting elem in list of list format 
# for elem in predicted_emotion_list: 
#     temp2 = elem.split(', ') 
#     temp.append((temp2)) 
  
# # List initialization 
# Output = []  
  
# # Using Iteration to convert  
# # element into list of list 
# for elem in temp: 
#     temp3 = [] 
#     for elem2 in elem: 
#         temp3.append(elem2) 
#     Output.append(temp3)


# In[53]:


def Extract(lst): 
    return [item[0] for item in lst] 


# In[54]:


# predicted_emotion_list = Extract(Output)


# In[55]:


# final_summary['predicted_emotion'] = predicted_emotion_list


# In[56]:


# Adding predicted driver first element


# In[57]:


predicted_driver_list = final_summary['predicted_driver'].tolist()
predicted_driver_list = map(str, predicted_driver_list)
temp = [] 
  
# Getting elem in list of list format 
for elem in predicted_driver_list: 
    temp2 = elem.split(', ') 
    temp.append((temp2)) 
  
# List initialization 
Output = []  
  
# Using Iteration to convert  
# element into list of list 
for elem in temp: 
    temp3 = [] 
    for elem2 in elem: 
        temp3.append(elem2) 
    Output.append(temp3)

predicted_driver_list = Extract(Output)

final_summary['predicted_driver'] = predicted_driver_list


# In[58]:


final_summary.shape


# In[59]:


ppp_dict = pd.Series(pathos_ref_drivers.customer_segment.values,index=pathos_ref_drivers.drivers).to_dict()
ilv_dict = pd.Series(pathos_ref_drivers.marketing_segment.values,index=pathos_ref_drivers.drivers).to_dict()


# In[60]:


final_summary['predicted_driver_ppp'] = final_summary['predicted_driver'].map(ppp_dict)


# In[61]:


final_summary['predicted_driver_ilv'] = final_summary['predicted_driver'].map(ilv_dict)


# ## Dropping unnecessary columns

# In[62]:


final_summary.drop(['pathos_transaction_id','Id','Post Level Text', 'score_prediction', 'predicted_emotion'], axis=1, inplace=True,errors='ignore' )


# # renaming column names to match with schema

# In[63]:


rename_dict = {'matched_pos_terms':'behaviour_qual_positive_comments',
'matched_neg_terms':'behaviour_qual_negative_comments',
'BRAND_ID': 'brand_id',
'MANUFACTURER_ID': 'manufacturer_id',
'Id':'pathos_summary_id',
'behavior_score': 'behaviour_score', 
'driver_score':'drivers_score',
'emotion_score': 'emotions_score',
'emotional_engagement': 'emotional_engagement_score',
'Channel':'channel',
'Country':'country',
               
'pos_score_by_weights':'positive_score_by_weights', 
'neg_score_by_weights':'negative_score_by_weights',
'Date':'date',

1001:'emotions_happy_score',
1002:'emotions_surprised_score',
1003:'emotions_excited_score',
1005:'emotions_sad_score',
1006:'emotions_fear_score',
1007:'emotions_frustration_score',
1008:'emotions_angry_score',
1009:'emotions_disgust_score',

20001:'drivers_personalization_score',
20002:'drivers_positivity_score',
20003:'drivers_well_being_score',
20004:'drivers_exciting_score',
20005:'drivers_belonging_score',
20006:'drivers_sustainability_score',
20007:'drivers_security_score',
20008:'drivers_convenience_score',
20009:'drivers_care_score',
20010:'drivers_timeliness_score',
20011:'drivers_respect_score',
20012:'drivers_knowledge_score',
20013:'drivers_fairness_score',
20014:'drivers_channel_satisfaction_score',
20015:'drivers_outcome_score',
20016:'drivers_communication_score',
20017:'drivers_ease_of_access_score',
20018:'drivers_future_issues_score',
20019:'drivers_competence_score',
20020:'drivers_extra_mile_score',
20021:'drivers_waiting_time_score',
20022:'drivers_appeal_score',
20023:'drivers_preferred_chanel_score',
20024:'drivers_freshness_score',
20025:'drivers_trust_score',
20026:'drivers_regret_score',
20027:'drivers_relationship_score',
20028:'drivers_innovative_score',
20029:'drivers_value_for_money_score',
20030:'drivers_packaging_score',
}


# In[64]:


final_summary.rename(columns=rename_dict, inplace=True)


# In[65]:


final_summary.head(1)


# # Feature Engineering

# ## Creating 'all_pathos' column where all values are 'ALL'

# In[66]:


final_summary['all_pathos']= 'Overall'


# ## Normalization

# In[67]:


final_summary


# In[68]:


# import pandas as pd
# from sklearn.preprocessing import MinMaxScaler

# scaler = MinMaxScaler(feature_range=(0, 1), clip=True)
# final_summary[['n_emotional_engagement_score']] = scaler.fit_transform(final_summary[['emotional_engagement_score']])


# In[69]:


import pandas as pd
from sklearn.preprocessing import MinMaxScaler

scaler = MinMaxScaler(feature_range=(0, 100), clip=True)
final_summary[['n_emotional_engagement_score']] = scaler.fit_transform(final_summary[['emotional_engagement_score']])


# In[70]:


final_summary.n_emotional_engagement_score.max()


# ## Quintiles

# In[71]:


#final_summary['emotional_engagement_quintile'] = pd.qcut(final_summary['n_emotional_engagement_score'], 4, labels=False)


# In[72]:


#final_summary['emotional_engagement_quintile']= final_summary['emotional_engagement_quintile'].map({0:'Not Engaged',1:'Somewhat Engaged', 2:'Engaged', 3: 'Highly Engaged'})


# In[73]:


final_summary['emotional_engagement_quintile']= ""


# In[74]:


#final_summary['emotional_engagement_quintile'].values[final_summary['n_emotional_engagement_score'].values > 0.5] = ("Highly Engaged")


# In[75]:


#final_summary['emotional_engagement_quintile'].values[final_summary['n_emotional_engagement_score'].values < (-0.5)] = ("Not Engaged")


# In[76]:


final_summary['emotional_engagement_quintile'] = np.where(final_summary["n_emotional_engagement_score"] > 0.5, "Highly Engaged", final_summary["emotional_engagement_quintile"])


# In[77]:


final_summary['emotional_engagement_quintile'] = np.where(final_summary["n_emotional_engagement_score"] < (-0.5), "Not Engaged", final_summary["emotional_engagement_quintile"])


# In[78]:


final_summary['emotional_engagement_quintile'] = np.where((final_summary["n_emotional_engagement_score"]<=0) & (final_summary["n_emotional_engagement_score"]>(-0.5)), "Somewhat Engaged", final_summary["emotional_engagement_quintile"])


# In[79]:


final_summary['emotional_engagement_quintile'] = np.where((final_summary["n_emotional_engagement_score"]>0) & (final_summary["n_emotional_engagement_score"]<=0.5), "Engaged", final_summary["emotional_engagement_quintile"])


# In[80]:


#final_summary['emotional_engagement_quintile'] = np.where((final_summary["n_emotional_engagement_score"] <= 0) & (final_summary["n_emotional_engagement_score"] > (-0.5)), final_summary["n_emotional_engagement_score"], "Somewhat Engaged")


# In[81]:


final_summary.emotional_engagement_quintile.value_counts()


# In[82]:


final_summary.groupby(final_summary['emotional_engagement_quintile']).n_emotional_engagement_score.mean().sort_values()


# # Summary Table

# In[83]:


final_summary.head(2)


# In[84]:


final_summary['pathos_summary_id']=list(range(1, final_summary.shape[0]+1))


# # ADDING NAME OF DIMENSIONS

# In[85]:


#REPLACING BRAND NAME WITH BRAND ID
try:
    s = final_summary.brand_id.replace(pathos_cl_master_brand.set_index('dim_val_id')['dim_value_name'])
    final_summary['brand'] = s
except:
    print('not available')


# In[86]:


#REPLACING MANUFACTURER NAME WITH MANUFACTURER ID
try:
    s = final_summary.manufacturer_id.replace(pathos_cl_master_manufacturer.set_index('dim_val_id')['dim_value_name'])
    final_summary['manufacturer'] = s
except:
    print('not available')


# In[87]:


#REPLACING PRODUCT NAME WITH PRODUCT ID
try:
    s = final_summary.product_id.replace(pathos_cl_master_product.set_index('dim_val_id')['dim_value_name'])
    final_summary['product'] = s
except:
    print('not available')


# In[88]:


#REPLACING GMO NAME WITH GMO ID
#final_summary.GMO.replace('with a Non GMO Claim','With a Non GMO Claim', inplace = True )
try:
    s = final_summary.gmo_id.replace(pathos_cl_master_gmo.set_index('dim_val_id')['dim_value_name'])
    final_summary['gmo'] = s
except:
    print('not available')


# In[89]:


#REPLACING SOURCE NAME WITH SOURCE ID
try:
    s = final_summary.source_id.replace(pathos_cl_master_source.set_index('dim_val_id')['dim_value_name'])
    final_summary['source'] = s
except:
    print('not available')


# In[90]:


#REPLACING CHANNEL NAME WITH CHANNEL ID
try:
    s = final_summary.channel_id.replace(pathos_cl_master_channel.set_index('dim_val_id')['dim_value_name'])
    final_summary['channel'] = s
except:
    print('not available')


# In[91]:


#REPLACING CHANNEL NAME WITH CHANNEL ID
try:
    s = final_summary.country_id.replace(pathos_cl_master_country.set_index('dim_val_id')['dim_value_name'])
    final_summary['country'] = s
except:
    print('not available')


# In[92]:


# convert version_id from object data type to float dtype
final_summary['version_id'] = final_summary['version_id'].astype(float).round(3)


# In[93]:


#REPLACING CHANNEL NAME WITH CHANNEL ID
try:
    s = final_summary.version_id.replace(pathos_cl_master_prd_ver.set_index('dim_val_id')['dim_value_name'])
    final_summary['version'] = s
except:
    print('not available')


# In[94]:


final_summary.head(1)


# ### Push Summary Table to Postgres

# In[95]:


connection = pg.connect('postgresql://aidatabases:Aidatabases#@65.1.96.15:5432/pathos_db')
engine = create_engine('postgresql://aidatabases:Aidatabases#@65.1.96.15:5432/pathos_db')


# In[96]:


final_summary.columns


# In[97]:


null_columns = ['drivers_positivity_score', 'drivers_well_being_score',                
'drivers_convenience_score',               
'drivers_care_score'        ,              
'drivers_timeliness_score'   ,             
'drivers_knowledge_score'     ,            
'drivers_fairness_score'       ,           
'drivers_channel_satisfaction_score',      
'drivers_outcome_score'              ,     
'drivers_waiting_time_score'          ,    
'drivers_freshness_score'              ,   
'drivers_trust_score'                   ,  
'drivers_value_for_money_score'          , 
'drivers_packaging_score'                 ]


# In[98]:


final_summary = final_summary.dropna(subset=null_columns) 


# In[99]:


final_summary.isnull().sum()


# In[100]:


final_summary


# In[101]:


truncate_table(table_name= 'rb_summary_table', schema = 'pathos_rb_schema')

push_table_pgres(final_summary, df_name = 'rb_summary_table', schema= 'pathos_rb_schema')

# engine.execute('TRUNCATE pathos.pathos_cl_summary_table RESTART IDENTITY;')
# df_summary.to_sql('pathos_cl_summary_table', con=engine, index=False, if_exists= 'append', schema='pathos')


# In[102]:


x-x-x-x-x-x-x-x-x-x-x-x-x-x-x-x-x-x-x-x


# # Persona Dataframes

# In[ ]:


active_persona


# In[ ]:


# common_to_all = ['pathos_transaction_id', 'date', 'behaviour_score', 'emotions_surprised_score', 'emotions_happy_score', 
#                  'emotions_excited_score', 'emotions_sad_score', 'emotions_disgust_score', 'emotions_angry_score', 
#                  'emotions_fear_score', 'emotions_frustration_score', 'drivers_care_score', 
#                  'drivers_channel_satisfaction_score', 'drivers_convenience_score', 'drivers_ease_of_access_score', 
#                  'drivers_fairness_score', 'drivers_freshness_score', 'drivers_knowledge_score', 'drivers_outcome_score', 
#                  'drivers_packaging_score', 'drivers_positivity_score', 'drivers_timeliness_score', 'drivers_trust_score', 
#                  'drivers_value_for_money_score', 'drivers_waiting_time_score', 'drivers_well_being_score', 
#                  'positive_score_by_weights', 'negative_score_by_weights', 'behaviour_qual_positive_comments', 
#                  'behaviour_qual_negative_comments', 'drivers_score', 'emotions_score', 'emotional_engagement_score', 'people_score', 
#                  'process_score', 'product_score','intent_score', 'loyalty_score', 'values_score',  'version', 
#                  'n_emotional_engagement_score', 'emotional_engagement_quintile', 'all_pathos']


# In[ ]:


active_persona_list= list(active_persona.keys())
active_persona_values = list(active_persona.values())


# In[ ]:


active_persona_values


# In[ ]:


active_persona_list


# In[ ]:


for i in range(len(active_persona_list)):
    sample_var = active_persona_list[i]
    print(sample_var)
    vars()[sample_var]=  pd.concat([final_summary['date'],final_summary[active_persona_values[i]]], axis=1)


# In[ ]:


chief_marketing_officer.head(1)


# In[ ]:


chief_marketing_officer.head(1)


# In[ ]:


# for i in range(len(active_persona_list)):
#     sample_var = active_persona_list[i]
#     print(sample_var)
#     vars()[sample_var]= pd.concat([rb_final_file_model_processed[common_to_all], rb_final_file_model_processed[active_persona_values[i]]], axis=1)


# # Replacing names of brands, manufacturers, gmo, source, product back from Ids

# In[ ]:


pathos_cl_master_dim_mapping


# In[ ]:


from pathos_config import *


# In[ ]:


pathos_cl_master_dim_mapping = pathos_cl_master_dim_mapping.loc[pathos_cl_master_dim_mapping['dim_active_reporting']=='Y']


# In[ ]:


k=[]
l=[]
for i, j in zip(pathos_cl_master_dim_mapping['dim_sql'], pathos_cl_master_dim_mapping['dimension_desc']):
    k.append(j)
    l.append(i)
    print(k)
    print(l)

connection = pg.connect('postgresql://aidatabases:Aidatabases#@65.1.96.15:5432/pathos_db')
   
for i in range(len(k)):
    sample_var = k[i]
    #print(sample_var)
    vars()[sample_var] = psql.read_sql(l[i], connection)
    vars()[sample_var] = vars()[sample_var].loc[vars()[sample_var]['dim_active_reporting']=='Y']


# In[ ]:


Time


# In[ ]:


for i in range(len(active_persona_list)):
#REPLACING BRAND NAME WITH BRAND ID
    try:
        sample_var = active_persona_list[i]
        s = vars()[sample_var].brand_id.replace(Brand.set_index('dim_val_id')['dim_value_name'])
        vars()[sample_var]['brand'] = s
    except:
        print('not available')
#REPLACING MANUFACTURER NAME WITH MANUFACTURER ID
    try:
        sample_var = active_persona_list[i]
        s = vars()[sample_var].manufacturer_id.replace(Manufacturer.set_index('dim_val_id')['dim_value_name'])
        vars()[sample_var]['manufacturer'] = s
    except:
        print('not available')
#REPLACING PRODUCT NAME WITH PRODUCT ID
    try:
        sample_var = active_persona_list[i]
        s = vars()[sample_var].product_id.replace(pathos_cl_master_product.set_index('dim_val_id')['dim_value_name'])
        vars()[sample_var]['product'] = s
    except:
        print('not available')
#REPLACING GMO NAME WITH GMO ID
    try:
        sample_var = active_persona_list[i]
        s = vars()[sample_var].gmo_id.replace(pathos_cl_master_gmo.set_index('dim_val_id')['dim_value_name'])
        vars()[sample_var]['gmo'] = s
    except:
        print('not available')
#REPLACING SOURCE NAME WITH SOURCE ID
    try:
        sample_var = active_persona_list[i]
        s = vars()[sample_var].source_id.replace(pathos_cl_master_source.set_index('dim_val_id')['dim_value_name'])
        vars()[sample_var]['source'] = s
    except:
        print('not available')
#REPLACING SOURCE NAME WITH Country
    try:
        sample_var = active_persona_list[i]
        s = vars()[sample_var].country_id.replace(pathos_cl_master_country.set_index('dim_val_id')['dim_value_name'])
        vars()[sample_var]['country'] = s
    except:
        print('not available')
        
#REPLACING SOURCE NAME WITH SOURCE ID
    try:
        sample_var = active_persona_list[i]
        s = vars()[sample_var].channel_id.replace(pathos_cl_master_channel.set_index('dim_val_id')['dim_value_name'])
        vars()[sample_var]['channel'] = s
    except:
        print('not available')
        
#REPLACING SOURCE NAME WITH SOURCE ID
    try:
        sample_var = active_persona_list[i]
        s = vars()[sample_var].version_id.replace(pathos_cl_master_prd_ver.set_index('dim_val_id')['dim_value_name'])
        vars()[sample_var]['version'] = s
    except:
        print('not available')


# In[ ]:


pathos_cl_master_prd_ver


# In[ ]:


channel_leader.head(2)


# In[ ]:


connection = pg.connect('postgresql://aidatabases:Aidatabases#@65.1.96.15:5432/pathos_db')
engine = create_engine('postgresql://aidatabases:Aidatabases#@65.1.96.15:5432/pathos_db')

# truncate_table(table_name= 'chief_marketing_officer', schema = 'generic')
# truncate_table(table_name= 'marketing_tactical', schema = 'generic')
# truncate_table(table_name= 'customer_experience_leader', schema = 'generic')
# truncate_table(table_name= 'customer_experience_tactical', schema = 'generic')
# truncate_table(table_name= 'product_service_leader', schema = 'generic')
# truncate_table(table_name= 'channel_leader', schema = 'generic')
# truncate_table(table_name= 'strategy_leader_ceo', schema = 'generic')

push_table_pgres(chief_marketing_officer, df_name = 'chief_marketing_officer', schema= 'pathos_rb_schema')
push_table_pgres(marketing_tactical, df_name = 'marketing_tactical', schema= 'pathos_rb_schema')
push_table_pgres(customer_experience_leader, df_name = 'customer_experience_leader', schema= 'pathos_rb_schema')
push_table_pgres(customer_experience_tactical, df_name = 'customer_experience_tactical', schema= 'pathos_rb_schema')
push_table_pgres(product_service_leader, df_name = 'product_service_leader', schema= 'pathos_rb_schema')
push_table_pgres(channel_leader, df_name = 'channel_leader', schema= 'pathos_rb_schema')
push_table_pgres(strategy_leader_ceo, df_name = 'strategy_leader_ceo', schema= 'pathos_rb_schema')


# In[ ]:


product_service_leader.head(1)


# In[ ]:


import os
os.system('jupyter nbconvert --to python load_rb_summary.ipynb')


# In[ ]:





# In[ ]:




