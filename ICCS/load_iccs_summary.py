#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pathos_config import *


# In[2]:

from dotenv import load_dotenv
load_dotenv()
import os
dim_schema = os.environ.get("schema")
# In[ ]:


final_transaction_table = load_data('load_iccs_transaction_cf7_cf8', dim_schema, connection)
#final_transaction_table.dropna(axis=1, how='all', inplace=True)
print('completed')
cf20_transaction_table = load_data('load_iccs_transaction_cf20', dim_schema, connection)
#from_ml_team = load_data('rb_from_ml_team', 'pathos_rb_schema', connection)


# In[ ]:


# final_transaction_table = load_data('iccs_transaction_table', dim_schema, connection)

# print('completed')
# cf20_transaction_table = load_data('iccs_transaction_table_cf20', dim_schema, connection)


# In[ ]:


final_transaction_table.drop(["drivers_personalization_score",
"drivers_positivity_score",
"drivers_well_being_score",
"drivers_exciting_score",
"drivers_belonging_score",
"drivers_sustainability_score",
"drivers_security_score",
"drivers_convenience_score",
"drivers_care_score",
"drivers_timeliness_score",
"drivers_respect_score",
"drivers_knowledge_score",
"drivers_fairness_score",
"drivers_channel_satisfaction_score",
"drivers_outcome_score",
"drivers_communication_score",
"drivers_ease_of_access_score",
"drivers_future_issues_score",
"drivers_competence_score",
"drivers_extra_mile_score",
"drivers_waiting_time_score",
"drivers_appeal_score",
"drivers_preferred_chanel_score",
"drivers_freshness_score",
"drivers_trust_score",
"drivers_regret_score",
"drivers_relationship_score",
"drivers_innovative_score",
"drivers_value_for_money_score",
"drivers_packaging_score",
"emotions_happy_score",
"emotions_surprised_score",
"emotions_excited_score",
"emotions_sad_score",
"emotions_fear_score",
"emotions_frustration_score",
"emotions_angry_score",
"emotions_disgust_score"], axis=1, inplace=True) 


# In[ ]:


# df.drop_duplicates(subset=['A', 'C'], keep=False)


# In[ ]:


cf7_verbatim = psql.read_sql('SELECT * FROM {}.cf7_verbatim'.format(dim_schema), connection)
cf8_verbatim = psql.read_sql('SELECT * FROM {}.cf8_verbatim'.format(dim_schema), connection)


# In[ ]:


cf7_verbatim.rename(columns={'region':'jurisdiction'}, inplace=True)


# In[ ]:





# In[ ]:


cf7_verbatim.rename(columns={"personalization":"drivers_personalization_score",
"positivity":"drivers_positivity_score",
"well_being":"drivers_well_being_score",
"exciting":"drivers_exciting_score",
"belonging":"drivers_belonging_score",
"sustainability":"drivers_sustainability_score",
"security":"drivers_security_score",
"convenience":"drivers_convenience_score",
"care":"drivers_care_score",
"timeliness":"drivers_timeliness_score",
"respect":"drivers_respect_score",
"knowledge":"drivers_knowledge_score",
"fairness":"drivers_fairness_score",
"channel_satisfaction":"drivers_channel_satisfaction_score",
"outcome":"drivers_outcome_score",
"communication":"drivers_communication_score",
"ease_of_access":"drivers_ease_of_access_score",
"future_issues":"drivers_future_issues_score",
"competence":"drivers_competence_score",
"extra_mile":"drivers_extra_mile_score",
"waiting_time":"drivers_waiting_time_score",
"appeal":"drivers_appeal_score",
"preferred_chanel":"drivers_preferred_chanel_score",
"freshness":"drivers_freshness_score",
"trust":"drivers_trust_score",
"regret":"drivers_regret_score",
"relationship":"drivers_relationship_score",
"innovative":"drivers_innovative_score",
"value_for_money":"drivers_value_for_money_score",
"packaging":"drivers_packaging_score"}, inplace=True)


# In[ ]:


cf7_verbatim = cf7_verbatim.rename(columns={"happy":"emotions_happy_score",
"surprised":"emotions_surprised_score",
"excited":"emotions_excited_score",
"neutral":"emotions_neutral_score",
"sad":"emotions_sad_score",
"fear":"emotions_fear_score",
"frustration":"emotions_frustration_score",
"angry":"emotions_angry_score",
"disgust":"emotions_disgust_score"})


# In[ ]:





# In[ ]:





# In[ ]:


cf8_verbatim.rename(columns={"personalization":"drivers_personalization_score",
"positivity":"drivers_positivity_score",
"well_being":"drivers_well_being_score",
"exciting":"drivers_exciting_score",
"belonging":"drivers_belonging_score",
"sustainability":"drivers_sustainability_score",
"security":"drivers_security_score",
"convenience":"drivers_convenience_score",
"care":"drivers_care_score",
"timeliness":"drivers_timeliness_score",
"respect":"drivers_respect_score",
"knowledge":"drivers_knowledge_score",
"fairness":"drivers_fairness_score",
"channel_satisfaction":"drivers_channel_satisfaction_score",
"outcome":"drivers_outcome_score",
"communication":"drivers_communication_score",
"ease_of_access":"drivers_ease_of_access_score",
"future_issues":"drivers_future_issues_score",
"competence":"drivers_competence_score",
"extra_mile":"drivers_extra_mile_score",
"waiting_time":"drivers_waiting_time_score",
"appeal":"drivers_appeal_score",
"preferred_chanel":"drivers_preferred_chanel_score",
"freshness":"drivers_freshness_score",
"trust":"drivers_trust_score",
"regret":"drivers_regret_score",
"relationship":"drivers_relationship_score",
"innovative":"drivers_innovative_score",
"value_for_money":"drivers_value_for_money_score",
"packaging":"drivers_packaging_score"}, inplace=True)


# In[ ]:


cf8_verbatim = cf8_verbatim.rename(columns={"happy":"emotions_happy_score",
"surprised":"emotions_surprised_score",
"excited":"emotions_excited_score",
"neutral":"emotions_neutral_score",
"sad":"emotions_sad_score",
"fear":"emotions_fear_score",
"frustration":"emotions_frustration_score",
"angry":"emotions_angry_score",
"disgust":"emotions_disgust_score"})


# In[ ]:


cf8_verbatim = cf8_verbatim.rename(columns={'serialid': 'id'})
cf8_merged = pd.merge(final_transaction_table, cf8_verbatim, on="id")


# In[ ]:


iso_canada = load_gsheet('iso_canada', 'iso_canada', sheet_id_iccs)


# In[ ]:


cf7_verbatim = cf7_verbatim.rename(columns={'serialid': 'id'})


# In[ ]:


cf7_merged = pd.merge(final_transaction_table, cf7_verbatim, on="id")


# In[ ]:


final_summary = pd.concat([cf8_merged,cf7_merged], axis=0, ignore_index=True)


# In[ ]:


final_summary = pd.concat([final_summary, cf20_transaction_table])


# # Filtering PERSONAS

# ### Loading Tables from Dimension and reference Schemas

# In[ ]:


### dimension schema

for i in range(len(dim_table_list_iccs)):
    sample_var = dim_table_list_iccs[i]
    print(sample_var)
    vars()[sample_var]= load_data(dim_table_list_iccs[i], dim_schema, connection)
    
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

# In[ ]:


try:
    pathos_cl_master_personas['new'] = pathos_cl_master_personas.apply(lambda x: x.index[x == "Y"].tolist(), axis=1)
except:
    print('not available')


# In[ ]:


p=[]
q=[]
for i, j in zip(pathos_cl_master_personas['persona'], pathos_cl_master_personas['new']):
    p.append(i)
    q.append(j)


# ### ACTIVE PERSONA LIST

# In[ ]:


active_persona = dict(zip(p, q))


# ## Client Filter Hard-Coded
# 

# In[ ]:


client = 'ICCS'
client_id = pathos_ref_clients.loc[pathos_ref_clients['client_name'] == client].client_id.iloc[0]
client_dict = pd.Series(pathos_ref_clients.client_sector_desc.values,index=pathos_ref_clients.client_id).to_dict()


# ## Calculating Driver Score based on current Client Sector

# In[ ]:


sector = client_dict.get(client_id)
sector_id = pathos_ref_sectors.loc[pathos_ref_sectors['sectors'] == sector].sector_id.iloc[0]
df_sector_weight = pathos_ref_drivers_sectors_mapping.loc[pathos_ref_drivers_sectors_mapping["sector_id"] == sector_id]
#dictionary of driver_id and sector_weight
sector_wt_dict = pd.Series(df_sector_weight.sector_weight.values,index=df_sector_weight.driver_id).to_dict()


# ## Emotion_score

# ### rename column name with emotion_ids

# In[ ]:


final_summary.rename(columns=dict(zip(pathos_ref_emotions["emotions"], pathos_ref_emotions["emotions_id"])), inplace=True)


# In[ ]:


#sector_id 
emotion_id_list = list(pathos_ref_emotions['emotions_id'])

#taking common driver_id from sector dataframe and main dataframe
common_emotions = list(set(emotion_id_list).intersection(final_summary.columns))


# In[ ]:


# dataframe containing only common drivers
df_emotions = final_summary[common_emotions]


# In[ ]:


#dictionary of driver_id and sector_weight
emotion_wt_dict = pd.Series(pathos_ref_emotions.emotion_weight.values,index=pathos_ref_emotions.emotions_id).to_dict()


# In[ ]:


#multiplying emotions with weight and summation
emotion_score = (pd.Series(emotion_wt_dict)*df_emotions).sum(1)


# In[ ]:


final_summary['emotion_score'] = pd.DataFrame(emotion_score)


# ## Driver Score

# ### rename column name with driver_ids

# In[ ]:


final_summary.rename(columns=dict(zip(pathos_ref_drivers["drivers"], pathos_ref_drivers["driver_id"])), inplace=True)


# In[ ]:


#sector_id 
sector_id_list = list(sector_wt_dict.keys())

#taking common driver_id from sector dataframe and main dataframe
common_driver = list(set(sector_id_list).intersection(final_summary.columns))


# In[ ]:


# dataframe containing only common drivers
df_drivers = final_summary[common_driver]


# In[ ]:


final_summary['drivers_summation']=df_drivers.sum(axis=1)


# In[ ]:


for i in df_drivers.columns:
    df_drivers[i]= (df_drivers[i]*final_summary['emotion_score'])/final_summary['drivers_summation']


# In[ ]:


df_drivers = (pd.Series(sector_wt_dict)*df_drivers)


# In[ ]:


### Only common drivers (no null drivers)


# In[ ]:


df_drivers = df_drivers[common_driver]


# In[ ]:


final_summary.jurisdiction.replace(['Region of Peel', 'Toronto'], ['Peel', 'City of Toronto'], inplace=True)


# In[ ]:


#multiplying drivers with weight and summation
driver_score = (pd.Series(sector_wt_dict)*df_drivers).sum(1)


# In[ ]:


final_summary['driver_score'] = pd.DataFrame(driver_score)


# ### Dropping old drivers scores

# In[ ]:


final_summary.drop(['drivers_summation', 20001,20002,20003,20004,20005,20006,20007,20008,20009,
                    20010,20011,20012,20013,20014,20015,20016,20017,20018,20019,20020,20021,20022,
                    20023,20024,20025,20026,20027,20028,20029,20030], axis=1, inplace=True,errors='ignore' )


# ### Adding updated drivers value

# In[ ]:


final_summary = pd.concat([final_summary, df_drivers], axis=1)


# ## Emotional Engagement

# In[ ]:


final_summary['emotional_engagement']= (final_summary.driver_score * final_summary.emotion_score * final_summary.pos_score) + (final_summary.driver_score * final_summary.emotion_score * final_summary.neg_score)


# # Customer_segment

# In[ ]:


df = pathos_ref_drivers.groupby(['customer_segment', 'driver_id'], as_index=False).count()


# In[ ]:


# list of driver_id on various customer segments
people_list = list(df.loc[df['customer_segment'] == "people"].driver_id)
process_list = list(df.loc[df['customer_segment'] == "process"].driver_id)
product_list = list(df.loc[df['customer_segment'] == "product"].driver_id)
product_service_list = list(df.loc[df['customer_segment'] == "product/service"].driver_id)


# In[ ]:


product_service_list = product_list + product_service_list


# ## Adding customer segment

# In[ ]:


#taking common driver_id from sector dataframe and main dataframe
common_people = list(set(people_list).intersection(final_summary.columns))
# dataframe containing only common drivers
df_people = final_summary[common_people]
#multiplying with weight and summation
people_score = (pd.Series(sector_wt_dict)*df_people).sum(1)
final_summary['people_score'] = pd.DataFrame(people_score)


# In[ ]:


#taking common driver_id from sector dataframe and main dataframe
common_process = list(set(process_list).intersection(final_summary.columns))
# dataframe containing only common drivers
df_process = final_summary[common_process]
#multiplying with weight and summation
process_score = (pd.Series(sector_wt_dict)*df_process).sum(1)
final_summary['process_score'] = pd.DataFrame(process_score)


# In[ ]:


#taking common driver_id from sector dataframe and main dataframe
common_product = list(set(product_service_list).intersection(final_summary.columns))
# dataframe containing only common drivers
df_product = final_summary[common_product]
#multiplying with weight and summation
product_score = (pd.Series(sector_wt_dict)*df_product).sum(1)
final_summary['product_score'] = pd.DataFrame(product_score)


# # Marketing_segment

# In[ ]:


df = pathos_ref_drivers.groupby(['marketing_segment', 'driver_id'], as_index=False).count()


# In[ ]:


# list of driver_id on various marketing segments
intent_list = list(df.loc[df['marketing_segment'] == "intent"].driver_id)
loyalty_list = list(df.loc[df['marketing_segment'] == "loyalty"].driver_id)
values_list = list(df.loc[df['marketing_segment'] == "values"].driver_id)


# ## Adding customer marketing segment

# In[ ]:


#taking common driver_id from sector dataframe and main dataframe
common_intent = list(set(intent_list).intersection(final_summary.columns))
# dataframe containing only common drivers
df_intent = final_summary[common_intent]
#multiplying with weight and summation
intent_score = (pd.Series(sector_wt_dict)*df_intent).sum(1)
final_summary['intent_score'] = pd.DataFrame(intent_score)


# In[ ]:


#taking common driver_id from sector dataframe and main dataframe
common_loyalty = list(set(loyalty_list).intersection(final_summary.columns))
# dataframe containing only common drivers
df_loyalty = final_summary[common_loyalty]
#multiplying with weight and summation
loyalty_score = (pd.Series(sector_wt_dict)*df_loyalty).sum(1)
final_summary['loyalty_score'] = pd.DataFrame(loyalty_score)


# In[ ]:


#taking common driver_id from sector dataframe and main dataframe
common_values = list(set(values_list).intersection(final_summary.columns))
# dataframe containing only common drivers
df_values = final_summary[common_values]
#multiplying with weight and summation
values_score = (pd.Series(sector_wt_dict)*df_values).sum(1)
final_summary['values_score'] = pd.DataFrame(values_score)


# In[ ]:


final_summary.drop(['behaviour_qual_positive_comments', 'behaviour_qual_negative_comments', 'location'], axis=1, inplace=True, errors='ignore')


# # renaming column names to match with schema

# In[ ]:


rename_dict = {'matched_positive_keywords':'behaviour_qual_positive_comments',
'matched_negative_keywords':'behaviour_qual_negative_comments',
'BRAND_ID': 'brand_id',
'MANUFACTURER_ID': 'manufacturer_id',
'Id':'pathos_summary_id',
'behavior_score': 'behaviour_score', 
'driver_score':'drivers_score',
'emotion_score': 'emotions_score',
'emotional_engagement': 'emotional_engagement_score',
'Channel':'channel',
'Country':'country',
'jurisdiction': 'location',
               
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
'Predicted Driver': 'predicted_driver'
}


# In[ ]:


final_summary.rename(columns=rename_dict, inplace=True)


# In[ ]:


#final_summary.dropna(axis=1, how='all', inplace=True)
final_summary.shape


# In[ ]:


final_summary.columns = map(str.lower, final_summary.columns)


# # Adding predict emotion first element

# In[ ]:


def Extract(lst): 
    return [item[0] for item in lst] 


# In[ ]:


# Adding predicted driver first element


# In[ ]:


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


# In[ ]:


ppp_dict = pd.Series(pathos_ref_drivers.customer_segment.values,index=pathos_ref_drivers.drivers).to_dict()
ilv_dict = pd.Series(pathos_ref_drivers.marketing_segment.values,index=pathos_ref_drivers.drivers).to_dict()


# In[ ]:


final_summary['predicted_driver_ppp'] = final_summary['predicted_driver'].map(ppp_dict)


# In[ ]:


final_summary['predicted_driver_ilv'] = final_summary['predicted_driver'].map(ilv_dict)


# # Addidng Canada ISO for province

# In[ ]:


iso_dict = pd.Series(iso_canada.code.values,index=iso_canada.province).to_dict()


# In[ ]:


final_summary['location_id'] = final_summary.location_id.astype(str)


# In[ ]:


try:
    s = final_summary.location.replace(pathos_cl_master_country.set_index('dim_value_name')['dim_val_id'])
    final_summary['location_id_cf78'] = s
except:
    print('not available')


# In[ ]:


final_summary['location_id'] = final_summary['location_id'].fillna(value='')
final_summary['location_id'] = final_summary['location_id'].replace({'nan':''})
final_summary['location_id'] = final_summary['location_id'].replace({'None':''})


# In[ ]:


final_summary['location_id_cf78'] = final_summary['location_id_cf78'].fillna(value='')


# In[ ]:


final_summary['location_id'] = final_summary['location_id'].astype(str) + final_summary['location_id_cf78'].astype(str)


# # location

# In[ ]:


final_summary['location_id'] = final_summary['location_id'].astype(float)


# In[ ]:


try:
    s = final_summary.location_id.replace(pathos_cl_master_country.set_index('dim_val_id')['dim_value_name'])
    final_summary['location'] = s
except:
    print('not available')


# In[ ]:


final_summary['iso_code'] = final_summary['location'].map(iso_dict)


# ## Dropping unnecessary columns

# In[ ]:


final_summary.drop(['id','pathos_transaction_id','services','service_id','predicted emotion', 'location_id_cf78'], axis=1, inplace=True)


# In[ ]:


final_summary.reset_index(inplace=True)


# In[ ]:


final_summary.drop(columns=['index'], inplace=True)


# # Feature Engineering

# ## Creating 'all_pathos' column where all values are 'ALL'

# In[ ]:


final_summary['all_pathos']= 'Overall'


# ## Normalization

# In[ ]:


final_summary = final_summary.groupby(level=0, axis=1).last()


# In[ ]:


import pandas as pd
from sklearn.preprocessing import MinMaxScaler

scaler = MinMaxScaler(feature_range=(0, 100), clip=True)
final_summary[['n_emotional_engagement_score']] = scaler.fit_transform(final_summary[['emotional_engagement_score']])


# ## Quintiles

# In[ ]:


final_summary['emotional_engagement_quintile']= np.nan


# In[ ]:


final_summary['emotional_engagement_quintile'] = np.where(final_summary["n_emotional_engagement_score"] > 75, "Highly Engaged", final_summary["emotional_engagement_quintile"])


# In[ ]:


final_summary['emotional_engagement_quintile'] = np.where(final_summary["n_emotional_engagement_score"] < 25, "Not Engaged", final_summary["emotional_engagement_quintile"])


# In[ ]:


final_summary['emotional_engagement_quintile'] = np.where((final_summary["n_emotional_engagement_score"]<=50) & (final_summary["n_emotional_engagement_score"]>25), "Somewhat Engaged", final_summary["emotional_engagement_quintile"])


# In[ ]:


final_summary['emotional_engagement_quintile'] = np.where((final_summary["n_emotional_engagement_score"]>50) & (final_summary["n_emotional_engagement_score"]<=75), "Engaged", final_summary["emotional_engagement_quintile"])


# In[ ]:


final_summary[['emotional_engagement_score','n_emotional_engagement_score', 'emotional_engagement_quintile']]


# # Summary Table

# In[ ]:


final_summary['pathos_summary_id']=list(range(1, final_summary.shape[0]+1))


# # Adding location_id

# In[ ]:


#REPLACING BRAND NAME WITH BRAND ID
try:
    s = final_summary.location.replace(pathos_cl_master_country.set_index('dim_value_name')['dim_val_id'])
    final_summary['location_id'] = s
except:
    print('not available')


# # ADDING NAME OF DIMENSIONS

# In[ ]:


final_summary[['age_id', 'gender_id', 'occupation_id', 'income_id', 'channel_id', 'time_id', 'education_id']] = final_summary[['age_id', 'gender_id', 'occupation_id', 'income_id', 'channel_id', 'time_id', 'education_id']].astype('float')


# In[ ]:


#REPLACING BRAND NAME WITH BRAND ID
try:
    s = final_summary.education_id.replace(pathos_cl_master_education.set_index('dim_val_id')['dim_value_desc'])
    final_summary['education'] = s
except:
    print('not available')


# In[ ]:


#REPLACING BRAND NAME WITH BRAND ID
try:
    s = final_summary.age_id.replace(pathos_cl_master_age.set_index('dim_val_id')['dim_value_desc'])
    final_summary['age'] = s
except:
    print('not available')


# In[ ]:


#REPLACING MANUFACTURER NAME WITH MANUFACTURER ID
try:
    s = final_summary.gender_id.replace(pathos_cl_master_gender.set_index('dim_val_id')['dim_value_desc'])
    final_summary['gender'] = s
except:
    print('not available')


# In[ ]:


#REPLACING PRODUCT NAME WITH PRODUCT ID
try:
    s = final_summary.occupation_id.replace(pathos_cl_master_occupation.set_index('dim_val_id')['dim_value_desc'])
    final_summary['occupation'] = s
except:
    print('not available')


# In[ ]:


#REPLACING SOURCE NAME WITH SOURCE ID
try:
    s = final_summary.income_id.replace(pathos_cl_master_income.set_index('dim_val_id')['dim_value_desc'])
    final_summary['income'] = s
except:
    print('not available')


# In[ ]:


#REPLACING CHANNEL NAME WITH CHANNEL ID
try:
    s = final_summary.channel_id.replace(pathos_cl_master_channel.set_index('dim_val_id')['dim_value_name'])
    final_summary['channel'] = s
except:
    print('not available')


# In[ ]:


#REPLACING CHANNEL NAME WITH CHANNEL ID
try:
    s = final_summary.time_id.replace(pathos_cl_master_time.set_index('dim_val_id')['dim_value_name'])
    final_summary['year'] = s
except:
    print('not available')


# ### Push Summary Table to Postgres

# In[ ]:


try:
    truncate_table(table_name= 'iccs_summary_table_with_cf20', schema = 'pathos_iccs_schema')
except:
    print('not available')


# In[ ]:


final_summary.drop(['date'], axis=1, inplace=True, errors='ignore')


# In[ ]:


final_summary['date']= pd.to_datetime(final_summary.year, format='%Y')
final_summary.drop(columns=['year'], inplace=True)


# In[ ]:


connection = pg.connect('postgresql://aidatabases:Aidatabases#@34.93.95.15:5432/pathos_db')
engine = create_engine('postgresql://aidatabases:Aidatabases#@34.93.95.15:5432/pathos_db')


# In[ ]:


push_table_pgres(final_summary, df_name = 'load_iccs_summary', schema= dim_schema)


# # Persona Dataframes

# In[ ]:


active_persona


# In[ ]:


active_persona_list= list(active_persona.keys())
active_persona_values = list(active_persona.values())


# In[ ]:


for i in range(len(active_persona_list)):
    sample_var = active_persona_list[i]
    print(sample_var)
    vars()[sample_var]= (final_summary[active_persona_values[i]])


# # Replacing names of brands, manufacturers, gmo, source, product back from Ids

# In[ ]:


pathos_cl_master_dim_mapping = pathos_cl_master_dim_mapping.loc[pathos_cl_master_dim_mapping['dim_active_reporting']=='Y']


# In[ ]:


pd.set_option('display.float_format', lambda x: '%.3f' % x) #to supress scientific notation
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)


# In[ ]:


# for i in pathos_cl_master_dim_mapping.dim_sql:
#     print(i)


# In[ ]:


k=[]
l=[]
for i, j in zip(pathos_cl_master_dim_mapping['dim_sql'], pathos_cl_master_dim_mapping['dimension_desc']):
    k.append(j)
    l.append(i)
    print(k)
    print(l)

connection = pg.connect('postgresql://aidatabases:Aidatabases#@34.93.95.15:5432/pathos_db')
   
for i in range(len(k)):
    sample_var = k[i]
    #print(sample_var)
    vars()[sample_var] = psql.read_sql(l[i], connection)
    vars()[sample_var] = vars()[sample_var].loc[vars()[sample_var]['dim_active_reporting']=='Y']


# In[ ]:


for i in range(len(active_persona_list)):
#REPLACING BRAND NAME WITH BRAND ID
    try:
        sample_var = active_persona_list[i]
        s = vars()[sample_var].time_id.replace(pathos_cl_master_time.set_index('dim_val_id')['dim_value_name'])
        vars()[sample_var]['year'] = s
    except:
        print('not available: ' + sample_var )
#REPLACING MANUFACTURER NAME WITH MANUFACTURER ID
    try:
        sample_var = active_persona_list[i]
        s = vars()[sample_var].channel_id.replace(pathos_cl_master_channel.set_index('dim_val_id')['dim_value_name'])
        vars()[sample_var]['channel'] = s
    except:
        print('not available: ' + sample_var )
#REPLACING PRODUCT NAME WITH PRODUCT ID
    try:
        sample_var = active_persona_list[i]
        s = vars()[sample_var].location_id.replace(pathos_cl_master_country.set_index('dim_val_id')['dim_value_name'])
        vars()[sample_var]['location'] = s
    except:
        print('not available: ' + sample_var )
#REPLACING GMO NAME WITH GMO ID
    try:
        sample_var = active_persona_list[i]
        s = vars()[sample_var].age_id.replace(pathos_cl_master_age.set_index('dim_val_id')['dim_value_desc'])
        vars()[sample_var]['age'] = s
    except:
        print('not available: ' + sample_var )
#REPLACING SOURCE NAME WITH SOURCE ID
    try:
        sample_var = active_persona_list[i]
        s = vars()[sample_var].gender_id.replace(pathos_cl_master_gender.set_index('dim_val_id')['dim_value_desc'])
        vars()[sample_var]['gender'] = s
    except:
        print('not available: ' + sample_var )
#REPLACING SOURCE NAME WITH Country
    try:
        sample_var = active_persona_list[i]
        s = vars()[sample_var].occupation_id.replace(pathos_cl_master_occupation.set_index('dim_val_id')['dim_value_desc'])
        vars()[sample_var]['occupation'] = s
    except:
        print('not available: ' + sample_var )
        
#REPLACING SOURCE NAME WITH SOURCE ID
    try:
        sample_var = active_persona_list[i]
        s = vars()[sample_var].income_id.replace(pathos_cl_master_income.set_index('dim_val_id')['dim_value_desc'])
        vars()[sample_var]['income'] = s
    except:
        print('not available: ' + sample_var )
        
#REPLACING SOURCE NAME WITH SOURCE ID
    try:
        sample_var = active_persona_list[i]
        s = vars()[sample_var].version_id.replace(pathos_cl_master_prod_serv_cf7.set_index('dim_val_id')['dim_value_name'])
        vars()[sample_var]['cf7_prod'] = s
    except:
        print('not available: ' + sample_var + ' cf7_prod' )
        
    try:
        sample_var = active_persona_list[i]
        s = vars()[sample_var].version_id.replace(pathos_cl_master_prod_serv_cf8h.set_index('dim_val_id')['dim_value_name'])
        vars()[sample_var]['cf8_prod'] = s
    except:
        print('not available: ' + sample_var + ' cf8_prod')


# In[ ]:


connection = pg.connect('postgresql://aidatabases:Aidatabases#@34.93.95.15:5432/pathos_db')
engine = create_engine('postgresql://aidatabases:Aidatabases#@34.93.95.15:5432/pathos_db')

# truncate_table(table_name= 'chief_marketing_officer', schema = 'generic')
# truncate_table(table_name= 'marketing_tactical', schema = 'generic')
# truncate_table(table_name= 'customer_experience_leader', schema = 'generic')
# truncate_table(table_name= 'customer_experience_tactical', schema = 'generic')
# truncate_table(table_name= 'product_service_leader', schema = 'generic')
# truncate_table(table_name= 'channel_leader', schema = 'generic')
# truncate_table(table_name= 'strategy_leader_ceo', schema = 'generic')

push_table_pgres(chief_marketing_officer, df_name = 'chief_marketing_officer', schema= dim_schema)
print('chief_marketing_officer')
# push_table_pgres(marketing_tactical, df_name = 'marketing_tactical', schema= 'pathos_iccs_schema')
# print('marketing_tactical')
push_table_pgres(customer_experience_leader, df_name = 'customer_experience_leader', schema= dim_schema)
print('customer_experience_leader')
# push_table_pgres(customer_experience_tactical, df_name = 'customer_experience_tactical', schema= 'pathos_iccs_schema')
# print('customer_experience_tactical')
push_table_pgres(product_service_leader, df_name = 'product_service_leader', schema= dim_schema)
print('product_service_leader')
push_table_pgres(channel_leader, df_name = 'channel_leader', schema= dim_schema)
print('channel_leader')
# push_table_pgres(strategy_leader_ceo, df_name = 'strategy_leader_ceo', schema= 'pathos_iccs_schema')
# print('strategy_leader_ceo')


# In[1]:


#import os
#os.system('jupyter nbconvert --to python load_iccs_summary.ipynb')


# In[ ]:




