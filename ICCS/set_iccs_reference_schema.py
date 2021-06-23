#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pathos_config import *

from dotenv import load_dotenv
load_dotenv()
import os
current_schema = os.environ.get("schema")
# In[2]:




# # Loading Schemas

# In[3]:


schema_final_string='''create SCHEMA IF NOT EXISTS {};
'''.format(current_schema)
engine.execute(schema_final_string)


# # Reference Schema

# ## pathos_ref_emotions

# In[4]:


schema_final_string='''create table  IF NOT EXISTS {}.pathos_ref_emotions(
		EMOTIONS_ID NUMERIC 					PRIMARY KEY,
		EMOTIONS 	VARCHAR(100) 				UNIQUE NOT NULL,
		EMOTION_WEIGHT 		DECIMAL,
		DIRECTIONAL VARCHAR(100),
		CREATED_ON 	TIMESTAMP WITH TIME ZONE 	DEFAULT CURRENT_TIMESTAMP NOT NULL,
		CREATED_BY 	VARCHAR(100)				NOT NULL DEFAULT 'System',
		UPDATED_ON 	TIMESTAMP WITH TIME ZONE 	DEFAULT CURRENT_TIMESTAMP NOT NULL,
		UPDATED_BY 	VARCHAR(100)				NOT NULL DEFAULT 'System'
);

'''.format(current_schema)
    
engine.execute(schema_final_string)


# ## pathos_ref_drivers_sectors_mapping

# In[5]:


schema_final_string='''create table  IF NOT EXISTS {}.pathos_ref_drivers_sectors_mapping(
        unique_id NUMERIC UNIQUE NOT NULL,
		SECTOR_ID 					NUMERIC 						,
		DRIVER_ID 					NUMERIC 						,        
		SECTOR_WEIGHT					NUMERIC 				NOT NULL,	
		SECTOR_REPORTING_FLAG					CHAR(1) 				,	
		CREATED_ON 	TIMESTAMP WITH TIME ZONE 	NOT NULL DEFAULT NOW(),
		CREATED_BY 	VARCHAR(100)				NOT NULL DEFAULT 'System',
		UPDATED_ON 	TIMESTAMP WITH TIME ZONE 	NOT NULL DEFAULT NOW(),
		UPDATED_BY 	VARCHAR(100)				NOT NULL DEFAULT 'System',
        PRIMARY KEY (SECTOR_ID, DRIVER_ID)
);
'''.format(current_schema)
    
engine.execute(schema_final_string)


# ## pathos_ref_sectors

# In[6]:


schema_final_string='''create table IF NOT EXISTS {}.pathos_ref_sectors(
		SECTOR_ID 			NUMERIC 					PRIMARY KEY,
		SECTORS 			VARCHAR(100) 				UNIQUE NOT NULL,
		CREATED_ON 	TIMESTAMP WITH TIME ZONE 	DEFAULT CURRENT_TIMESTAMP NOT NULL,
		CREATED_BY 	VARCHAR(100)				NOT NULL DEFAULT 'System',
		UPDATED_ON 	TIMESTAMP WITH TIME ZONE 	DEFAULT CURRENT_TIMESTAMP NOT NULL,
		UPDATED_BY 	VARCHAR(100)				NOT NULL DEFAULT 'System'
);
'''.format(current_schema)
    
engine.execute(schema_final_string)


# ## Pathos_ref_drivers

# In[7]:


schema_final_string='''create table  IF NOT EXISTS {}.pathos_ref_drivers(
		DRIVER_ID 					NUMERIC 						PRIMARY KEY,
		DRIVERS					VARCHAR(100) 				UNIQUE NOT NULL,	
		CUSTOMER_SEGMENT					VARCHAR(100) 				,	
		MARKETING_SEGMENT			VARCHAR(100) 					,	
		CREATED_ON 	TIMESTAMP WITH TIME ZONE 	NOT NULL DEFAULT NOW(),
		CREATED_BY 	VARCHAR(100)				NOT NULL DEFAULT 'System',
		UPDATED_ON 	TIMESTAMP WITH TIME ZONE 	NOT NULL DEFAULT NOW(),
		UPDATED_BY 	VARCHAR(100)				NOT NULL DEFAULT 'System'
);
'''.format(current_schema)
    
engine.execute(schema_final_string)


# ## PATHOS_REF_CLIENTS

# In[8]:


schema_final_string='''create table  IF NOT EXISTS {}.pathos_ref_clients(
		CLIENT_ID 			NUMERIC 					PRIMARY KEY,
		CLIENT_NAME 		VARCHAR(100) 				UNIQUE NOT NULL,
		CLIENT_SECTOR_ID 	NUMERIC 					NOT NULL,
		CLIENT_SECTOR_DESC 	VARCHAR(100) 				NOT NULL,
		CREATED_ON 			TIMESTAMP WITH TIME ZONE 	DEFAULT CURRENT_TIMESTAMP NOT NULL,
		CREATED_BY 			VARCHAR(100)				NOT NULL DEFAULT 'System',
		UPDATED_ON 			TIMESTAMP WITH TIME ZONE 	DEFAULT CURRENT_TIMESTAMP NOT NULL,
		UPDATED_BY 			VARCHAR(100)				NOT NULL DEFAULT 'System'
);
'''.format(current_schema) 
    
engine.execute(schema_final_string)


# # Query from Google Sheet

# ## Reference google sheet

# In[9]:


for i in range(len(ref_table_list)):
    sample_var1 = ref_table_list[i]
    sample_var2 = ref_table_list[i]
    print("Reference DataFrame: {}".format(sample_var1, sample_var2))
    vars()[sample_var2]= load_gsheet(ref_table_list[i], ref_table_list[i], sheet_id_ref)


# ## Reference tables

# In[10]:


# Truncate
try:
    for i in ref_table_list:
        print(i)
        truncate_table(i, current_schema)
except:
    print('not available: ' + i)


# In[ ]:


# Push
for i, j in zip(range(len(ref_table_list)), ref_table_list):
    sample_var = ref_table_list[i]
    print(sample_var)
    push_table_pgres(df = vars()[sample_var],df_name = j, schema= current_schema)


# In[ ]:


#import os
#os.system('jupyter nbconvert --to python pathos_load_common_reference.ipynb')


# In[ ]:


