#!/usr/bin/env python
# coding: utf-8

# In[1]:
from dotenv import load_dotenv
load_dotenv()
import os
current_schema = os.environ.get("schema")

from pathos_config import *


# # Loading Schemas

# In[2]:




# In[3]:


schema_final_string='''create SCHEMA IF NOT EXISTS {};
'''.format(current_schema)  
engine.execute(schema_final_string)


# # Dimension Schema

# ## PATHOS_CL_MASTER_DIM_MAPPING

# In[4]:


schema_final_string='''create table  IF NOT EXISTS {}.PATHOS_CL_MASTER_DIM_MAPPING(
		DIMENSION_ID 					NUMERIC 					PRIMARY KEY,
		DIMENSION_CODE					VARCHAR(100) 				UNIQUE NOT NULL,	
		DIMENSION_DESC					VARCHAR(100) 				,	
		DIM_ACTIVE_REPORTING			CHAR(1) 					NOT NULL,	
		DIM_USE_FOR_SUMMARY				CHAR(1) 					NOT NULL,		
		DIM_SEQUENCE					NUMERIC						NOT NULL,			
		DIMENSION_DATA_TABLE			VARCHAR(100) 				UNIQUE NOT NULL,				
		DIM_SQL							TEXT,	
		CREATED_ON 	TIMESTAMP WITH TIME ZONE 	DEFAULT CURRENT_TIMESTAMP NOT NULL,
		CREATED_BY 	VARCHAR(100)				NOT NULL DEFAULT 'System',
		UPDATED_ON 	TIMESTAMP WITH TIME ZONE 	DEFAULT CURRENT_TIMESTAMP NOT NULL,
		UPDATED_BY 	VARCHAR(100)				NOT NULL DEFAULT 'System'
);
'''.format(current_schema)
    
engine.execute(schema_final_string)


# ## PATHOS_CL_MASTER_TIME

# In[5]:


schema_final_string='''create table  IF NOT EXISTS {}.PATHOS_CL_MASTER_TIME(
		DIM_VAL_ID 					NUMERIC 						PRIMARY KEY,
		DIM_VALUE_NAME					VARCHAR(100) 				UNIQUE NOT NULL,	
		DIM_VALUE_DESC					VARCHAR(100) 				,	
		DIM_ACTIVE_REPORTING			CHAR(1) 					NOT NULL,	
		DIM_ACTIVE_PROCESSSING			CHAR(1) 					,		
		CHILD_RECORD					CHAR(1) 					,		
		PARENT_RECORD_ID 				NUMERIC						DEFAULT NULL, 			
		HIERARCHY_LEVEL					NUMERIC						NOT NULL,			
		CREATED_ON 	TIMESTAMP WITH TIME ZONE 	DEFAULT CURRENT_TIMESTAMP NOT NULL,
		CREATED_BY 	VARCHAR(100)				NOT NULL DEFAULT 'System',
		UPDATED_ON 	TIMESTAMP WITH TIME ZONE 	DEFAULT CURRENT_TIMESTAMP NOT NULL,
		UPDATED_BY 	VARCHAR(100)				NOT NULL DEFAULT 'System'
);
'''.format(current_schema)
    
engine.execute(schema_final_string)


# ## PATHOS_CL_MASTER_TIME_NEW

# In[6]:


schema_final_string='''create table  IF NOT EXISTS {}.PATHOS_CL_MASTER_TIME_NEW(
		DIM_VAL_ID 					NUMERIC 						PRIMARY KEY,
		DIM_VALUE_NAME					VARCHAR(100) 				UNIQUE NOT NULL,	
		DIM_VALUE_DESC					VARCHAR(100) 				,	
		DIM_ACTIVE_REPORTING			CHAR(1) 					NOT NULL,	
		DIM_ACTIVE_PROCESSSING			CHAR(1) 					,		
		CHILD_RECORD					CHAR(1) 					,		
		PARENT_RECORD_ID 				NUMERIC						DEFAULT NULL, 			
		HIERARCHY_LEVEL					NUMERIC						NOT NULL,			
		CREATED_ON 	TIMESTAMP WITH TIME ZONE 	DEFAULT CURRENT_TIMESTAMP NOT NULL,
		CREATED_BY 	VARCHAR(100)				NOT NULL DEFAULT 'System',
		UPDATED_ON 	TIMESTAMP WITH TIME ZONE 	DEFAULT CURRENT_TIMESTAMP NOT NULL,
		UPDATED_BY 	VARCHAR(100)				NOT NULL DEFAULT 'System'
);
'''.format(current_schema)
    
engine.execute(schema_final_string)


# ## pathos_cl_master_prod_serv_cf8

# In[7]:


schema_final_string='''create table  IF NOT EXISTS {}.pathos_cl_master_prod_serv_cf8(
		DIM_VAL_ID 					NUMERIC 						PRIMARY KEY,
		DIM_VALUE_NAME					VARCHAR(100) 				UNIQUE NOT NULL,	
		DIM_VALUE_DESC					TEXT 				,	
		DIM_ACTIVE_REPORTING			CHAR(1) 					,	
		DIM_ACTIVE_PROCESSSING			CHAR(1) 					,		
		CHILD_RECORD					CHAR(1) 					,		
		PARENT_RECORD_ID 				NUMERIC						DEFAULT NULL, 			
		HIERARCHY_LEVEL					NUMERIC						,			
        cat_mpi TEXT,
        category TEXT,
		CREATED_ON 	TIMESTAMP WITH TIME ZONE 	DEFAULT CURRENT_TIMESTAMP NOT NULL,
		CREATED_BY 	VARCHAR(100)				NOT NULL DEFAULT 'System',
		UPDATED_ON 	TIMESTAMP WITH TIME ZONE 	DEFAULT CURRENT_TIMESTAMP NOT NULL,
		UPDATED_BY 	VARCHAR(100)				NOT NULL DEFAULT 'System'
);
'''.format(current_schema)
    
engine.execute(schema_final_string)


# ## pathos_cl_master_prod_serv_cf7

# In[8]:


schema_final_string='''create table  IF NOT EXISTS {}.pathos_cl_master_prod_serv_cf7(
		DIM_VAL_ID 					NUMERIC 						PRIMARY KEY,
		DIM_VALUE_NAME					VARCHAR(100) 				UNIQUE NOT NULL,	
		DIM_VALUE_DESC					TEXT 				,	
		DIM_ACTIVE_REPORTING			CHAR(1) 					,	
		DIM_ACTIVE_PROCESSSING			CHAR(1) 					,		
		CHILD_RECORD					CHAR(1) 					,		
		PARENT_RECORD_ID 				NUMERIC						DEFAULT NULL, 			
		HIERARCHY_LEVEL					NUMERIC						,			
        cat_mpi TEXT,
        category TEXT,
		CREATED_ON 	TIMESTAMP WITH TIME ZONE 	DEFAULT CURRENT_TIMESTAMP NOT NULL,
		CREATED_BY 	VARCHAR(100)				NOT NULL DEFAULT 'System',
		UPDATED_ON 	TIMESTAMP WITH TIME ZONE 	DEFAULT CURRENT_TIMESTAMP NOT NULL,
		UPDATED_BY 	VARCHAR(100)				NOT NULL DEFAULT 'System'
);
'''.format(current_schema)
    
engine.execute(schema_final_string)


# ## pathos_cl_master_prod_serv_cf20

# In[9]:


schema_final_string='''create table  IF NOT EXISTS {}.pathos_cl_master_prod_serv_cf20(
		DIM_VAL_ID 					NUMERIC 						PRIMARY KEY,
		DIM_VALUE_NAME					VARCHAR(100) 				UNIQUE,	
		DIM_VALUE_DESC					TEXT 				,	
		DIM_ACTIVE_REPORTING			CHAR(1) 					,	
		DIM_ACTIVE_PROCESSSING			CHAR(1) 					,		
		CHILD_RECORD					CHAR(1) 					,		
		PARENT_RECORD_ID 				NUMERIC						DEFAULT NULL, 			
		HIERARCHY_LEVEL					NUMERIC						,			
        category TEXT,
		CREATED_ON 	TIMESTAMP WITH TIME ZONE 	DEFAULT CURRENT_TIMESTAMP NOT NULL,
		CREATED_BY 	VARCHAR(100)				NOT NULL DEFAULT 'System',
		UPDATED_ON 	TIMESTAMP WITH TIME ZONE 	DEFAULT CURRENT_TIMESTAMP NOT NULL,
		UPDATED_BY 	VARCHAR(100)				NOT NULL DEFAULT 'System'
);
'''.format(current_schema)
    
engine.execute(schema_final_string)


# ## pathos_cl_master_prod_ver

# In[10]:


schema_final_string='''create table  IF NOT EXISTS {}.pathos_cl_master_prod_ver(
		pathos_ref_prd_rel_unique_id 					NUMERIC 						PRIMARY KEY,
		pathos_product_version					TEXT 				UNIQUE NOT NULL,	
		product_release_name					TEXT 				,	
		ml_model_version			TEXT 					,	
		emotions_model_version			TEXT					,		
		drivers_model_version					TEXT 					,		
		context_model_version 				TEXT						, 			
		dim_active_reporting					CHAR(1)						,			
        dim_active_processsing CHAR(1),
        description TEXT,
		CREATED_ON 	TIMESTAMP WITH TIME ZONE 	DEFAULT CURRENT_TIMESTAMP NOT NULL,
		CREATED_BY 	VARCHAR(100)				NOT NULL DEFAULT 'System',
		UPDATED_ON 	TIMESTAMP WITH TIME ZONE 	DEFAULT CURRENT_TIMESTAMP NOT NULL,
		UPDATED_BY 	VARCHAR(100)				NOT NULL DEFAULT 'System'
);
'''.format(current_schema)
    
engine.execute(schema_final_string)


# ## pathos_cl_master_channel

# In[11]:


schema_final_string='''create table  IF NOT EXISTS {}.pathos_cl_master_channel(
		DIM_VAL_ID 					NUMERIC 						PRIMARY KEY,
		DIM_VALUE_NAME					VARCHAR(100) 				UNIQUE NOT NULL,	
		DIM_VALUE_DESC					VARCHAR(100) 				,	
		DIM_ACTIVE_REPORTING			CHAR(1) 					NOT NULL,	
		DIM_ACTIVE_PROCESSSING			CHAR(1) 					,		
		CHILD_RECORD					CHAR(1) 					,		
		PARENT_RECORD_ID 				NUMERIC						DEFAULT NULL, 			
		HIERARCHY_LEVEL					NUMERIC						NOT NULL,			
		CREATED_ON 	TIMESTAMP WITH TIME ZONE 	DEFAULT CURRENT_TIMESTAMP NOT NULL,
		CREATED_BY 	VARCHAR(100)				NOT NULL DEFAULT 'System',
		UPDATED_ON 	TIMESTAMP WITH TIME ZONE 	DEFAULT CURRENT_TIMESTAMP NOT NULL,
		UPDATED_BY 	VARCHAR(100)				NOT NULL DEFAULT 'System'
);
'''.format(current_schema)
    
engine.execute(schema_final_string)


# # pathos_cl_master_education

# In[12]:


schema_final_string='''create table  IF NOT EXISTS {}.pathos_cl_master_education(
		DIM_VAL_ID 					NUMERIC 						PRIMARY KEY,
		DIM_VALUE_NAME					VARCHAR(100) 				UNIQUE NOT NULL,	
		DIM_VALUE_DESC					VARCHAR(100) 				,	
		DIM_ACTIVE_REPORTING			CHAR(1) 					NOT NULL,	
		DIM_ACTIVE_PROCESSSING			CHAR(1) 					,		
		CHILD_RECORD					CHAR(1) 					,		
		PARENT_RECORD_ID 				NUMERIC						DEFAULT NULL, 			
		HIERARCHY_LEVEL					NUMERIC						NOT NULL,			
		CREATED_ON 	TIMESTAMP WITH TIME ZONE 	DEFAULT CURRENT_TIMESTAMP NOT NULL,
		CREATED_BY 	VARCHAR(100)				NOT NULL DEFAULT 'System',
		UPDATED_ON 	TIMESTAMP WITH TIME ZONE 	DEFAULT CURRENT_TIMESTAMP NOT NULL,
		UPDATED_BY 	VARCHAR(100)				NOT NULL DEFAULT 'System'
);
'''.format(current_schema)
    
engine.execute(schema_final_string)


# ## PATHOS_CL_MASTER_AGE

# In[13]:


schema_final_string='''create table  IF NOT EXISTS {}.PATHOS_CL_MASTER_AGE(
		DIM_VAL_ID 					NUMERIC 						PRIMARY KEY,
		DIM_VALUE_NAME					VARCHAR(100) 				UNIQUE NOT NULL,	
		DIM_VALUE_DESC					VARCHAR(100) 				,	
		DIM_ACTIVE_REPORTING			CHAR(1) 					NOT NULL,	
		DIM_ACTIVE_PROCESSSING			CHAR(1) 					,		
		CHILD_RECORD					CHAR(1) 					,		
		PARENT_RECORD_ID 				NUMERIC						DEFAULT NULL, 			
		HIERARCHY_LEVEL					NUMERIC						NOT NULL,			
		CREATED_ON 	TIMESTAMP WITH TIME ZONE 	DEFAULT CURRENT_TIMESTAMP NOT NULL,
		CREATED_BY 	VARCHAR(100)				NOT NULL DEFAULT 'System',
		UPDATED_ON 	TIMESTAMP WITH TIME ZONE 	DEFAULT CURRENT_TIMESTAMP NOT NULL,
		UPDATED_BY 	VARCHAR(100)				NOT NULL DEFAULT 'System'
);
'''.format(current_schema)
    
engine.execute(schema_final_string)


# ## PATHOS_CL_MASTER_GENDER

# In[14]:


schema_final_string='''create table  IF NOT EXISTS {}.PATHOS_CL_MASTER_GENDER(
		DIM_VAL_ID 					NUMERIC 						PRIMARY KEY,
		DIM_VALUE_NAME					VARCHAR(100) 				UNIQUE NOT NULL,	
		DIM_VALUE_DESC					VARCHAR(100) 				,	
		DIM_ACTIVE_REPORTING			CHAR(1) 					NOT NULL,	
		DIM_ACTIVE_PROCESSSING			CHAR(1) 					,		
		CHILD_RECORD					CHAR(1) 					,		
		PARENT_RECORD_ID 				NUMERIC						DEFAULT NULL, 			
		HIERARCHY_LEVEL					NUMERIC						NOT NULL,			
		CREATED_ON 	TIMESTAMP WITH TIME ZONE 	DEFAULT CURRENT_TIMESTAMP NOT NULL,
		CREATED_BY 	VARCHAR(100)				NOT NULL DEFAULT 'System',
		UPDATED_ON 	TIMESTAMP WITH TIME ZONE 	DEFAULT CURRENT_TIMESTAMP NOT NULL,
		UPDATED_BY 	VARCHAR(100)				NOT NULL DEFAULT 'System'
);
'''.format(current_schema)
    
engine.execute(schema_final_string)


# ## PATHOS_CL_MASTER_INCOME

# In[15]:


schema_final_string='''create table  IF NOT EXISTS {}.PATHOS_CL_MASTER_INCOME(
		DIM_VAL_ID 					NUMERIC 						PRIMARY KEY,
		DIM_VALUE_NAME					VARCHAR(100) 				UNIQUE NOT NULL,	
		DIM_VALUE_DESC					VARCHAR(100) 				,	
		DIM_ACTIVE_REPORTING			CHAR(1) 					NOT NULL,	
		DIM_ACTIVE_PROCESSSING			CHAR(1) 					,		
		CHILD_RECORD					CHAR(1) 					,		
		PARENT_RECORD_ID 				NUMERIC						DEFAULT NULL, 			
		HIERARCHY_LEVEL					NUMERIC						NOT NULL,			
		CREATED_ON 	TIMESTAMP WITH TIME ZONE 	DEFAULT CURRENT_TIMESTAMP NOT NULL,
		CREATED_BY 	VARCHAR(100)				NOT NULL DEFAULT 'System',
		UPDATED_ON 	TIMESTAMP WITH TIME ZONE 	DEFAULT CURRENT_TIMESTAMP NOT NULL,
		UPDATED_BY 	VARCHAR(100)				NOT NULL DEFAULT 'System'
);
'''.format(current_schema)
    
engine.execute(schema_final_string)


# ## PATHOS_CL_MASTER_OCCUPATION

# In[16]:


schema_final_string='''create table  IF NOT EXISTS {}.PATHOS_CL_MASTER_OCCUPATION(
		DIM_VAL_ID 					NUMERIC 						PRIMARY KEY,
		DIM_VALUE_NAME					VARCHAR(100) 				UNIQUE NOT NULL,	
		DIM_VALUE_DESC					VARCHAR(100) 				,	
		DIM_ACTIVE_REPORTING			CHAR(1) 					NOT NULL,	
		DIM_ACTIVE_PROCESSSING			CHAR(1) 					,		
		CHILD_RECORD					CHAR(1) 					,		
		PARENT_RECORD_ID 				NUMERIC						DEFAULT NULL, 			
		HIERARCHY_LEVEL					NUMERIC						NOT NULL,			
		CREATED_ON 	TIMESTAMP WITH TIME ZONE 	DEFAULT CURRENT_TIMESTAMP NOT NULL,
		CREATED_BY 	VARCHAR(100)				NOT NULL DEFAULT 'System',
		UPDATED_ON 	TIMESTAMP WITH TIME ZONE 	DEFAULT CURRENT_TIMESTAMP NOT NULL,
		UPDATED_BY 	VARCHAR(100)				NOT NULL DEFAULT 'System'
);
'''.format(current_schema)
    
engine.execute(schema_final_string)


# ## PATHOS_CL_MASTER_COUNTRY

# In[17]:


schema_final_string='''create table  IF NOT EXISTS {}.PATHOS_CL_MASTER_COUNTRY(
		DIM_VAL_ID 					NUMERIC 						PRIMARY KEY,
		DIM_VALUE_NAME					VARCHAR(100) 				UNIQUE NOT NULL,	
		DIM_VALUE_DESC					VARCHAR(100) 				,	
		DIM_ACTIVE_REPORTING			CHAR(1) 					NOT NULL,	
		DIM_ACTIVE_PROCESSSING			CHAR(1) 					,		
		CHILD_RECORD					CHAR(1) 					,		
		PARENT_RECORD_ID 				NUMERIC						DEFAULT NULL, 			
		HIERARCHY_LEVEL					NUMERIC						NOT NULL,			
		CREATED_ON 	TIMESTAMP WITH TIME ZONE 	DEFAULT CURRENT_TIMESTAMP NOT NULL,
		CREATED_BY 	VARCHAR(100)				NOT NULL DEFAULT 'System',
		UPDATED_ON 	TIMESTAMP WITH TIME ZONE 	DEFAULT CURRENT_TIMESTAMP NOT NULL,
		UPDATED_BY 	VARCHAR(100)				NOT NULL DEFAULT 'System'
);
'''.format(current_schema) 
    
engine.execute(schema_final_string)


# ## pathos_cl_master_personas

# In[18]:


schema_final_string='''create table  IF NOT EXISTS {}.pathos_cl_master_personas(
		PERSONA_ID 					NUMERIC 						PRIMARY KEY,
		PERSONA_NAME					VARCHAR(100) 				UNIQUE NOT NULL,	
		PERSONA					VARCHAR(100) 				UNIQUE NOT NULL,
		age_id					VARCHAR(100) 				,	
		gender_id			VARCHAR(100) 					,
        income_id			VARCHAR(100) 					,
        occupation_id			VARCHAR(100) 					,
        location_id			VARCHAR(100) 					,
        TIME_ID			VARCHAR(100) 					,
        channel_id VARCHAR(100) 					,
        education_id VARCHAR(100) 					,
		CREATED_ON 	TIMESTAMP WITH TIME ZONE 	DEFAULT CURRENT_TIMESTAMP NOT NULL,
		CREATED_BY 	VARCHAR(100)				NOT NULL DEFAULT 'System',
		UPDATED_ON 	TIMESTAMP WITH TIME ZONE 	DEFAULT CURRENT_TIMESTAMP NOT NULL,
		UPDATED_BY 	VARCHAR(100)				NOT NULL DEFAULT 'System'
);
'''.format(current_schema)
  
engine.execute(schema_final_string)


# In[ ]:





# # Generic schema

# ## PATHOS_CL_TRANSACTION_TABLE

# In[19]:


'''schema_final_string=create table  IF NOT EXISTS {}.load_iccs_transaction_cf7_cf8(
		PATHOS_TRANSACTION_ID 				NUMERIC 					PRIMARY KEY,
        id    NUMERIC,
		CLIENT_TRANSACTION_ID 				VARCHAR(100) 				UNIQUE,
		CLIENT_TRANSACTION_DATE 			VARCHAR(100) 			,
		MESSAGE_DESC						TEXT					,
        analysis_completed BOOLEAN DEFAULT FALSE,
        education_id   NUMERIC,
        q5a_1 NUMERIC,
        q5a_2 NUMERIC,
        q5a_3 NUMERIC,
        q5a_4 NUMERIC,
        q5a_5 NUMERIC,
        q5a_6 NUMERIC,
        q5a_7 NUMERIC,
        q5a_8 NUMERIC,
        q5a_9 NUMERIC,
        q5a_10   NUMERIC,
        q5b_1   NUMERIC,
        q5b_2   NUMERIC,
        q5b_3   NUMERIC,
        q5b_4   NUMERIC,
        q5b_5   NUMERIC,
        q5b_6   NUMERIC,
        q5b_7   NUMERIC,
        q5b_8   NUMERIC,
        q5b_9   NUMERIC,
        q6_1   NUMERIC,
        q6_2   NUMERIC,
        q6_3   NUMERIC,
        q6_4   NUMERIC,
        q6_5   NUMERIC,
        q6_6   NUMERIC,
        q6_7   NUMERIC,
        q6_8   NUMERIC,
        q6_9  NUMERIC,
        q6_10   NUMERIC,
        q6_11   NUMERIC,
        q8a_1   NUMERIC,
        q8a_2   NUMERIC,
        q8a_3   NUMERIC,
        q8a_4   NUMERIC,
        q8a_5  NUMERIC,
        q8a_6   NUMERIC,
        q8a_7   NUMERIC,
        q8a_8   NUMERIC,
        q8a_9   NUMERIC,
        q8b_1   NUMERIC,
        q8b_2   NUMERIC,
        q8b_3   NUMERIC,
        q8b_4   NUMERIC,
        q8b_5   NUMERIC,
        q8b_6   NUMERIC,
        q8c_1   NUMERIC,
        q8c_2   NUMERIC,
        q8c_3   NUMERIC,
        q8c_4   NUMERIC,
        q8c_5   NUMERIC,
        q8c_6   NUMERIC,
        q8d_1   NUMERIC,
        q8d_2   NUMERIC,
        q8d_3   NUMERIC,
        q8d_4   NUMERIC,
        q8d_5   NUMERIC,
        q8e_1   NUMERIC,
        q8e_2   NUMERIC,
        q8e_3   NUMERIC,
        q8e_4   NUMERIC,
        q8f_1   NUMERIC,
        q8f_2   NUMERIC,
        q8f_3   NUMERIC,
        q8f_4  NUMERIC,
        q8f_5   NUMERIC,
        q8f_6   NUMERIC,
        q8f_7   NUMERIC,
        q8f_8   NUMERIC,
        q8g_1   NUMERIC,
        q8g_2   NUMERIC,
        q8g_3   NUMERIC,
        q8g_4   NUMERIC,
        q8g_5   NUMERIC,
        q8g_6   NUMERIC,
        q8g_7   NUMERIC,
        q8g_8  NUMERIC,
        q8g_9    NUMERIC,
        q5_01   NUMERIC,
        q5_02  NUMERIC,
        q5_03   NUMERIC,
        q5_04    NUMERIC,
        q5_05   NUMERIC,
        q5_06   NUMERIC,
        q5_07   NUMERIC,
        q5_08   NUMERIC,
        q5_09   NUMERIC,
        q5_10   NUMERIC,
        q5_11  NUMERIC,
        q5_12    NUMERIC,
        q5_13   NUMERIC,
        q5_14   NUMERIC,
        q5_15   NUMERIC,
        q5_16  NUMERIC,
        q5_17 NUMERIC,
        q5_18  NUMERIC,
        q6_01 NUMERIC,
        q6_02 NUMERIC,
        q6_03  NUMERIC,
        q6_04 NUMERIC,
        q6_05 NUMERIC,
        q6_06 NUMERIC,
        q6_07 NUMERIC,
        q6_08 NUMERIC,
        q6_09 NUMERIC,
        q8a_01 NUMERIC,
        q8a_02 NUMERIC,
        q8a_03 NUMERIC,
        q8a_04 NUMERIC,
        q8a_05 NUMERIC,
        q8a_06 NUMERIC,
        q8b_01  NUMERIC,
        q8b_02 NUMERIC,
        q8b_03 NUMERIC,
        q8b_04 NUMERIC,
        q8b_05 NUMERIC,
        q8b_06 NUMERIC,
        q8c_01 NUMERIC,
        q8c_02 NUMERIC,
        q8c_03 NUMERIC,
        q8c_04 NUMERIC,
        q8c_05 NUMERIC,
        q8d_01 NUMERIC,
        q8d_02 NUMERIC,
        q8d_03 NUMERIC,
        q8d_04 NUMERIC,
        q8d_05 NUMERIC,
        q8e_01 NUMERIC,
        q8e_02 NUMERIC,
        q8e_03 NUMERIC,
        q8e_04 NUMERIC,
        q8f_01 NUMERIC,
        q8f_02 NUMERIC,
        q8f_03 NUMERIC,
        q8f_04 NUMERIC,
        q8f_05 NUMERIC,
        q8f_06 NUMERIC,
        q8f_07 NUMERIC,
        q8g_01 NUMERIC,
        q8g_02 NUMERIC,
        q8g_03 NUMERIC,
        q8g_04 NUMERIC,
        q8g_05 NUMERIC,
        q8g_06 NUMERIC,
        q8g_07 NUMERIC,
        gender_id   NUMERIC,
        age_id   NUMERIC,
        occupation_id   NUMERIC,
        income_id  NUMERIC,
        location TEXT,
        location_id NUMERIC,
        reviews TEXT,
        
        q8a_11 NUMERIC,
        q8a_10 NUMERIC,
        q6l NUMERIC,
        q6k NUMERIC,
        q6j NUMERIC,
        q6g NUMERIC,
        q6f NUMERIC,
        q6e NUMERIC,       
        q6d NUMERIC,
        q6c NUMERIC,
        q5br NUMERIC,
        q5bq NUMERIC,
        q5bp NUMERIC,
        q5bl NUMERIC,
        q5bk NUMERIC,
        q5bj NUMERIC,
        q5ai NUMERIC,
        q5ah NUMERIC,
        q5ag NUMERIC,
        q5af NUMERIC,
        q5ae NUMERIC,
        q5ad NUMERIC,
        q5ac NUMERIC,
        q5ab NUMERIC,
        q5aa NUMERIC,
        
        
        pathos_ref_prd_rel_unique_id NUMERIC,
        
        

        time_id						NUMERIC,
        channel_id NUMERIC,
        country_id NUMERIC,
        year						NUMERIC,
        VERSION_ID VARCHAR(100),
        all_pathos   TEXT,

		EMOTIONS_Surprised_SCORE			DECIMAL,
		EMOTIONS_Happy_SCORE				DECIMAL,
		EMOTIONS_Excited_SCORE 				DECIMAL,
		EMOTIONS_Sad_SCORE					DECIMAL,
		EMOTIONS_Disgust_SCORE				DECIMAL,	
		EMOTIONS_Angry_SCORE				DECIMAL,	
		EMOTIONS_Fear_SCORE					DECIMAL,		
		EMOTIONS_Frustration_SCORE			DECIMAL,

		DRIVERS_Personalization_SCORE				DECIMAL,		
		DRIVERS_Positivity_SCORE					DECIMAL,		
		DRIVERS_Well_being_SCORE					DECIMAL,		
		DRIVERS_Exciting_SCORE					    DECIMAL,		
		DRIVERS_Belonging_SCORE					    DECIMAL,		
		DRIVERS_Sustainability_SCORE				DECIMAL,		
		DRIVERS_Security_SCORE					    DECIMAL,		
		DRIVERS_Convenience_SCORE					DECIMAL,		
		DRIVERS_Care_SCORE					        DECIMAL,		
		DRIVERS_Timeliness_SCORE					DECIMAL,		
		DRIVERS_Respect_SCORE					    DECIMAL,		
		DRIVERS_Knowledge_SCORE					    DECIMAL,		
		DRIVERS_Fairness_SCORE					    DECIMAL,		
		DRIVERS_Channel_Satisfaction_SCORE			DECIMAL,		
		DRIVERS_Outcome_SCORE					    DECIMAL,		
		DRIVERS_Communication_SCORE					DECIMAL,		
		DRIVERS_Ease_of_access_SCORE				DECIMAL,		
		DRIVERS_Future_issues_SCORE					DECIMAL,		
		DRIVERS_Competence_SCORE					DECIMAL,		
		DRIVERS_Extra_mile_SCORE					DECIMAL,		
		DRIVERS_Waiting_time_SCORE					DECIMAL,		
		DRIVERS_Appeal_SCORE					    DECIMAL,		
		DRIVERS_Preferred_chanel_SCORE				DECIMAL,		
		DRIVERS_Freshness_SCORE					    DECIMAL,		
		DRIVERS_Trust_SCORE					        DECIMAL,		
		DRIVERS_Regret_SCORE					    DECIMAL,		
		DRIVERS_Relationship_SCORE					DECIMAL,		
		DRIVERS_Innovative_SCORE					DECIMAL,		
		DRIVERS_Value_for_Money_SCORE				DECIMAL,		
		DRIVERS_Packaging_SCORE					    DECIMAL,		

	
		BEHAVIOUR_ENABLERL_POSITIVE_SCORE	DECIMAL,
		BEHAVIOUR_QUAL_POSITIVE_COMMENTS		VARCHAR(4000),
		BEHAVIOUR_ENABLERL_NEGATIVE_SCORE	DECIMAL,	
		BEHAVIOUR_QUAL_NEGATIVE_COMMENTS		VARCHAR(4000),	
	
		BEHAVIOUR_SCORE						DECIMAL,
		POSITIVE_SCORE_BY_WEIGHTS			DECIMAL,	
		NEGATIVE_SCORE_BY_WEIGHTS			DECIMAL,
        
        DATE TIMESTAMP WITHOUT TIME ZONE,

		CREATED_ON 	TIMESTAMP WITH TIME ZONE 	DEFAULT CURRENT_TIMESTAMP NOT NULL,
		CREATED_BY 	VARCHAR(100)				NOT NULL DEFAULT 'System',
		UPDATED_ON 	TIMESTAMP WITH TIME ZONE 	DEFAULT CURRENT_TIMESTAMP NOT NULL,
		UPDATED_BY 	VARCHAR(100)				NOT NULL DEFAULT 'System'
);
'''.format(current_schema) 
    
engine.execute(schema_final_string)


# ## PATHOS_CL_TRANSACTION_TABLE_CF20

# In[ ]:





# In[21]:

schema_final_string='''create table  IF NOT EXISTS {}.load_iccs_transaction_cf20_ml(
		PATHOS_TRANSACTION_ID 				NUMERIC 					PRIMARY KEY,
        id    NUMERIC,
		CLIENT_TRANSACTION_ID 				VARCHAR(100) 				UNIQUE,
		CLIENT_TRANSACTION_DATE 			VARCHAR(100) 			,
		MESSAGE_DESC						TEXT					,
        analysis_completed BOOLEAN DEFAULT FALSE,
        q5a_1 NUMERIC,
        q5a_2 NUMERIC,
        q5a_3 NUMERIC,
        q5a_4 NUMERIC,
        q5a_5 NUMERIC,
        q5a_6 NUMERIC,
        q5a_7 NUMERIC,
        q5a_8 NUMERIC,
        q5a_9 NUMERIC,
        q5a_10   NUMERIC,
        q5b_1   NUMERIC,
        q5b_2   NUMERIC,
        q5b_3   NUMERIC,
        q5b_4   NUMERIC,
        q5b_5   NUMERIC,
        q5b_6   NUMERIC,
        q5b_7   NUMERIC,
        q5b_8   NUMERIC,
        q5b_9   NUMERIC,
        q6_1   NUMERIC,
        q6_2   NUMERIC,
        q6_3   NUMERIC,
        q6_4   NUMERIC,
        q6_5   NUMERIC,
        q6_6   NUMERIC,
        q6_7   NUMERIC,
        q6_8   NUMERIC,
        q6_9  NUMERIC,
        q6_10   NUMERIC,
        q6_11   NUMERIC,
        q8a_1   NUMERIC,
        q8a_2   NUMERIC,
        q8a_3   NUMERIC,
        q8a_4   NUMERIC,
        q8a_5  NUMERIC,
        q8a_6   NUMERIC,
        q8a_7   NUMERIC,
        q8a_8   NUMERIC,
        q8a_9   NUMERIC,
        q8b_1   NUMERIC,
        q8b_2   NUMERIC,
        q8b_3   NUMERIC,
        q8b_4   NUMERIC,
        q8b_5   NUMERIC,
        q8b_6   NUMERIC,
        q8c_1   NUMERIC,
        q8c_2   NUMERIC,
        q8c_3   NUMERIC,
        q8c_4   NUMERIC,
        q8c_5   NUMERIC,
        q8c_6   NUMERIC,
        q8d_1   NUMERIC,
        q8d_2   NUMERIC,
        q8d_3   NUMERIC,
        q8d_4   NUMERIC,
        q8d_5   NUMERIC,
        q8e_1   NUMERIC,
        q8e_2   NUMERIC,
        q8e_3   NUMERIC,
        q8e_4   NUMERIC,
        q8f_1   NUMERIC,
        q8f_2   NUMERIC,
        q8f_3   NUMERIC,
        q8f_4  NUMERIC,
        q8f_5   NUMERIC,
        q8f_6   NUMERIC,
        q8f_7   NUMERIC,
        q8f_8   NUMERIC,
        q8g_1   NUMERIC,
        q8g_2   NUMERIC,
        q8g_3   NUMERIC,
        q8g_4   NUMERIC,
        q8g_5   NUMERIC,
        q8g_6   NUMERIC,
        q8g_7   NUMERIC,
        q8g_8  NUMERIC,
        q8g_9    NUMERIC,
        q5_01   NUMERIC,
        q5_02  NUMERIC,
        q5_03   NUMERIC,
        q5_04    NUMERIC,
        q5_05   NUMERIC,
        q5_06   NUMERIC,
        q5_07   NUMERIC,
        q5_08   NUMERIC,
        q5_09   NUMERIC,
        q5_10   NUMERIC,
        q5_11  NUMERIC,
        q5_12    NUMERIC,
        q5_13   NUMERIC,
        q5_14   NUMERIC,
        q5_15   NUMERIC,
        q5_16  NUMERIC,
        q5_17 NUMERIC,
        q5_18  NUMERIC,
        q6_01 NUMERIC,
        q6_02 NUMERIC,
        q6_03  NUMERIC,
        q6_04 NUMERIC,
        q6_05 NUMERIC,
        q6_06 NUMERIC,
        q6_07 NUMERIC,
        q6_08 NUMERIC,
        q6_09 NUMERIC,
        q8a_01 NUMERIC,
        q8a_02 NUMERIC,
        q8a_03 NUMERIC,
        q8a_04 NUMERIC,
        q8a_05 NUMERIC,
        q8a_06 NUMERIC,
        q8b_01  NUMERIC,
        q8b_02 NUMERIC,
        q8b_03 NUMERIC,
        q8b_04 NUMERIC,
        q8b_05 NUMERIC,
        q8b_06 NUMERIC,
        q8c_01 NUMERIC,
        q8c_02 NUMERIC,
        q8c_03 NUMERIC,
        q8c_04 NUMERIC,
        q8c_05 NUMERIC,
        q8d_01 NUMERIC,
        q8d_02 NUMERIC,
        q8d_03 NUMERIC,
        q8d_04 NUMERIC,
        q8d_05 NUMERIC,
        q8e_01 NUMERIC,
        q8e_02 NUMERIC,
        q8e_03 NUMERIC,
        q8e_04 NUMERIC,
        q8f_01 NUMERIC,
        q8f_02 NUMERIC,
        q8f_03 NUMERIC,
        q8f_04 NUMERIC,
        q8f_05 NUMERIC,
        q8f_06 NUMERIC,
        q8f_07 NUMERIC,
        q8g_01 NUMERIC,
        q8g_02 NUMERIC,
        q8g_03 NUMERIC,
        q8g_04 NUMERIC,
        q8g_05 NUMERIC,
        q8g_06 NUMERIC,
        q8g_07 NUMERIC,
        gender_id   NUMERIC,
        age_id   NUMERIC,
        occupation_id   NUMERIC,
        income_id  NUMERIC,
        location TEXT,
        location_id NUMERIC,
        education_id   NUMERIC,
        education TEXT,
        reviews TEXT,
        
        q8a_11 NUMERIC,
        q8a_10 NUMERIC,
        q6l NUMERIC,
        q6k NUMERIC,
        q6j NUMERIC,
        q6g NUMERIC,
        q6f NUMERIC,
        q6e NUMERIC,       
        q6d NUMERIC,
        q6c NUMERIC,
        q5br NUMERIC,
        q5bq NUMERIC,
        q5bp NUMERIC,
        q5bl NUMERIC,
        q5bk NUMERIC,
        q5bj NUMERIC,
        q5ai NUMERIC,
        q5ah NUMERIC,
        q5ag NUMERIC,
        q5af NUMERIC,
        q5ae NUMERIC,
        q5ad NUMERIC,
        q5ac NUMERIC,
        q5ab NUMERIC,
        q5aa NUMERIC,
        
        
        pathos_ref_prd_rel_unique_id NUMERIC,
        
        

        time_id						NUMERIC,
        channel_id NUMERIC,
        country_id NUMERIC,
        year						NUMERIC,
        VERSION_ID VARCHAR(100),
        all_pathos   TEXT,

		EMOTIONS_Surprised_SCORE			DECIMAL DEFAULT 0.00001,
		EMOTIONS_Happy_SCORE				DECIMAL DEFAULT 0.00001,
		EMOTIONS_Excited_SCORE 				DECIMAL DEFAULT 0.00001,
		EMOTIONS_Sad_SCORE					DECIMAL DEFAULT 0.00001,
		EMOTIONS_Disgust_SCORE				DECIMAL DEFAULT 0.00001,	
		EMOTIONS_Angry_SCORE				DECIMAL DEFAULT 0.00001,	
		EMOTIONS_Fear_SCORE					DECIMAL DEFAULT 0.00001,		
		EMOTIONS_Frustration_SCORE			DECIMAL DEFAULT 0.00001,

		DRIVERS_Personalization_SCORE				DECIMAL DEFAULT 0.00001,		
		DRIVERS_Positivity_SCORE					DECIMAL DEFAULT 0.00001,		
		DRIVERS_Well_being_SCORE					DECIMAL DEFAULT 0.00001,		
		DRIVERS_Exciting_SCORE					    DECIMAL DEFAULT 0.00001,		
		DRIVERS_Belonging_SCORE					    DECIMAL DEFAULT 0.00001,		
		DRIVERS_Sustainability_SCORE				DECIMAL DEFAULT 0.00001,		
		DRIVERS_Security_SCORE					    DECIMAL DEFAULT 0.00001,		
		DRIVERS_Convenience_SCORE					DECIMAL DEFAULT 0.00001,		
		DRIVERS_Care_SCORE					        DECIMAL DEFAULT 0.00001,		
		DRIVERS_Timeliness_SCORE					DECIMAL DEFAULT 0.00001,		
		DRIVERS_Respect_SCORE					    DECIMAL DEFAULT 0.00001,		
		DRIVERS_Knowledge_SCORE					    DECIMAL DEFAULT 0.00001,		
		DRIVERS_Fairness_SCORE					    DECIMAL DEFAULT 0.00001,		
		DRIVERS_Channel_Satisfaction_SCORE			DECIMAL DEFAULT 0.00001,		
		DRIVERS_Outcome_SCORE					    DECIMAL DEFAULT 0.00001,		
		DRIVERS_Communication_SCORE					DECIMAL DEFAULT 0.00001,		
		DRIVERS_Ease_of_access_SCORE				DECIMAL DEFAULT 0.00001,		
		DRIVERS_Future_issues_SCORE					DECIMAL DEFAULT 0.00001,		
		DRIVERS_Competence_SCORE					DECIMAL DEFAULT 0.00001,		
		DRIVERS_Extra_mile_SCORE					DECIMAL DEFAULT 0.00001,		
		DRIVERS_Waiting_time_SCORE					DECIMAL DEFAULT 0.00001,		
		DRIVERS_Appeal_SCORE					    DECIMAL DEFAULT 0.00001,		
		DRIVERS_Preferred_chanel_SCORE				DECIMAL DEFAULT 0.00001,		
		DRIVERS_Freshness_SCORE					    DECIMAL DEFAULT 0.00001,		
		DRIVERS_Trust_SCORE					        DECIMAL DEFAULT 0.00001,		
		DRIVERS_Regret_SCORE					    DECIMAL DEFAULT 0.00001,		
		DRIVERS_Relationship_SCORE					DECIMAL DEFAULT 0.00001,		
		DRIVERS_Innovative_SCORE					DECIMAL DEFAULT 0.00001,		
		DRIVERS_Value_for_Money_SCORE				DECIMAL DEFAULT 0.00001,		
		DRIVERS_Packaging_SCORE					    DECIMAL DEFAULT 0.00001,		

	
		BEHAVIOUR_ENABLERL_POSITIVE_SCORE	DECIMAL,
		BEHAVIOUR_QUAL_POSITIVE_COMMENTS		VARCHAR(4000),
		BEHAVIOUR_ENABLERL_NEGATIVE_SCORE	DECIMAL,	
		BEHAVIOUR_QUAL_NEGATIVE_COMMENTS		VARCHAR(4000),	
	
		BEHAVIOUR_SCORE						DECIMAL,
		POSITIVE_SCORE_BY_WEIGHTS			DECIMAL,	
		NEGATIVE_SCORE_BY_WEIGHTS			DECIMAL,
        
        DATE TIMESTAMP WITHOUT TIME ZONE,

		CREATED_ON 	TIMESTAMP WITH TIME ZONE 	DEFAULT CURRENT_TIMESTAMP NOT NULL,
		CREATED_BY 	VARCHAR(100)				NOT NULL DEFAULT 'System',
		UPDATED_ON 	TIMESTAMP WITH TIME ZONE 	DEFAULT CURRENT_TIMESTAMP NOT NULL,
		UPDATED_BY 	VARCHAR(100)				NOT NULL DEFAULT 'System'
);
'''.format(current_schema)
    
engine.execute(schema_final_string)



schema_final_string='''create table  IF NOT EXISTS {}.load_iccs_transaction_cf20(
		PATHOS_TRANSACTION_ID 				NUMERIC 					PRIMARY KEY,
        id    NUMERIC,
		CLIENT_TRANSACTION_ID 				VARCHAR(100) 				UNIQUE,
		CLIENT_TRANSACTION_DATE 			VARCHAR(100) 			,
		MESSAGE_DESC						TEXT					,
        analysis_completed BOOLEAN DEFAULT FALSE,
        q5a_1 NUMERIC,
        q5a_2 NUMERIC,
        q5a_3 NUMERIC,
        q5a_4 NUMERIC,
        q5a_5 NUMERIC,
        q5a_6 NUMERIC,
        q5a_7 NUMERIC,
        q5a_8 NUMERIC,
        q5a_9 NUMERIC,
        q5a_10   NUMERIC,
        q5b_1   NUMERIC,
        q5b_2   NUMERIC,
        q5b_3   NUMERIC,
        q5b_4   NUMERIC,
        q5b_5   NUMERIC,
        q5b_6   NUMERIC,
        q5b_7   NUMERIC,
        q5b_8   NUMERIC,
        q5b_9   NUMERIC,
        q6_1   NUMERIC,
        q6_2   NUMERIC,
        q6_3   NUMERIC,
        q6_4   NUMERIC,
        q6_5   NUMERIC,
        q6_6   NUMERIC,
        q6_7   NUMERIC,
        q6_8   NUMERIC,
        q6_9  NUMERIC,
        q6_10   NUMERIC,
        q6_11   NUMERIC,
        q8a_1   NUMERIC,
        q8a_2   NUMERIC,
        q8a_3   NUMERIC,
        q8a_4   NUMERIC,
        q8a_5  NUMERIC,
        q8a_6   NUMERIC,
        q8a_7   NUMERIC,
        q8a_8   NUMERIC,
        q8a_9   NUMERIC,
        q8b_1   NUMERIC,
        q8b_2   NUMERIC,
        q8b_3   NUMERIC,
        q8b_4   NUMERIC,
        q8b_5   NUMERIC,
        q8b_6   NUMERIC,
        q8c_1   NUMERIC,
        q8c_2   NUMERIC,
        q8c_3   NUMERIC,
        q8c_4   NUMERIC,
        q8c_5   NUMERIC,
        q8c_6   NUMERIC,
        q8d_1   NUMERIC,
        q8d_2   NUMERIC,
        q8d_3   NUMERIC,
        q8d_4   NUMERIC,
        q8d_5   NUMERIC,
        q8e_1   NUMERIC,
        q8e_2   NUMERIC,
        q8e_3   NUMERIC,
        q8e_4   NUMERIC,
        q8f_1   NUMERIC,
        q8f_2   NUMERIC,
        q8f_3   NUMERIC,
        q8f_4  NUMERIC,
        q8f_5   NUMERIC,
        q8f_6   NUMERIC,
        q8f_7   NUMERIC,
        q8f_8   NUMERIC,
        q8g_1   NUMERIC,
        q8g_2   NUMERIC,
        q8g_3   NUMERIC,
        q8g_4   NUMERIC,
        q8g_5   NUMERIC,
        q8g_6   NUMERIC,
        q8g_7   NUMERIC,
        q8g_8  NUMERIC,
        q8g_9    NUMERIC,
        q5_01   NUMERIC,
        q5_02  NUMERIC,
        q5_03   NUMERIC,
        q5_04    NUMERIC,
        q5_05   NUMERIC,
        q5_06   NUMERIC,
        q5_07   NUMERIC,
        q5_08   NUMERIC,
        q5_09   NUMERIC,
        q5_10   NUMERIC,
        q5_11  NUMERIC,
        q5_12    NUMERIC,
        q5_13   NUMERIC,
        q5_14   NUMERIC,
        q5_15   NUMERIC,
        q5_16  NUMERIC,
        q5_17 NUMERIC,
        q5_18  NUMERIC,
        q6_01 NUMERIC,
        q6_02 NUMERIC,
        q6_03  NUMERIC,
        q6_04 NUMERIC,
        q6_05 NUMERIC,
        q6_06 NUMERIC,
        q6_07 NUMERIC,
        q6_08 NUMERIC,
        q6_09 NUMERIC,
        q8a_01 NUMERIC,
        q8a_02 NUMERIC,
        q8a_03 NUMERIC,
        q8a_04 NUMERIC,
        q8a_05 NUMERIC,
        q8a_06 NUMERIC,
        q8b_01  NUMERIC,
        q8b_02 NUMERIC,
        q8b_03 NUMERIC,
        q8b_04 NUMERIC,
        q8b_05 NUMERIC,
        q8b_06 NUMERIC,
        q8c_01 NUMERIC,
        q8c_02 NUMERIC,
        q8c_03 NUMERIC,
        q8c_04 NUMERIC,
        q8c_05 NUMERIC,
        q8d_01 NUMERIC,
        q8d_02 NUMERIC,
        q8d_03 NUMERIC,
        q8d_04 NUMERIC,
        q8d_05 NUMERIC,
        q8e_01 NUMERIC,
        q8e_02 NUMERIC,
        q8e_03 NUMERIC,
        q8e_04 NUMERIC,
        q8f_01 NUMERIC,
        q8f_02 NUMERIC,
        q8f_03 NUMERIC,
        q8f_04 NUMERIC,
        q8f_05 NUMERIC,
        q8f_06 NUMERIC,
        q8f_07 NUMERIC,
        q8g_01 NUMERIC,
        q8g_02 NUMERIC,
        q8g_03 NUMERIC,
        q8g_04 NUMERIC,
        q8g_05 NUMERIC,
        q8g_06 NUMERIC,
        q8g_07 NUMERIC,
        gender_id   NUMERIC,
        age_id   NUMERIC,
        occupation_id   NUMERIC,
        income_id  NUMERIC,
        location TEXT,
        location_id NUMERIC,
        education_id   NUMERIC,
        education TEXT,
        reviews TEXT,
        
        q8a_11 NUMERIC,
        q8a_10 NUMERIC,
        q6l NUMERIC,
        q6k NUMERIC,
        q6j NUMERIC,
        q6g NUMERIC,
        q6f NUMERIC,
        q6e NUMERIC,       
        q6d NUMERIC,
        q6c NUMERIC,
        q5br NUMERIC,
        q5bq NUMERIC,
        q5bp NUMERIC,
        q5bl NUMERIC,
        q5bk NUMERIC,
        q5bj NUMERIC,
        q5ai NUMERIC,
        q5ah NUMERIC,
        q5ag NUMERIC,
        q5af NUMERIC,
        q5ae NUMERIC,
        q5ad NUMERIC,
        q5ac NUMERIC,
        q5ab NUMERIC,
        q5aa NUMERIC,
        
        
        pathos_ref_prd_rel_unique_id NUMERIC,
        
        

        time_id						NUMERIC,
        channel_id NUMERIC,
        country_id NUMERIC,
        year						NUMERIC,
        VERSION_ID VARCHAR(100),
        all_pathos   TEXT,

		EMOTIONS_Surprised_SCORE			DECIMAL DEFAULT 0.00001,
		EMOTIONS_Happy_SCORE				DECIMAL DEFAULT 0.00001,
		EMOTIONS_Excited_SCORE 				DECIMAL DEFAULT 0.00001,
		EMOTIONS_Sad_SCORE					DECIMAL DEFAULT 0.00001,
		EMOTIONS_Disgust_SCORE				DECIMAL DEFAULT 0.00001,	
		EMOTIONS_Angry_SCORE				DECIMAL DEFAULT 0.00001,	
		EMOTIONS_Fear_SCORE					DECIMAL DEFAULT 0.00001,		
		EMOTIONS_Frustration_SCORE			DECIMAL DEFAULT 0.00001,

		DRIVERS_Personalization_SCORE				DECIMAL DEFAULT 0.00001,		
		DRIVERS_Positivity_SCORE					DECIMAL DEFAULT 0.00001,		
		DRIVERS_Well_being_SCORE					DECIMAL DEFAULT 0.00001,		
		DRIVERS_Exciting_SCORE					    DECIMAL DEFAULT 0.00001,		
		DRIVERS_Belonging_SCORE					    DECIMAL DEFAULT 0.00001,		
		DRIVERS_Sustainability_SCORE				DECIMAL DEFAULT 0.00001,		
		DRIVERS_Security_SCORE					    DECIMAL DEFAULT 0.00001,		
		DRIVERS_Convenience_SCORE					DECIMAL DEFAULT 0.00001,		
		DRIVERS_Care_SCORE					        DECIMAL DEFAULT 0.00001,		
		DRIVERS_Timeliness_SCORE					DECIMAL DEFAULT 0.00001,		
		DRIVERS_Respect_SCORE					    DECIMAL DEFAULT 0.00001,		
		DRIVERS_Knowledge_SCORE					    DECIMAL DEFAULT 0.00001,		
		DRIVERS_Fairness_SCORE					    DECIMAL DEFAULT 0.00001,		
		DRIVERS_Channel_Satisfaction_SCORE			DECIMAL DEFAULT 0.00001,		
		DRIVERS_Outcome_SCORE					    DECIMAL DEFAULT 0.00001,		
		DRIVERS_Communication_SCORE					DECIMAL DEFAULT 0.00001,		
		DRIVERS_Ease_of_access_SCORE				DECIMAL DEFAULT 0.00001,		
		DRIVERS_Future_issues_SCORE					DECIMAL DEFAULT 0.00001,		
		DRIVERS_Competence_SCORE					DECIMAL DEFAULT 0.00001,		
		DRIVERS_Extra_mile_SCORE					DECIMAL DEFAULT 0.00001,		
		DRIVERS_Waiting_time_SCORE					DECIMAL DEFAULT 0.00001,		
		DRIVERS_Appeal_SCORE					    DECIMAL DEFAULT 0.00001,		
		DRIVERS_Preferred_chanel_SCORE				DECIMAL DEFAULT 0.00001,		
		DRIVERS_Freshness_SCORE					    DECIMAL DEFAULT 0.00001,		
		DRIVERS_Trust_SCORE					        DECIMAL DEFAULT 0.00001,		
		DRIVERS_Regret_SCORE					    DECIMAL DEFAULT 0.00001,		
		DRIVERS_Relationship_SCORE					DECIMAL DEFAULT 0.00001,		
		DRIVERS_Innovative_SCORE					DECIMAL DEFAULT 0.00001,		
		DRIVERS_Value_for_Money_SCORE				DECIMAL DEFAULT 0.00001,		
		DRIVERS_Packaging_SCORE					    DECIMAL DEFAULT 0.00001,		

	
		BEHAVIOUR_ENABLERL_POSITIVE_SCORE	DECIMAL,
		BEHAVIOUR_QUAL_POSITIVE_COMMENTS		VARCHAR(4000),
		BEHAVIOUR_ENABLERL_NEGATIVE_SCORE	DECIMAL,	
		BEHAVIOUR_QUAL_NEGATIVE_COMMENTS		VARCHAR(4000),	
	
		BEHAVIOUR_SCORE						DECIMAL,
		POSITIVE_SCORE_BY_WEIGHTS			DECIMAL,	
		NEGATIVE_SCORE_BY_WEIGHTS			DECIMAL,
        
        DATE TIMESTAMP WITHOUT TIME ZONE,

		CREATED_ON 	TIMESTAMP WITH TIME ZONE 	DEFAULT CURRENT_TIMESTAMP NOT NULL,
		CREATED_BY 	VARCHAR(100)				NOT NULL DEFAULT 'System',
		UPDATED_ON 	TIMESTAMP WITH TIME ZONE 	DEFAULT CURRENT_TIMESTAMP NOT NULL,
		UPDATED_BY 	VARCHAR(100)				NOT NULL DEFAULT 'System'
);
'''.format(current_schema)
    
engine.execute(schema_final_string)


# ## PATHOS_CL_SUMMARY_TABLE

# In[22]:


# schema_final_string='''create table  IF NOT EXISTS {}.ICCS_SUMMARY_TABLE(
# 		PATHOS_SUMMARY_ID 				NUMERIC 					PRIMARY KEY,
#         review_id    TEXT,
# 		CLIENT_TRANSACTION_ID 				VARCHAR(100) 				UNIQUE,
# 		CLIENT_TRANSACTION_DATE 			VARCHAR(100) 			,
# 		MESSAGE_DESC						TEXT					,
#         date 	TIMESTAMP WITH TIME ZONE,
#         analysis_completed BOOLEAN DEFAULT FALSE, 
#         education_id   NUMERIC,
#         education TEXT,
# 		q5a_1 NUMERIC,
#         q5a_2 NUMERIC,
#         q5a_3 NUMERIC,
#         q5a_4 NUMERIC,
#         q5a_5 NUMERIC,
#         q5a_6 NUMERIC,
#         q5a_7 NUMERIC,
#         q5a_8 NUMERIC,
#         q5a_9 NUMERIC,
#         q5a_10   NUMERIC,
#         q5b_1   NUMERIC,
#         q5b_2   NUMERIC,
#         q5b_3   NUMERIC,
#         q5b_4   NUMERIC,
#         q5b_5   NUMERIC,
#         q5b_6   NUMERIC,
#         q5b_7   NUMERIC,
#         q5b_8   NUMERIC,
#         q5b_9   NUMERIC,
#         q6_1   NUMERIC,
#         q6_2   NUMERIC,
#         q6_3   NUMERIC,
#         q6_4   NUMERIC,
#         q6_5   NUMERIC,
#         q6_6   NUMERIC,
#         q6_7   NUMERIC,
#         q6_8   NUMERIC,
#         q6_9  NUMERIC,
#         q6_10   NUMERIC,
#         q6_11   NUMERIC,
#         q8a_1   NUMERIC,
#         q8a_2   NUMERIC,
#         q8a_3   NUMERIC,
#         q8a_4   NUMERIC,
#         q8a_5  NUMERIC,
#         q8a_6   NUMERIC,
#         q8a_7   NUMERIC,
#         q8a_8   NUMERIC,
#         q8a_9   NUMERIC,
#         q8b_1   NUMERIC,
#         q8b_2   NUMERIC,
#         q8b_3   NUMERIC,
#         q8b_4   NUMERIC,
#         q8b_5   NUMERIC,
#         q8b_6   NUMERIC,
#         q8c_1   NUMERIC,
#         q8c_2   NUMERIC,
#         q8c_3   NUMERIC,
#         q8c_4   NUMERIC,
#         q8c_5   NUMERIC,
#         q8c_6   NUMERIC,
#         q8d_1   NUMERIC,
#         q8d_2   NUMERIC,
#         q8d_3   NUMERIC,
#         q8d_4   NUMERIC,
#         q8d_5   NUMERIC,
#         q8e_1   NUMERIC,
#         q8e_2   NUMERIC,
#         q8e_3   NUMERIC,
#         q8e_4   NUMERIC,
#         q8f_1   NUMERIC,
#         q8f_2   NUMERIC,
#         q8f_3   NUMERIC,
#         q8f_4  NUMERIC,
#         q8f_5   NUMERIC,
#         q8f_6   NUMERIC,
#         q8f_7   NUMERIC,
#         q8f_8   NUMERIC,
#         q8g_1   NUMERIC,
#         q8g_2   NUMERIC,
#         q8g_3   NUMERIC,
#         q8g_4   NUMERIC,
#         q8g_5   NUMERIC,
#         q8g_6   NUMERIC,
#         q8g_7   NUMERIC,
#         q8g_8  NUMERIC,
#         q8g_9    NUMERIC,
#         q5_01   NUMERIC,
#         q5_02  NUMERIC,
#         q5_03   NUMERIC,
#         q5_04    NUMERIC,
#         q5_05   NUMERIC,
#         q5_06   NUMERIC,
#         q5_07   NUMERIC,
#         q5_08   NUMERIC,
#         q5_09   NUMERIC,
#         q5_10   NUMERIC,
#         q5_11  NUMERIC,
#         q5_12    NUMERIC,
#         q5_13   NUMERIC,
#         q5_14   NUMERIC,
#         q5_15   NUMERIC,
#         q5_16  NUMERIC,
#         q5_17 NUMERIC,
#         q5_18  NUMERIC,
#         q6_01 NUMERIC,
#         q6_02 NUMERIC,
#         q6_03  NUMERIC,
#         q6_04 NUMERIC,
#         q6_05 NUMERIC,
#         q6_06 NUMERIC,
#         q6_07 NUMERIC,
#         q6_08 NUMERIC,
#         q6_09 NUMERIC,
#         q8a_01 NUMERIC,
#         q8a_02 NUMERIC,
#         q8a_03 NUMERIC,
#         q8a_04 NUMERIC,
#         q8a_05 NUMERIC,
#         q8a_06 NUMERIC,
#         q8b_01  NUMERIC,
#         q8b_02 NUMERIC,
#         q8b_03 NUMERIC,
#         q8b_04 NUMERIC,
#         q8b_05 NUMERIC,
#         q8b_06 NUMERIC,
#         q8c_01 NUMERIC,
#         q8c_02 NUMERIC,
#         q8c_03 NUMERIC,
#         q8c_04 NUMERIC,
#         q8c_05 NUMERIC,
#         q8d_01 NUMERIC,
#         q8d_02 NUMERIC,
#         q8d_03 NUMERIC,
#         q8d_04 NUMERIC,
#         q8d_05 NUMERIC,
#         q8e_01 NUMERIC,
#         q8e_02 NUMERIC,
#         q8e_03 NUMERIC,
#         q8e_04 NUMERIC,
#         q8f_01 NUMERIC,
#         q8f_02 NUMERIC,
#         q8f_03 NUMERIC,
#         q8f_04 NUMERIC,
#         q8f_05 NUMERIC,
#         q8f_06 NUMERIC,
#         q8f_07 NUMERIC,
#         q8g_01 NUMERIC,
#         q8g_02 NUMERIC,
#         q8g_03 NUMERIC,
#         q8g_04 NUMERIC,
#         q8g_05 NUMERIC,
#         q8g_06 NUMERIC,
#         q8g_07 NUMERIC,
#         gender_id   NUMERIC,
#         age_id   NUMERIC,
#         occupation_id   NUMERIC,
#         income_id  NUMERIC,
#         jurisdiction TEXT,
#         pos_score DECIMAL,
#         neg_score DECIMAL,
#         location TEXT,
#         location_id NUMERIC,

#         time_id						NUMERIC,
#         channel_id NUMERIC,
#         country_id NUMERIC,
#         year						TIMESTAMP WITHOUT TIME ZONE,
#         gender TEXT,
#         age TEXT,
#         occupation TEXT,
#         income TEXT,
#         channel TEXT,
#         people_score DECIMAL,
#         process_score DECIMAL,
#         product_score DECIMAL,
#         intent_score DECIMAL,
#         loyalty_score DECIMAL,
#         values_score DECIMAL,
#         VERSION_ID VARCHAR(100),
#         all_pathos   TEXT,

# 		EMOTIONS_SCORE						DECIMAL,
# 		DRIVERS_SCORE						DECIMAL,
# 		EMOTIONAL_ENGAGEMENT_SCORE			DECIMAL,
#         n_emotional_engagement_score        DECIMAL, 
#         emotional_engagement_quintile       TEXT,
	
# 		EMOTIONS_Surprised_SCORE			DECIMAL DEFAULT 0.00001,
# 		EMOTIONS_Happy_SCORE				DECIMAL DEFAULT 0.00001,
# 		EMOTIONS_Excited_SCORE 				DECIMAL DEFAULT 0.00001,
# 		EMOTIONS_Sad_SCORE					DECIMAL DEFAULT 0.00001,
# 		EMOTIONS_Disgust_SCORE				DECIMAL DEFAULT 0.00001,	
# 		EMOTIONS_Angry_SCORE				DECIMAL DEFAULT 0.00001,	
# 		EMOTIONS_Fear_SCORE					DECIMAL DEFAULT 0.00001,		
# 		EMOTIONS_Frustration_SCORE			DECIMAL DEFAULT 0.00001,

# 		DRIVERS_Personalization_SCORE				DECIMAL DEFAULT 0.00001,		
# 		DRIVERS_Positivity_SCORE					DECIMAL DEFAULT 0.00001,		
# 		DRIVERS_Well_being_SCORE					DECIMAL DEFAULT 0.00001,		
# 		DRIVERS_Exciting_SCORE					    DECIMAL DEFAULT 0.00001,		
# 		DRIVERS_Belonging_SCORE					    DECIMAL DEFAULT 0.00001,		
# 		DRIVERS_Sustainability_SCORE				DECIMAL DEFAULT 0.00001,		
# 		DRIVERS_Security_SCORE					    DECIMAL DEFAULT 0.00001,		
# 		DRIVERS_Convenience_SCORE					DECIMAL DEFAULT 0.00001,		
# 		DRIVERS_Care_SCORE					        DECIMAL DEFAULT 0.00001,		
# 		DRIVERS_Timeliness_SCORE					DECIMAL DEFAULT 0.00001,		
# 		DRIVERS_Respect_SCORE					    DECIMAL DEFAULT 0.00001,		
# 		DRIVERS_Knowledge_SCORE					    DECIMAL DEFAULT 0.00001,		
# 		DRIVERS_Fairness_SCORE					    DECIMAL DEFAULT 0.00001,		
# 		DRIVERS_Channel_Satisfaction_SCORE			DECIMAL DEFAULT 0.00001,		
# 		DRIVERS_Outcome_SCORE					    DECIMAL DEFAULT 0.00001,		
# 		DRIVERS_Communication_SCORE					DECIMAL DEFAULT 0.00001,		
# 		DRIVERS_Ease_of_access_SCORE				DECIMAL DEFAULT 0.00001,		
# 		DRIVERS_Future_issues_SCORE					DECIMAL DEFAULT 0.00001,		
# 		DRIVERS_Competence_SCORE					DECIMAL DEFAULT 0.00001,		
# 		DRIVERS_Extra_mile_SCORE					DECIMAL DEFAULT 0.00001,		
# 		DRIVERS_Waiting_time_SCORE					DECIMAL DEFAULT 0.00001,		
# 		DRIVERS_Appeal_SCORE					    DECIMAL DEFAULT 0.00001,		
# 		DRIVERS_Preferred_chanel_SCORE				DECIMAL DEFAULT 0.00001,		
# 		DRIVERS_Freshness_SCORE					    DECIMAL DEFAULT 0.00001,		
# 		DRIVERS_Trust_SCORE					        DECIMAL DEFAULT 0.00001,		
# 		DRIVERS_Regret_SCORE					    DECIMAL DEFAULT 0.00001,		
# 		DRIVERS_Relationship_SCORE					DECIMAL DEFAULT 0.00001,		
# 		DRIVERS_Innovative_SCORE					DECIMAL DEFAULT 0.00001,		
# 		DRIVERS_Value_for_Money_SCORE				DECIMAL DEFAULT 0.00001,		
# 		DRIVERS_Packaging_SCORE					    DECIMAL DEFAULT 0.00001,		

# 		BEHAVIOUR_ENABLERL_POSITIVE_SCORE	DECIMAL,
# 		BEHAVIOUR_QUAL_POSITIVE_COMMENTS		VARCHAR(4000),
# 		BEHAVIOUR_ENABLERL_NEGATIVE_SCORE	DECIMAL,	
# 		BEHAVIOUR_QUAL_NEGATIVE_COMMENTS		VARCHAR(4000),	
#         PREDICTED_DRIVER			TEXT,
#         predicted_driver_ppp TEXT,
#         predicted_driver_ilv TEXT,
#         iso_code TEXT,
	
# 		BEHAVIOUR_SCORE						DECIMAL,
# 		POSITIVE_SCORE_BY_WEIGHTS			DECIMAL,	
# 		NEGATIVE_SCORE_BY_WEIGHTS			DECIMAL,
# 		CREATED_ON 	TIMESTAMP WITH TIME ZONE 	DEFAULT CURRENT_TIMESTAMP NOT NULL,
# 		CREATED_BY 	VARCHAR(100)				NOT NULL DEFAULT 'System',
# 		UPDATED_ON 	TIMESTAMP WITH TIME ZONE 	DEFAULT CURRENT_TIMESTAMP NOT NULL,
# 		UPDATED_BY 	VARCHAR(100)				NOT NULL DEFAULT 'System'
# );
# '''.format(current_schema)
    
# engine.execute(schema_final_string)


# ## PATHOS_CL_SUMMARY_TABLE_with_cf20

# In[23]:


schema_final_string='''create table  IF NOT EXISTS {}.load_iccs_summary(
		PATHOS_SUMMARY_ID 				NUMERIC 					PRIMARY KEY,
        review_id    TEXT,
		CLIENT_TRANSACTION_ID 				VARCHAR(100) 				UNIQUE,
		CLIENT_TRANSACTION_DATE 			VARCHAR(100) 			,
		MESSAGE_DESC						TEXT					,
        analysis_completed BOOLEAN DEFAULT FALSE, 
        date 	TIMESTAMP WITHOUT TIME ZONE,
        education_id   NUMERIC,
        education TEXT,
		q5a_1 NUMERIC,
        q5a_2 NUMERIC,
        q5a_3 NUMERIC,
        q5a_4 NUMERIC,
        q5a_5 NUMERIC,
        q5a_6 NUMERIC,
        q5a_7 NUMERIC,
        q5a_8 NUMERIC,
        q5a_9 NUMERIC,
        q5a_10   NUMERIC,
        q5b_1   NUMERIC,
        q5b_2   NUMERIC,
        q5b_3   NUMERIC,
        q5b_4   NUMERIC,
        q5b_5   NUMERIC,
        q5b_6   NUMERIC,
        q5b_7   NUMERIC,
        q5b_8   NUMERIC,
        q5b_9   NUMERIC,
        q6_1   NUMERIC,
        q6_2   NUMERIC,
        q6_3   NUMERIC,
        q6_4   NUMERIC,
        q6_5   NUMERIC,
        q6_6   NUMERIC,
        q6_7   NUMERIC,
        q6_8   NUMERIC,
        q6_9  NUMERIC,
        q6_10   NUMERIC,
        q6_11   NUMERIC,
        q8a_1   NUMERIC,
        q8a_2   NUMERIC,
        q8a_3   NUMERIC,
        q8a_4   NUMERIC,
        q8a_5  NUMERIC,
        q8a_6   NUMERIC,
        q8a_7   NUMERIC,
        q8a_8   NUMERIC,
        q8a_9   NUMERIC,
        q8b_1   NUMERIC,
        q8b_2   NUMERIC,
        q8b_3   NUMERIC,
        q8b_4   NUMERIC,
        q8b_5   NUMERIC,
        q8b_6   NUMERIC,
        q8c_1   NUMERIC,
        q8c_2   NUMERIC,
        q8c_3   NUMERIC,
        q8c_4   NUMERIC,
        q8c_5   NUMERIC,
        q8c_6   NUMERIC,
        q8d_1   NUMERIC,
        q8d_2   NUMERIC,
        q8d_3   NUMERIC,
        q8d_4   NUMERIC,
        q8d_5   NUMERIC,
        q8e_1   NUMERIC,
        q8e_2   NUMERIC,
        q8e_3   NUMERIC,
        q8e_4   NUMERIC,
        q8f_1   NUMERIC,
        q8f_2   NUMERIC,
        q8f_3   NUMERIC,
        q8f_4  NUMERIC,
        q8f_5   NUMERIC,
        q8f_6   NUMERIC,
        q8f_7   NUMERIC,
        q8f_8   NUMERIC,
        q8g_1   NUMERIC,
        q8g_2   NUMERIC,
        q8g_3   NUMERIC,
        q8g_4   NUMERIC,
        q8g_5   NUMERIC,
        q8g_6   NUMERIC,
        q8g_7   NUMERIC,
        q8g_8  NUMERIC,
        q8g_9    NUMERIC,
        q5_01   NUMERIC,
        q5_02  NUMERIC,
        q5_03   NUMERIC,
        q5_04    NUMERIC,
        q5_05   NUMERIC,
        q5_06   NUMERIC,
        q5_07   NUMERIC,
        q5_08   NUMERIC,
        q5_09   NUMERIC,
        q5_10   NUMERIC,
        q5_11  NUMERIC,
        q5_12    NUMERIC,
        q5_13   NUMERIC,
        q5_14   NUMERIC,
        q5_15   NUMERIC,
        q5_16  NUMERIC,
        q5_17 NUMERIC,
        q5_18  NUMERIC,
        q6_01 NUMERIC,
        q6_02 NUMERIC,
        q6_03  NUMERIC,
        q6_04 NUMERIC,
        q6_05 NUMERIC,
        q6_06 NUMERIC,
        q6_07 NUMERIC,
        q6_08 NUMERIC,
        q6_09 NUMERIC,
        q8a_01 NUMERIC,
        q8a_02 NUMERIC,
        q8a_03 NUMERIC,
        q8a_04 NUMERIC,
        q8a_05 NUMERIC,
        q8a_06 NUMERIC,
        q8b_01  NUMERIC,
        q8b_02 NUMERIC,
        q8b_03 NUMERIC,
        q8b_04 NUMERIC,
        q8b_05 NUMERIC,
        q8b_06 NUMERIC,
        q8c_01 NUMERIC,
        q8c_02 NUMERIC,
        q8c_03 NUMERIC,
        q8c_04 NUMERIC,
        q8c_05 NUMERIC,
        q8d_01 NUMERIC,
        q8d_02 NUMERIC,
        q8d_03 NUMERIC,
        q8d_04 NUMERIC,
        q8d_05 NUMERIC,
        q8e_01 NUMERIC,
        q8e_02 NUMERIC,
        q8e_03 NUMERIC,
        q8e_04 NUMERIC,
        q8f_01 NUMERIC,
        q8f_02 NUMERIC,
        q8f_03 NUMERIC,
        q8f_04 NUMERIC,
        q8f_05 NUMERIC,
        q8f_06 NUMERIC,
        q8f_07 NUMERIC,
        q8g_01 NUMERIC,
        q8g_02 NUMERIC,
        q8g_03 NUMERIC,
        q8g_04 NUMERIC,
        q8g_05 NUMERIC,
        q8g_06 NUMERIC,
        q8g_07 NUMERIC,
        gender_id   NUMERIC,
        age_id   NUMERIC,
        occupation_id   NUMERIC,
        income_id  NUMERIC,
        pos_score DECIMAL,
        neg_score DECIMAL,
        location TEXT,
        location_id NUMERIC,

        time_id						NUMERIC,
        channel_id NUMERIC,
        country_id NUMERIC,
        year						TIMESTAMP WITHOUT TIME ZONE,
        gender TEXT,
        age TEXT,
        occupation TEXT,
        income TEXT,
        channel TEXT,
        people_score DECIMAL,
        process_score DECIMAL,
        product_score DECIMAL,
        intent_score DECIMAL,
        loyalty_score DECIMAL,
        values_score DECIMAL,
        VERSION_ID VARCHAR(100),
        all_pathos   TEXT,

		EMOTIONS_SCORE						DECIMAL,
		DRIVERS_SCORE						DECIMAL,
		EMOTIONAL_ENGAGEMENT_SCORE			DECIMAL,
        n_emotional_engagement_score        DECIMAL, 
        emotional_engagement_quintile       TEXT,
	
		EMOTIONS_Surprised_SCORE			DECIMAL DEFAULT 0.00001,
		EMOTIONS_Happy_SCORE				DECIMAL DEFAULT 0.00001,
		EMOTIONS_Excited_SCORE 				DECIMAL DEFAULT 0.00001,
		EMOTIONS_Sad_SCORE					DECIMAL DEFAULT 0.00001,
		EMOTIONS_Disgust_SCORE				DECIMAL DEFAULT 0.00001,	
		EMOTIONS_Angry_SCORE				DECIMAL DEFAULT 0.00001,	
		EMOTIONS_Fear_SCORE					DECIMAL DEFAULT 0.00001,		
		EMOTIONS_Frustration_SCORE			DECIMAL DEFAULT 0.00001,

		DRIVERS_Personalization_SCORE				DECIMAL DEFAULT 0.00001,		
		DRIVERS_Positivity_SCORE					DECIMAL DEFAULT 0.00001,		
		DRIVERS_Well_being_SCORE					DECIMAL DEFAULT 0.00001,		
		DRIVERS_Exciting_SCORE					    DECIMAL DEFAULT 0.00001,		
		DRIVERS_Belonging_SCORE					    DECIMAL DEFAULT 0.00001,		
		DRIVERS_Sustainability_SCORE				DECIMAL DEFAULT 0.00001,		
		DRIVERS_Security_SCORE					    DECIMAL DEFAULT 0.00001,		
		DRIVERS_Convenience_SCORE					DECIMAL DEFAULT 0.00001,		
		DRIVERS_Care_SCORE					        DECIMAL DEFAULT 0.00001,		
		DRIVERS_Timeliness_SCORE					DECIMAL DEFAULT 0.00001,		
		DRIVERS_Respect_SCORE					    DECIMAL DEFAULT 0.00001,		
		DRIVERS_Knowledge_SCORE					    DECIMAL DEFAULT 0.00001,		
		DRIVERS_Fairness_SCORE					    DECIMAL DEFAULT 0.00001,		
		DRIVERS_Channel_Satisfaction_SCORE			DECIMAL DEFAULT 0.00001,		
		DRIVERS_Outcome_SCORE					    DECIMAL DEFAULT 0.00001,		
		DRIVERS_Communication_SCORE					DECIMAL DEFAULT 0.00001,		
		DRIVERS_Ease_of_access_SCORE				DECIMAL DEFAULT 0.00001,		
		DRIVERS_Future_issues_SCORE					DECIMAL DEFAULT 0.00001,		
		DRIVERS_Competence_SCORE					DECIMAL DEFAULT 0.00001,		
		DRIVERS_Extra_mile_SCORE					DECIMAL DEFAULT 0.00001,		
		DRIVERS_Waiting_time_SCORE					DECIMAL DEFAULT 0.00001,		
		DRIVERS_Appeal_SCORE					    DECIMAL DEFAULT 0.00001,		
		DRIVERS_Preferred_chanel_SCORE				DECIMAL DEFAULT 0.00001,		
		DRIVERS_Freshness_SCORE					    DECIMAL DEFAULT 0.00001,		
		DRIVERS_Trust_SCORE					        DECIMAL DEFAULT 0.00001,		
		DRIVERS_Regret_SCORE					    DECIMAL DEFAULT 0.00001,		
		DRIVERS_Relationship_SCORE					DECIMAL DEFAULT 0.00001,		
		DRIVERS_Innovative_SCORE					DECIMAL DEFAULT 0.00001,		
		DRIVERS_Value_for_Money_SCORE				DECIMAL DEFAULT 0.00001,		
		DRIVERS_Packaging_SCORE					    DECIMAL DEFAULT 0.00001,		

		BEHAVIOUR_ENABLERL_POSITIVE_SCORE	DECIMAL,
		BEHAVIOUR_QUAL_POSITIVE_COMMENTS		VARCHAR(4000),
		BEHAVIOUR_ENABLERL_NEGATIVE_SCORE	DECIMAL,	
		BEHAVIOUR_QUAL_NEGATIVE_COMMENTS		VARCHAR(4000),	
        PREDICTED_DRIVER			TEXT,
        predicted_driver_ppp TEXT,
        predicted_driver_ilv TEXT,
        iso_code TEXT,
        
        pathos_ref_prd_rel_unique_id TEXT,
        
        q6d TEXT,
        q8a_11 TEXT,
        q5bj TEXT,
        q6j TEXT,
        q5ab TEXT,
        q6g TEXT,
        q5ae TEXT,
        q5br TEXT,
        q8a_10 TEXT,
        q6c TEXT,
        q5bq TEXT,
        q5bk TEXT,
        q5bp TEXT,
        q5ac TEXT,
        reviews TEXT,
        jurisdiction TEXT,
        q5ag TEXT,
        q5bl TEXT,
        q5ad TEXT,
        q6e TEXT,
        q5aa TEXT,
        q5ai TEXT,
        q6k TEXT,
        q6f TEXT,
        q5ah TEXT,
        q5af TEXT,
        q6l TEXT,
	
		BEHAVIOUR_SCORE						DECIMAL,
		POSITIVE_SCORE_BY_WEIGHTS			DECIMAL,	
		NEGATIVE_SCORE_BY_WEIGHTS			DECIMAL,
		CREATED_ON 	TIMESTAMP WITH TIME ZONE 	DEFAULT CURRENT_TIMESTAMP NOT NULL,
		CREATED_BY 	VARCHAR(100)				NOT NULL DEFAULT 'System',
		UPDATED_ON 	TIMESTAMP WITH TIME ZONE 	DEFAULT CURRENT_TIMESTAMP NOT NULL,
		UPDATED_BY 	VARCHAR(100)				NOT NULL DEFAULT 'System'
);
'''.format(current_schema)
    
engine.execute(schema_final_string)


# In[ ]:


#import os
#os.system('jupyter nbconvert --to python set_iccs_table_schema.ipynb')


# In[ ]:




