#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pathos_config import *


# # Loading Schemas

# In[2]:


schema_final_string='''create SCHEMA IF NOT EXISTS pathos_rb_schema;
'''   
engine.execute(schema_final_string)


# # Dimension Schema

# ## PATHOS_CL_MASTER_DIM_MAPPING

# In[3]:


schema_final_string='''create table  IF NOT EXISTS pathos_rb_schema.PATHOS_CL_MASTER_DIM_MAPPING(
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
''' 
    
engine.execute(schema_final_string)


# ## Brand Dimension

# In[4]:


schema_final_string='''create table  IF NOT EXISTS pathos_rb_schema.PATHOS_CL_MASTER_BRAND(
		DIM_VAL_ID 					NUMERIC 						PRIMARY KEY,
		DIM_VALUE_NAME					VARCHAR(100) 				UNIQUE NOT NULL,	
		DIM_VALUE_DESC					VARCHAR(100) 				,	
		DIM_ACTIVE_REPORTING			CHAR(1) 					NOT NULL,	
		DIM_ACTIVE_PROCESSSING			CHAR(1) 					,		
		CHILD_RECORD					CHAR(1) 					,		
		PARENT_RECORD_ID 				NUMERIC						DEFAULT NULL, 			
		HIERARCHY_LEVEL					NUMERIC						NOT NULL,			
		CREATED_ON 	TIMESTAMP WITH TIME ZONE 	NOT NULL DEFAULT NOW(),
		CREATED_BY 	VARCHAR(100)				NOT NULL DEFAULT 'System',
		UPDATED_ON 	TIMESTAMP WITH TIME ZONE 	NOT NULL DEFAULT NOW(),
		UPDATED_BY 	VARCHAR(100)				NOT NULL DEFAULT 'System'
);
''' 
    
engine.execute(schema_final_string)


# ## Manufacturer dim

# In[5]:


schema_final_string='''create table  IF NOT EXISTS pathos_rb_schema.PATHOS_CL_MASTER_MANUFACTURER(
		DIM_VAL_ID 					NUMERIC 						PRIMARY KEY,
		DIM_VALUE_NAME					VARCHAR(100) 				UNIQUE NOT NULL,	
		DIM_VALUE_DESC					VARCHAR(100) 				,	
		DIM_ACTIVE_REPORTING			CHAR(1) 					NOT NULL,	
		DIM_ACTIVE_PROCESSSING			CHAR(1) 					,		
		CHILD_RECORD					CHAR(1) 					,		
		PARENT_RECORD_ID 				NUMERIC						DEFAULT NULL, 			
		HIERARCHY_LEVEL					NUMERIC						NOT NULL,			
		CREATED_ON 	TIMESTAMP WITH TIME ZONE 	NOT NULL DEFAULT NOW(),
		CREATED_BY 	VARCHAR(100)				NOT NULL DEFAULT 'System',
		UPDATED_ON 	TIMESTAMP WITH TIME ZONE 	NOT NULL DEFAULT NOW(),
		UPDATED_BY 	VARCHAR(100)				NOT NULL DEFAULT 'System'
);
''' 
    
engine.execute(schema_final_string)


# ## Pathos Version Dimension

# In[6]:


schema_final_string='''create table  IF NOT EXISTS pathos_rb_schema.pathos_cl_master_prod_ver(
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
''' 
    
engine.execute(schema_final_string)


# ## PATHOS_CL_MASTER_TIME

# In[7]:


schema_final_string='''create table  IF NOT EXISTS pathos_rb_schema.PATHOS_CL_MASTER_TIME(
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
''' 
    
engine.execute(schema_final_string)


# ## PATHOS_CL_MASTER_PRODUCT

# In[8]:


schema_final_string='''create table  IF NOT EXISTS pathos_rb_schema.PATHOS_CL_MASTER_PRODUCT(
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
''' 
    
engine.execute(schema_final_string)


# ## PATHOS_CL_MASTER_GMO

# In[9]:


schema_final_string='''create table  IF NOT EXISTS pathos_rb_schema.PATHOS_CL_MASTER_GMO(
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
''' 
    
engine.execute(schema_final_string)


# ## PATHOS_CL_MASTER_SOURCE

# In[10]:


schema_final_string='''create table  IF NOT EXISTS pathos_rb_schema.PATHOS_CL_MASTER_SOURCE(
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
''' 
    
engine.execute(schema_final_string)


# ## PATHOS_CL_MASTER_COUNTRY

# In[11]:


schema_final_string='''create table  IF NOT EXISTS pathos_rb_schema.PATHOS_CL_MASTER_COUNTRY(
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
''' 
    
engine.execute(schema_final_string)


# ## PATHOS_CL_MASTER_CHANNEL

# In[12]:


schema_final_string='''create table  IF NOT EXISTS pathos_rb_schema.PATHOS_CL_MASTER_CHANNEL(
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
''' 
    
engine.execute(schema_final_string)


# ## pathos_cl_master_personas

# In[13]:


schema_final_string='''create table  IF NOT EXISTS pathos_rb_schema.pathos_cl_master_personas(
		PERSONA_ID 					NUMERIC 						PRIMARY KEY,
		PERSONA_NAME					VARCHAR(100) 				UNIQUE NOT NULL,	
		PERSONA					VARCHAR(100) 				UNIQUE NOT NULL,
		VERSION_ID					VARCHAR(100) 				,	
		BRAND_ID			VARCHAR(100) 					,	
		MANUFACTURER_ID			VARCHAR(100) 					,
        PRODUCT_ID			VARCHAR(100) 					,
        GMO_ID			VARCHAR(100) 					,
        SOURCE_ID			VARCHAR(100) 					,
        TIME_ID			VARCHAR(100) 					,
        CHANNEL_ID			VARCHAR(100) 					,
        COUNTRY_ID			VARCHAR(100) 					,
		CREATED_ON 	TIMESTAMP WITH TIME ZONE 	DEFAULT CURRENT_TIMESTAMP NOT NULL,
		CREATED_BY 	VARCHAR(100)				NOT NULL DEFAULT 'System',
		UPDATED_ON 	TIMESTAMP WITH TIME ZONE 	DEFAULT CURRENT_TIMESTAMP NOT NULL,
		UPDATED_BY 	VARCHAR(100)				NOT NULL DEFAULT 'System'
);
''' 
  
engine.execute(schema_final_string)


# In[ ]:





# # Generic schema

# ## PATHOS_CL_TRANSACTION_TABLE

# In[14]:


schema_final_string='''create table  IF NOT EXISTS pathos_rb_schema.RB_TRANSACTION_TABLE(
		PATHOS_TRANSACTION_ID 				NUMERIC 					PRIMARY KEY,
        review_id    TEXT,
		CLIENT_TRANSACTION_ID 				VARCHAR(100) 				UNIQUE,
		CLIENT_TRANSACTION_DATE 			VARCHAR(100) 			,
		MESSAGE_DESC						TEXT					,
		COUNTRY 			VARCHAR(100) 			,   
		CHANNEL 			VARCHAR(100) 			,        
		BRAND_ID						NUMERIC,
		MANUFACTURER_ID						NUMERIC,
        product_id						NUMERIC,
        gmo_id						NUMERIC,
        source_id						NUMERIC,
        time_id						NUMERIC,
        channel_id NUMERIC,
        country_id NUMERIC,
        year						NUMERIC,
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
        analysis_completed BOOLEAN DEFAULT FALSE,
	
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
''' 
    
engine.execute(schema_final_string)


# ## PATHOS_CL_SUMMARY_TABLE

# In[15]:


schema_final_string='''create table  IF NOT EXISTS pathos_rb_schema.RB_SUMMARY_TABLE(
		PATHOS_SUMMARY_ID 				NUMERIC 					PRIMARY KEY,
        review_id    TEXT,
		CLIENT_TRANSACTION_ID 				VARCHAR(100) 				UNIQUE,
		CLIENT_TRANSACTION_DATE 			VARCHAR(100) 			,
		MESSAGE_DESC						TEXT					,
		COUNTRY 			VARCHAR(100) 			,   
		CHANNEL 			VARCHAR(100) 			,          
		BRAND_ID						NUMERIC,
		MANUFACTURER_ID						NUMERIC,
        product_id						NUMERIC,
        gmo_id						NUMERIC,
        source_id						NUMERIC,
        time_id						NUMERIC,
        year						NUMERIC,
        channel_id NUMERIC,
        country_id NUMERIC,
        people_score DECIMAL,
        process_score DECIMAL,
        product_score DECIMAL,
        intent_score DECIMAL,
        loyalty_score DECIMAL,
        values_score DECIMAL,
        VERSION_ID VARCHAR(100),
        all_pathos   TEXT,
        brand   TEXT,
        manufacturer   TEXT,
        gmo   TEXT,
        product   TEXT,
        version   TEXT,
        source   TEXT,

		EMOTIONS_SCORE						DECIMAL,
		DRIVERS_SCORE						DECIMAL,
		EMOTIONAL_ENGAGEMENT_SCORE			DECIMAL,
        n_emotional_engagement_score        DECIMAL, 
        emotional_engagement_quintile       TEXT,
	
		EMOTIONS_Surprised_SCORE			DECIMAL NOT NULL DEFAULT 0,
		EMOTIONS_Happy_SCORE				DECIMAL NOT NULL DEFAULT 0,
		EMOTIONS_Excited_SCORE 				DECIMAL NOT NULL DEFAULT 0,
		EMOTIONS_Sad_SCORE					DECIMAL NOT NULL DEFAULT 0,
		EMOTIONS_Disgust_SCORE				DECIMAL NOT NULL DEFAULT 0,	
		EMOTIONS_Angry_SCORE				DECIMAL NOT NULL DEFAULT 0,	
		EMOTIONS_Fear_SCORE					DECIMAL NOT NULL DEFAULT 0,		
		EMOTIONS_Frustration_SCORE			DECIMAL NOT NULL DEFAULT 0,

		DRIVERS_Personalization_SCORE				DECIMAL NOT NULL DEFAULT 0,		
		DRIVERS_Positivity_SCORE					DECIMAL NOT NULL DEFAULT 0,		
		DRIVERS_Well_being_SCORE					DECIMAL NOT NULL DEFAULT 0,		
		DRIVERS_Exciting_SCORE					    DECIMAL NOT NULL DEFAULT 0,		
		DRIVERS_Belonging_SCORE					    DECIMAL NOT NULL DEFAULT 0,		
		DRIVERS_Sustainability_SCORE				DECIMAL NOT NULL DEFAULT 0,		
		DRIVERS_Security_SCORE					    DECIMAL NOT NULL DEFAULT 0,		
		DRIVERS_Convenience_SCORE					DECIMAL NOT NULL DEFAULT 0,		
		DRIVERS_Care_SCORE					        DECIMAL NOT NULL DEFAULT 0,		
		DRIVERS_Timeliness_SCORE					DECIMAL NOT NULL DEFAULT 0,		
		DRIVERS_Respect_SCORE					    DECIMAL NOT NULL DEFAULT 0,		
		DRIVERS_Knowledge_SCORE					    DECIMAL NOT NULL DEFAULT 0,		
		DRIVERS_Fairness_SCORE					    DECIMAL NOT NULL DEFAULT 0,		
		DRIVERS_Channel_Satisfaction_SCORE			DECIMAL NOT NULL DEFAULT 0,		
		DRIVERS_Outcome_SCORE					    DECIMAL NOT NULL DEFAULT 0,		
		DRIVERS_Communication_SCORE					DECIMAL NOT NULL DEFAULT 0,		
		DRIVERS_Ease_of_access_SCORE				DECIMAL NOT NULL DEFAULT 0,		
		DRIVERS_Future_issues_SCORE					DECIMAL NOT NULL DEFAULT 0,		
		DRIVERS_Competence_SCORE					DECIMAL NOT NULL DEFAULT 0,		
		DRIVERS_Extra_mile_SCORE					DECIMAL NOT NULL DEFAULT 0,		
		DRIVERS_Waiting_time_SCORE					DECIMAL NOT NULL DEFAULT 0,		
		DRIVERS_Appeal_SCORE					    DECIMAL NOT NULL DEFAULT 0,		
		DRIVERS_Preferred_chanel_SCORE				DECIMAL NOT NULL DEFAULT 0,		
		DRIVERS_Freshness_SCORE					    DECIMAL NOT NULL DEFAULT 0,		
		DRIVERS_Trust_SCORE					        DECIMAL NOT NULL DEFAULT 0,		
		DRIVERS_Regret_SCORE					    DECIMAL NOT NULL DEFAULT 0,		
		DRIVERS_Relationship_SCORE					DECIMAL NOT NULL DEFAULT 0,		
		DRIVERS_Innovative_SCORE					DECIMAL NOT NULL DEFAULT 0,		
		DRIVERS_Value_for_Money_SCORE				DECIMAL NOT NULL DEFAULT 0,		
		DRIVERS_Packaging_SCORE					    DECIMAL NOT NULL DEFAULT 0,		

	
		BEHAVIOUR_ENABLERL_POSITIVE_SCORE	DECIMAL,
		BEHAVIOUR_QUAL_POSITIVE_COMMENTS		VARCHAR(4000),
		BEHAVIOUR_ENABLERL_NEGATIVE_SCORE	DECIMAL,	
		BEHAVIOUR_QUAL_NEGATIVE_COMMENTS		VARCHAR(4000),	
	
		BEHAVIOUR_SCORE						DECIMAL,
		POSITIVE_SCORE_BY_WEIGHTS			DECIMAL,	
		NEGATIVE_SCORE_BY_WEIGHTS			DECIMAL,
        PREDICTED_DRIVER			TEXT,
        predicted_driver_ppp TEXT,
        predicted_driver_ilv TEXT,
        
        DATE TIMESTAMP WITHOUT TIME ZONE,

		CREATED_ON 	TIMESTAMP WITH TIME ZONE 	DEFAULT CURRENT_TIMESTAMP NOT NULL,
		CREATED_BY 	VARCHAR(100)				NOT NULL DEFAULT 'System',
		UPDATED_ON 	TIMESTAMP WITH TIME ZONE 	DEFAULT CURRENT_TIMESTAMP NOT NULL,
		UPDATED_BY 	VARCHAR(100)				NOT NULL DEFAULT 'System'
);
''' 
    
engine.execute(schema_final_string)


# In[16]:


import os
os.system('jupyter nbconvert --to python set_rb_table_schema.ipynb')

