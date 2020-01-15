#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Jan  3 14:51:59 2020

@author: anhphan
"""
#---------------------------------------------------------------------------------------------------------
# import libraries
#---------------------------------------------------------------------------------------------------------

import pandas as pd
import os
import seaborn as sns
import sqlalchemy
import simplejson
import matplotlib.pyplot as plt
import numpy as np

#---------------------------------------------------------------------------------------------------------
# import the datasets
#---------------------------------------------------------------------------------------------------------

#### creating the dataset for the log modeling

os.chdir('/Users/anhphan/Python')
with open("redshift_creds.json") as fh:
    creds = simplejson.loads(fh.read())
    
connect_to_db = 'postgresql+psycopg2://' + \
            creds['user_name'] + ':' + creds['password'] + '@' + \
            creds['host_name'] + ':' + creds['port_num'] + '/' + creds['db_name'];

conn = sqlalchemy.create_engine(connect_to_db)

sql_code = """
With platform_data as (
  select distinct
         tenant_id,
         trunc(date_trunc('month', launch_date))                      start_date,
         trunc(date_trunc('month', dateadd('day', 190, launch_date))) end_date
  from public.platform
  where platform_type not in ('Brand', 'Sandbox')
),

website_visitors_data as (
  select distinct wv.tenant_id,
                  date_month,
                  start_date,
                  end_date,
                  wv.website_visitor_count,
                  row_number() over (partition by wv.tenant_id order by date_month) row_order
  from sandbox.aphan_upgrades_opp_website_visits wv
         join platform_data pd on wv.tenant_id = pd.tenant_id
       --and date_month >= start_date and date_month <= end_date
  order by tenant_id, date_month, upgrade_status
)

select tenant_id,
       website_visitor_count
from website_visitors_data
where row_order between 1 and 7

"""

df = pd.read_sql_query(sql_code,conn)


### creating the list of unique ids 
 
sql_code2 ="""
With platform_data as (
  select distinct
         tenant_id,
         trunc(date_trunc('month', launch_date))                      start_date,
         trunc(date_trunc('month', dateadd('day', 180, launch_date))) end_date
  from public.platform
  where platform_type not in ('Brand', 'Sandbox')
)

select distinct wv.tenant_id
from sandbox.aphan_upgrades_opp_website_visits wv
   join platform_data pd on wv.tenant_id = pd.tenant_id
      and date_month >= start_date and date_month <= end_date
order by tenant_id
"""

df_id = pd.read_sql_query(sql_code2,conn)


### creating the original dataset used for DataRobot
## will merge the slope and intercept to this table before exporting into a csv file
 
sql_code3 ="""
select distinct --platform_id,
       tenant_id,
       original_transaction_type,
       upgrade_type,
       upgrade_status,
       tenant_calc_user_count,
       franchise_or_independent,
       tenant_calc_active_campaign,
       tenant_calc_user_coverage_ratio,
       case when launched_to_upgrade_days <= 183 then '6 months or less'
            when launched_to_upgrade_days > 183 and launched_to_upgrade_days <= 365 then '6 months to 1 year'
            when launched_to_upgrade_days > 365 and launched_to_upgrade_days <= 548 then '1 to 1.5 year'
            when launched_to_upgrade_days > 548 and launched_to_upgrade_days <= 730 then '1.5 to 2 years'
            when launched_to_upgrade_days > 730 and launched_to_upgrade_days <= 913 then '2 to 2.5 years'
            when launched_to_upgrade_days > 913 then '2.5+ years'
        end launhed_to_upgrade_days_group,
       case when tenant_calc_certified_users >= .80 then '80 or more'
            else 'less than 80'
            end certified_user_percent,
       case when tenant_calc_all_active_leads <= 200 then '200 or less'
            when tenant_calc_all_active_leads between 201 and 400 then '201-400'
            when tenant_calc_all_active_leads between 401 and 600 then '401-600'
            when tenant_calc_all_active_leads between 601 and 00 then '601-800'
            when tenant_calc_all_active_leads between 801 and 1000 then '801-1000'
            when tenant_calc_all_active_leads between 1001 and 1200 then '1001-1200'
            when tenant_calc_all_active_leads >1200 then '1201+'
            end all_active_leads,
       case when tenant_calc_avg_wkly_leads_to_user <= 4 then '0-4'
            when tenant_calc_avg_wkly_leads_to_user between 5 and 9 then '5-9'
            when tenant_calc_avg_wkly_leads_to_user between 10 and 14 then '10-14'
            when tenant_calc_avg_wkly_leads_to_user between 15 and 19 then '15-19'
            when tenant_calc_avg_wkly_leads_to_user >= 20 then '20+'
            end avg_wkly_leads_to_user_group,
       case when tenant_calc_percent_new_leads_contacted_first_hour <0.26 then '0-25'
            when tenant_calc_percent_new_leads_contacted_first_hour >= 0.26 and tenant_calc_percent_new_leads_contacted_first_hour <0.51 then '26-50'
            when tenant_calc_percent_new_leads_contacted_first_hour >= 0.51 and tenant_calc_percent_new_leads_contacted_first_hour <0.76 then '51-75'
            when tenant_calc_percent_new_leads_contacted_first_hour >= 0.76 then '76-100'
            end percent_new_leads_contacted_first_hour_group,
       case when age_of_original_opp <= 30 then '0-30'
            when age_of_original_opp between 31 and 60 then '31-60'
            when age_of_original_opp between 61 and 90 then '61-90'
            when age_of_original_opp between 91 and 120 then '91-120'
            when age_of_original_opp > 120 then '121+'
             end parent_opps_age
 from sandbox.aphan_upgrades_opp
order by tenant_id
"""

df_data = pd.read_sql_query(sql_code3,conn)

#---------------------------------------------------------------------------------------------------------
# create log curve function to fit to the data using a closed form formula for finding the least-squares fit

# log function : y = a + b ln x
#---------------------------------------------------------------------------------------------------------

### fit a log curve to the data using a closed form formula for finding the least-squares fit

## this works but it's treating the entire dataset as a tenant
def logFit_test(x_var,y_var): 
    x = df[x_var]
    y = df[y_var]
    
    sum_y = np.sum(y)
    sum_log_x = np.sum(np.log(x))

    n = len(y)

    b = (n*np.sum(y*np.log(x)) - sum_y * sum_log_x)/(n*np.sum((np.log(x))**2) - (sum_log_x)**2)
    a = (np.sum(y) - b*sum_log_x)/(n)

    return a,b

 # logFit("month_num", "website_visitor_count")


#---------------------------------------------------------------------------------------------------------
# This is what the results should be for tenant #63
# a = 1084.039753223293
# b = 979.1863606900349
#---------------------------------------------------------------------------------------------------------


def logFit(x,y): 

    sum_y = np.sum(y)               
    sum_log_x = np.sum(np.log(x))

    n = len(y)

    b = (n*np.sum(y*np.log(x)) - sum_y * sum_log_x)/(n*np.sum((np.log(x))**2) - (sum_log_x)**2)
    a = (np.sum(y) - b*sum_log_x)/(n)

    return a,b

x_var = [1,2,3,4,5,6,7]
y_var = [194, 2826, 2828, 1880, 2876, 3327, 2005]
logFit(x_var,y_var)

a = logFit(x_var,y_var)
print(a)

#---------------------------------------------------------------------------------------------------------
# convert it a dictionary with list 
#---------------------------------------------------------------------------------------------------------
'''
data_set = {}

for i in df['tenant_id'].unique():
    data_set[i] = [(df['month_num'][j], df['website_visitor_count'][j]) for j in df[df['tenant_id']==i].index]
 '''
    
data_list = {}
for i in df['tenant_id'].unique():
    data_list[i] = [df['website_visitor_count'][j] for j in df[df['tenant_id']==i].index]
    
    
#---------------------------------------------------------------------------------------------------------
# Create a table with the results from the log function 
#---------------------------------------------------------------------------------------------------------
 df_id['website_visitor_slope']
 
 results = []
 for ids in data_list: 
     x = [1,2,3,4,5,6,7]
     y = data_list[ids]
     
     results.append(logFit(x,y))
     
  
#---------------------------------------------------------------------------------------------------------
# Append the result table to the id table 
#---------------------------------------------------------------------------------------------------------

a = []
b = []
for r in results: 
 
    a.append(r[0])
    b.append(r[1])
    
df_id["slope"] = b
df_id["intercept"] = a

#---------------------------------------------------------------------------------------------------------
# Merge the id table and the data table together
#---------------------------------------------------------------------------------------------------------

df_final = pd.merge(df_data, df_id, left_on='tenant_id', right_on = 'tenant_id', how='left').drop('tenant_id', axis=1)


#---------------------------------------------------------------------------------------------------------
# Export to a csv file 
#---------------------------------------------------------------------------------------------------------

df_final.to_csv(r'Bonnie_CSE_upgrade_profile_candidates.csv')
