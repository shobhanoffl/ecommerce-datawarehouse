from google.cloud import bigquery
import pandas as pd
import numpy as np
client = bigquery.Client()

job_config = bigquery.LoadJobConfig(
    schema=[
        bigquery.SchemaField("index","INTEGER","NULLABLE"),			
bigquery.SchemaField("Order_ID","STRING","NULLABLE"),			
bigquery.SchemaField("Date","STRING","NULLABLE"),			
bigquery.SchemaField("Status","STRING","NULLABLE"),			
bigquery.SchemaField("Fulfilment","STRING","NULLABLE"),			
bigquery.SchemaField("Sales_Channel","STRING","NULLABLE"),			
bigquery.SchemaField("ship_service_level","STRING","NULLABLE"),			
bigquery.SchemaField("Style","STRING","NULLABLE"),			
bigquery.SchemaField("SKU","STRING","NULLABLE"),			
bigquery.SchemaField("Category","STRING","NULLABLE"),			
bigquery.SchemaField("Size","STRING","NULLABLE"),			
bigquery.SchemaField("ASIN","STRING","NULLABLE"),			
bigquery.SchemaField("Courier_Status","STRING","NULLABLE"),			
bigquery.SchemaField("Qty","INTEGER","NULLABLE"),			
bigquery.SchemaField("currency","STRING","NULLABLE"),			
bigquery.SchemaField("Amount","FLOAT","NULLABLE"),			
bigquery.SchemaField("ship_city","STRING","NULLABLE"),			
bigquery.SchemaField("ship_state","STRING","NULLABLE"),			
bigquery.SchemaField("ship_postal_code","STRING","NULLABLE"),			
bigquery.SchemaField("ship_country","STRING","NULLABLE"),			
bigquery.SchemaField("promotion_ids","STRING","NULLABLE"),			
bigquery.SchemaField("B2B","STRING","NULLABLE"),			
bigquery.SchemaField("fulfilled_by","STRING","NULLABLE")
    ],
    skip_leading_rows=1,
    source_format=bigquery.SourceFormat.CSV,
)
uri = "gs://ecommerce_sales_new/sales_data.csv"

load_job = client.load_table_from_uri(
    uri, 'dataeng-383420.ecommerce.Sales Report', job_config=job_config
)

load_job.result()

table_ref = client.dataset('ecommerce').table('Sales Report')
try:
    desttable = client.get_table('dataeng-383420.ecommerce.Sales Report')
    print("Loaded")
except:
    table = bigquery.Table(table_ref, schema=schema)
    table = client.create_table(table)
    print("Table 'Sales Report' created in the dataset.")
    desttable = client.get_table('dataeng-383420.ecommerce.Sales Report')
    print("Loaded")

# --------------------------

# New Code to Process DF

# --------------------------

df = client.list_rows(desttable).to_dataframe()

# Cleaning Data

df = df.dropna()
df=df.drop(["Fulfilment","Sales_Channel","ship_service_level","Style","SKU","currency","ship_country","promotion_ids","B2B","fulfilled_by","Size","ASIN","Courier_Status"],axis='columns')

a=df.isnull()
for i in a:
  temp=df[i].isnull().sum()
  if(temp>0):
    print(i,temp)

mean_prices = df.groupby('Category')['Amount'].mean()

for i, row in df.iterrows():
    if pd.isnull(row['Amount']):
        category = row['Category']
        mean_price = mean_prices[category]
        df.at[i, 'Amount'] = mean_price

def clean_text(text):

    words = str(text).split(" ")
    first_word = words[0]
    if len(first_word) > 6:
        try:
          return first_word[0:6]
        except:
          return ""
    else:
        return first_word
      
df['ship_postal_code'] = df['ship_postal_code'].apply(clean_text)

df = df.dropna(subset=['ship_city','ship_state','ship_postal_code'])

# Cleaning Data

# Relation B/W Cost & Location

amount_ranges = np.percentile(df['Amount'], [0, 25, 50, 75, 100])
new_df = pd.DataFrame(columns=['Amount_Range', 'City', 'Total_Sales'])

high=max(df["Amount"])
step=high/20
curr=0

while(curr<=high):
    amount_min=curr
    curr+=step
    amount_max=curr
    pin_codes = df.loc[(df['Amount'] >= amount_min) & (df['Amount'] <= amount_max), 'ship_city']

    for pin_code in pin_codes.unique():
        total_amount = df.loc[(df['Amount'] >= amount_min) & (df['Amount'] <= amount_max) & (df['ship_city'] == pin_code), 'Amount'].sum()

        new_row = pd.DataFrame({
            'Amount_Range': [f'{int(amount_min)}-{int(amount_max)}'],
            'City': [pin_code],
            'Total_Sales': [total_amount]
        })
        new_df = pd.concat([new_df, new_row])

# Relation B/W Cost & Location

# Prizes Need to be Optimized ?

amount_ranges = np.percentile(df['Amount'], [0, 20, 40, 60, 80, 100])

new_df2 = pd.DataFrame(columns=['amount', 'category', 'total_number_of_sales'])

high=max(df["Amount"])
step=high/20
curr=0

while(curr<=high):
    amount_min=curr
    curr+=step
    amount_max=curr
    categories = df.loc[(df['Amount'] >= amount_min) & (df['Amount'] <= amount_max), 'Category']
    
    for category in categories.unique():
        total_sales = df.loc[(df['Amount'] >= amount_min) & (df['Amount'] <= amount_max) & (df['Category'] == category)].shape[0]

        new_row = pd.DataFrame({
            'amount': [f'{int(amount_min)}-{int(amount_max)}'],
            'category': [category],
            'total_number_of_sales': [total_sales]
        })
        new_df2 = pd.concat([new_df2, new_row])

# Prizes Need to be Optimized ?

# Inventory Reallocation

sales_by_location_status = df.groupby(['ship_city', 'Status']).size().reset_index(name='Total_Sales')
sales_summary = sales_by_location_status[['ship_city', 'Status', 'Total_Sales']]

# Inventory Reallocation

# Write the cleaned data back to BigQuery.

# 1st Table
print(new_df.head())
premium_prod_table = client.dataset('ecommerce').table('Premium Products')
premium_prod_job_config = bigquery.LoadJobConfig(
   schema=[
   bigquery.SchemaField("Amount_Range","STRING"),
   bigquery.SchemaField("City","STRING"),
   bigquery.SchemaField("Total_Sales","FLOAT"),
]
)
premium_prod_job_config._properties['load']['schemaUpdateOptions'] = ['ALLOW_FIELD_ADDITION']
premium_prod_job = client.load_table_from_dataframe(new_df, premium_prod_table, job_config=premium_prod_job_config)
premium_prod_job.result()
# 1st Table

# 2nd Table
new_df2['amount'] = new_df2['amount'].astype(str)
price_optimize_table = client.dataset('ecommerce').table('Price Optimize')
price_optimize_schema = [
   bigquery.SchemaField("amount","STRING"),
   bigquery.SchemaField("category","STRING"),
   bigquery.SchemaField("total_number_of_sales","INTEGER"),
]
price_optimize_job_config = bigquery.LoadJobConfig(
   schema=price_optimize_schema
)
price_optimize_job_config._properties['load']['schemaUpdateOptions'] = ['ALLOW_FIELD_ADDITION']
price_optimize_job = client.load_table_from_dataframe(new_df2, price_optimize_table, job_config=price_optimize_job_config)
price_optimize_job.result()
# 2nd Table

# 3rd Table
sales_summary['Total_Sales'] = sales_summary['Total_Sales'].astype(str)
sales_summary_table = client.dataset('ecommerce').table('Location Reallocation')
sales_summary_schema = [
   bigquery.SchemaField("ship_city","STRING","NULLABLE"),
   bigquery.SchemaField("Status","STRING","NULLABLE"),
   bigquery.SchemaField("Total_Sales","STRING","NULLABLE"),
]
sales_summary_job_config = bigquery.LoadJobConfig(
   schema=sales_summary_schema
)
sales_summary_job = client.load_table_from_dataframe(sales_summary, sales_summary_table, job_config=sales_summary_job_config)
sales_summary_job.result()
# 3rd Table