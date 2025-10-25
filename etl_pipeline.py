# Import Necessary Libraries
import pandas as pd
import os
import io
from azure.storage.blob import BlobServiceClient, BlobClient
from dotenv import load_dotenv


# Extraction Layer
lemco_df = pd.read_csv(r'lemco_logistics_data.csv')


# Data Cleaning and Transformation Layer
lemco_df.fillna({
    'Unit_Price': lemco_df['Unit_Price'].mean(),
    'Total_Cost': lemco_df['Total_Cost'].mean(),
    'Discount_Rate': 0.0,
    'Return_Reason': 'unknown'
}, inplace=True)


# Remove rows where 'Date' is missing or empty
lemco_df = lemco_df[lemco_df['Date'].notna() & (lemco_df['Date'] != '')]


lemco_df['Date'] = pd.to_datetime(lemco_df['Date'], errors='coerce')
# Optionally, drop rows where 'Date' could not be parsed
lemco_df = lemco_df[lemco_df['Date'].notna()]


lemco_df['Date'] = pd.to_datetime(lemco_df['Date'],)


# Customer Table
customer = lemco_df[['Customer_ID', 'Customer_Name', 'Customer_Phone', 'Customer_Email', 'Customer_Address']].copy().drop_duplicates().reset_index(drop=True)


# Products Table
products = lemco_df[['Product_ID', 'Quantity', 'Unit_Price', 'Total_Cost', 'Discount_Rate','Product_List_Title']].copy().drop_duplicates().reset_index(drop=True)


# Transactions_Fact_ Table

transaction_fact = lemco_df.merge(customer, on=['Customer_ID','Customer_Name','Customer_Phone','Customer_Email', 'Customer_Address'], how='left') \
                     .merge(products, on=['Product_ID', 'Quantity','Product_List_Title','Unit_Price','Total_Cost', 'Discount_Rate'], how='left') \
                     [['Transaction_ID', 'Date', 'Customer_ID', 'Product_ID','Sales_Channel','Order_Priority', \
                         'Warehouse_Code', 'Ship_Mode', 'Delivery_Status','Customer_Satisfaction', 'Item_Returned', 'Return_Reason', \
                         'Payment_Type', 'Taxable', 'Region', 'Country']]
                     
                     
transaction_fact['Date'] = transaction_fact['Date'].astype('datetime64[ns]')


# Temprary Loading
customer.to_csv(r'dataset/customer.csv', index=False)
products.to_csv(r'dataset/products.csv', index=False)
transaction_fact.to_csv(r'dataset/transaction_fact.csv', index=False)

print ('files have been loaded temporarily into the local machine successfully')


# Data Loading
# Azure blob connection
load_dotenv()
connect_str = os.getenv('CONNECT_STR')
blob_service_client = BlobServiceClient.from_connection_string(connect_str)

container_name = os.getenv('CONTAINER_NAME')
container_client = blob_service_client.get_container_client(container_name)


# Create a function to upload files to Azure Blob Storage as a parquet file
def upload_df_to_blob_as_parquet(df, container_client, blob_name):
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)
    blob_client = container_client.get_blob_client(blob_name)
    blob_client.upload_blob(buffer, blob_type="BlockBlob", overwrite=True)
    print(f"{blob_name} uploaded to Azure Blob Storage successfully.")
    
    
upload_df_to_blob_as_parquet(customer, container_client, 'rawdata/customer.parquet')
upload_df_to_blob_as_parquet(products, container_client, 'rawdata/products.parquet')
upload_df_to_blob_as_parquet(transaction_fact, container_client, 'rawdata/transaction_fact.parquet')
                     
