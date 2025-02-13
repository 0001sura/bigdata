import pandas as pd
from sqlalchemy import create_engine

def clean_and_transform():
    # Load raw data
    raw_df = pd.read_csv(
        'data/raw/Pakistan Largest Ecommerce Dataset.csv',
        low_memory=False,
        parse_dates=['created_at', 'Working Date', 'M-Y'],
        infer_datetime_format=True
    )

    # --- Null Handling ---
    # Categorical columns
    raw_df['sales_commission_code'] = raw_df['sales_commission_code'].fillna('Unknown')
    raw_df['BI Status'] = raw_df['BI Status'].replace('#REF!', 'Unknown').fillna('Unknown')
    
    # Numerical columns
    num_cols = ['price', 'grand_total', 'discount_amount', 'qty_ordered']
    for col in num_cols:
        raw_df[col] = pd.to_numeric(raw_df[col], errors='coerce')
        raw_df[col] = raw_df[col].fillna(raw_df[col].median())  # Use median for robustness

    # --- Data Cleaning ---
    # Remove duplicates
    raw_df = raw_df.drop_duplicates()

    # Date columns
    date_cols = ['created_at', 'Working Date', 'M-Y']
    for col in date_cols:
        raw_df[col] = pd.to_datetime(raw_df[col], errors='coerce')

    # --- Column Removal ---
    cols_to_drop = [
        'MV', 'Year', 'Month', 'Customer Since', 
        'Unnamed: 19', 'Unnamed: 20', 'Unnamed: 21',
        'Unnamed: 22', 'Unnamed: 23', 'Unnamed: 24', 'Unnamed: 25'
    ]
    return raw_df.drop(columns=[c for c in cols_to_drop if c in raw_df.columns])



def load_to_postgres(cleaned_df):
    # Database connection
    engine = create_engine('postgresql://postgres:1221@localhost:5432/bigdata')
    
    # Create table if not exists
    cleaned_df.head(0).to_sql('ecommerce_data', engine, if_exists='replace', index=False)
    
    # Load in chunks (for large datasets)
    cleaned_df.to_sql('ecommerce_data', engine, if_exists='append', index=False, chunksize=10000)
    print("Data successfully loaded to PostgreSQL!")
    print(cleaned_df.shape)

if __name__ == "__main__":
    df = clean_and_transform()
    load_to_postgres(df)