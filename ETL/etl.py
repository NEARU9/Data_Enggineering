import requests
import pandas as pd
from sqlalchemy import create_engine

def extract_data() -> dict:
    """This API extracts data from http://universities.hipolabs.com"""
    
    API_URL = "http://universities.hipolabs.com/search?country=United+States"
    data = requests.get(API_URL).json()

    return data

def transform(data: dict) -> pd.DataFrame:
    """Transforms the dataset into desired structure and filters"""

    df = pd.DataFrame(data)
    print(f"Total Number of universities from API {len(data)}")
    df = df[df["name"].str.contains("California")]
    print(f"Number of universities in California {len(df)}")
    df['domains'] = [','.join(map(str, l)) for l in df['domains']]
    df['web_pages'] = [','.join(map(str, l)) for l in df['web_pages']]
    df = df.reset_index(drop=True)
    return df[["domains", "country", "web_pages", "name"]]


"creates a connection to a SQLite database named my_lite_store.db"
# def load(df: pd.DataFrame) -> None:
#     """Loads data into a SQLite database"""
    
#     disk_engine = create_engine('sqlite:///my_lite_store.db')
#     df.to_sql('cal_uni', disk_engine, if_exists='replace')

# # Run the ETL process
# data = extract_data()
# df = transform(data)
# load(df)

"Save To Csv"
def load_to_csv(df: pd.DataFrame, file_name: str) -> None:
    """Saves the transformed data into a CSV file"""
    
    df.to_csv(file_name, index=False)
    print(f"Data saved to {file_name}")

# Run the ETL process
data = extract_data()
df = transform(data)
load_to_csv(df, "universities_in_california.csv")

# print (df)