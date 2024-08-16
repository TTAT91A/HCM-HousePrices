from pymongo import MongoClient
from extensions import *

def import_csv_to_mongodb(df, database_name='House_prices', collection_name = "HCMCity", mongo_uri='mongodb+srv://nattan1811:taibitri123@cluster0.voqacs7.mongodb.net/'):
# mongodb+srv://nattan1811:<password>@cluster0.voqacs7.mongodb.net/
    # Read CSV into a Pandas DataFrame

    # Connect to MongoDB
    client = MongoClient(mongo_uri)
    db = client[database_name]
    collection = db[collection_name]

    # Convert DataFrame to dictionary for easier MongoDB insertion
    data = df.to_dict(orient='records')

    try:
        # Insert data into MongoDB
        collection.insert_many(data)
        print("Insert data to MongoDB successfully")
    except:
        print("Insert data to MongoDB failed")

    # Close MongoDB connection
    client.close()

if __name__ == '__main__':
    # Load data
    all_files_github = get_all_files(repo_name='Mogi_HousePrices_Pipeline')

    overpass_name = f'overpass({today}).csv'
    overpass_path = "dags/RawData/OverpassData/" + overpass_name
    if overpass_path in all_files_github:
        
        # overpass_path = "https://raw.githubusercontent.com/TTAT91A/Mogi_HousePrices_Pipeline/main/dags/data1/" + overpass_name
        input_path = "https://raw.githubusercontent.com/" + GITHUB_USERNAME + "/" + REPO_NAME + "/main/" + overpass_path

        df = pd.read_csv(input_path,dtype={'phone': str})
        # pre_processing(df)
        import_csv_to_mongodb(df)
    else:
        print(f"{overpass_path} not found")