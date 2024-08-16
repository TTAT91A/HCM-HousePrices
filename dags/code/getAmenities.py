import overpy
import threading

from extensions import *

def pre_processing(df):
    cols_to_fillna = ['no_hospital_1km','no_school_1km','no_cafe_1km','no_restaurant_1km','no_atm_1km','no_bank_1km','no_supermarket_1km','no_marketplace_1km','no_pharmacy_1km','no_fuel_1km']
    df[cols_to_fillna] = df[cols_to_fillna].fillna(0)
    
    # Remove space beginning of address
    df['address'] = df['address'].str.lstrip()
    df['address_district'] = df['address'].str.split(", ").str.get(-2)
    df['address_ward'] = df['address'].str.split(", ").str.get(-3)
    df['address_street'] = df['address'].str.split(", ").str.get(-4)
    df.loc[df['address_district'] == 'TP. Thủ Đức', 'address_district'] = 'Quận Thủ Đức (TP. Thủ Đức)'
    df['address'] = df['address_street'] + ', ' + df['address_ward'] + ', ' + df['address_district'] + ', ' + 'TPHCM'

    #bedroom, wc
    df[['bedroom','wc']] = df[['bedroom','wc']].fillna(1)

    #latitude, longitude
    def find_lat_lon(df, address_distric, address_ward, address_street):
        lat_lon_df = df[df['address_district'].str.contains(address_distric) & df['address_ward'].str.contains(address_ward) & df['address_street'].str.contains(address_street)]
        if lat_lon_df['latitude'].isnull().all():
            lat_lon_df = df[df['address_district'].str.contains(address_distric) & df['address_ward'].str.contains(address_ward)]
            if lat_lon_df['latitude'].isnull().all():
                lat_lon_df = df[df['address_district'].str.contains(address_distric)]
        lat = lat_lon_df['latitude'].mean()
        lon = lat_lon_df['longitude'].mean()
        return lat, lon

    lat_lon_null = df[df['latitude'].isnull() & df['longitude'].isnull()]
    for index, row in lat_lon_null.iterrows():
        address_distric = row['address_district']
        address_ward = row['address_ward']
        address_street = row['address_street']

        address_distric = address_distric.replace("(","\(")
        address_distric = address_distric.replace(")","\)")
        address_ward = address_ward.replace("(","\(")
        address_ward = address_ward.replace(")","\)")
        address_street = address_street.replace("(","\(")
        address_street = address_street.replace(")","\)")
        lat, lon = find_lat_lon(df, address_distric, address_ward, address_street)
        df.loc[index,'latitude'] = lat
        df.loc[index,'longitude'] = lon
    
    df.rename(columns={'witdh':'width'},inplace=True)
    df.loc[~df['area_used'].isnull() & df['area'].isnull(), 'area'] = df['area_used']
    df.fillna(1, inplace=True)

    index_outlier = df[(df['price(billionVND)'] > 1000) & ((df['area'] < 400) | (df['area_used'] < 400))].index
    df.drop(index_outlier, inplace=True)
    df.drop(columns=['address_district','address_ward','address_street'], inplace=True)
    
    df['no_hospital_1km'] = df['no_hospital_1km'].astype('float64')
    df['no_school_1km'] = df['no_school_1km'].astype('float64')
    df['no_cafe_1km'] = df['no_cafe_1km'].astype('float64')
    df['no_restaurant_1km'] = df['no_restaurant_1km'].astype('float64')
    df['no_atm_1km'] = df['no_atm_1km'].astype('float64')
    df['no_bank_1km'] = df['no_bank_1km'].astype('float64')
    df['no_supermarket_1km'] = df['no_supermarket_1km'].astype('float64')
    df['no_marketplace_1km'] = df['no_marketplace_1km'].astype('float64')
    df['no_pharmacy_1km'] = df['no_pharmacy_1km'].astype('float64')
    df['no_fuel_1km'] = df['no_fuel_1km'].astype('float64')

def get_new_info(latitude, longitude, obj):
    # Initialize the Overpass API client
    api = overpy.Overpass()

    # Define the search radius in meters
    radius = 1000

    # Create an Overpass QL query to find schools within the radius of the given point
    query = """
    (
    node["amenity"="{obj}"](around:{radius},{lat},{lon});
    way["amenity"="{obj}"](around:{radius},{lat},{lon});
    relation["amenity"="{obj}"](around:{radius},{lat},{lon});
    );
    out center;
    """.format(radius=radius, lat=latitude, lon=longitude, obj=obj)

    # Execute the query
    result = api.query(query)

    # Extract and count the names of the objects
    count = result.nodes + result.ways + result.relations

    return len(count)

obj_arr = ['school', 'hospital', 'restaurant', 'cafe', 'bank', 'atm', 'marketplace', 'supermarket', 'fuel', 'pharmacy']

def helper(df, obj):
    new_col = f"no_{obj}_1km"
    df[new_col] = df.apply(lambda row: get_new_info(row['latitude'], row['longitude'], obj), axis=1)
    print(f"Process {obj} done")

# for obj in obj_arr:
#     helper(df, obj)

def overpass(house_info_processed_path, output_path):
    df = pd.read_csv(house_info_processed_path, dtype={'phone': str})
    obj_arr = ['school', 'hospital', 'restaurant', 'cafe', 'bank', 'atm', 'marketplace', 'supermarket', 'fuel', 'pharmacy']

    p1 = threading.Thread(target=helper, args=(df, obj_arr[0]))

    p2 = threading.Thread(target=helper, args=(df, obj_arr[1]))

    p3 = threading.Thread(target=helper, args=(df, obj_arr[2]))

    p4 = threading.Thread(target=helper, args=(df, obj_arr[3]))

    p5 = threading.Thread(target=helper, args=(df, obj_arr[4]))

    p6 = threading.Thread(target=helper, args=(df, obj_arr[5]))

    p7 = threading.Thread(target=helper, args=(df, obj_arr[6]))

    p8 = threading.Thread(target=helper, args=(df, obj_arr[7]))

    p9 = threading.Thread(target=helper, args=(df, obj_arr[8]))

    p10 = threading.Thread(target=helper, args=(df, obj_arr[9]))

    p1.start()
    p2.start()

    # check if the thread is done
    p1.join()
    p2.join()

    p3.start()
    p4.start()

    # check if the thread is done
    p3.join()
    p4.join()

    p5.start()
    p6.start()

    # check if the thread is done
    p5.join()
    p6.join()

    p7.start()
    p8.start()

    # check if the thread is done
    p7.join()
    p8.join()

    p9.start()
    p10.start()

    # check if the thread is done
    p9.join()
    p10.join()

    pre_processing(df)
    df.to_csv(output_path, index=False)

if __name__ == '__main__':
    folder_path = os.path.join(os.path.dirname(__file__))
    dags_folder = os.path.dirname(folder_path)

    all_files_github = get_all_files(repo_name='Mogi_HousePrices_Pipeline')

    processed_name = f'processed({today}).csv'
    processed_path_github = "dags/RawData/ProcessedData" + processed_name
    if processed_path_github in all_files_github:
        overpass_name = f'overpass({today}).csv'

        # processed_path = "https://raw.githubusercontent.com/TTAT91A/Mogi_HousePrices_Pipeline/main/dags/data/" + processed_name
        input_path = "https://raw.githubusercontent.com/" + GITHUB_USERNAME + "/" + REPO_NAME + "/main/" + processed_path_github
        dest_path = dags_folder + "/RawData/OverpassData/" + overpass_name #path on local

        # output_path = "/kaggle/working/" + overpass_name
        overpass(input_path, dest_path)

        pushToGithub(git_path="dags/RawData/OverpassData" ,local_file_path=dest_path, file_name=overpass_name, repo_name=REPO_NAME)
    else:
        print(f"{processed_path_github} not found")