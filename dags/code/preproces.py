import numpy as np
import pandas as pd
from pathlib import Path 
from extensions import *


def read_csv(text):
    df = pd.read_csv(text)
    return df
    
def duplicated(house_df):
    have_duplicate_rows = house_df.duplicated().any()
    if have_duplicate_rows == True:
        house_df.drop_duplicates(inplace=True)

def convert_to_number(text):
    total = 0
    if str(text) != 'nan':
        parts = text.split()
        if len(parts)==2:
            if parts[1]=='năm':
                total=float(parts[0])*365
            elif parts[1]=='tháng':
                total=float(parts[0])*30
            else:
                total=float(parts[0])
        elif len(parts)==4:
            if parts[1]=='năm':
                if parts[3]=='tháng':
                    total=float(parts[0])*365 + float(parts[2])*30
                else:
                    total=float(parts[0])*365 + float(parts[2])
            elif parts[1]=='tháng':
                total=float(parts[0])*30 + float(parts[2])
    else:
        return total
    return total 

def convert_data(house_df):
    #Tách giá trị diện tích ở cột area_used
    house_df['area_used']=house_df['area_used'].str.extract(r'(\d+)').astype(float)
    #Tách các giá trị diện tích, chiều dài, chiều rộng ở cột area
#     extracted_data = house_df['area'].str.extract(r'(\d+(?:\.\d+)?) m2 \((\d+(?:,\d+)?)x(\d+(?:,\d+)?)\)')
    extracted_data = house_df['area'].str.extract(r'(.*) m2 \((\d+(?:,\d+)?)x(\d+(?:,\d+)?)\)')
    house_df['area'] = extracted_data[0].str.replace(',', '.').astype(float)
    house_df['width'] = extracted_data[1]
    house_df['length'] = extracted_data[2]
    house_df['width'] = house_df['width'].str.replace(',', '.')
    house_df['length'] = house_df['length'].str.replace(',', '.')
    house_df['width'] = house_df['width'].astype(float)
    house_df['length'] = house_df['length'].astype(float)
    #Tách giá trị cột seniority
    house_df['seniority'] = house_df['seniority'].apply( convert_to_number)
    # Xử lý phone
    house_df['phone'] = pd.to_numeric(house_df['phone'], errors='coerce')
    house_df['phone'].fillna(0, inplace=True)
    house_df['phone'] = house_df['phone'].astype('int64')
    house_df['phone'] = "0" + house_df['phone'].astype(str)

def missing_value(house_df):
    house_df['area_used'] = house_df['area_used'].fillna(house_df['area'])
    house_df=house_df.dropna();
    
def save_data(text, house_df):
    filepath = Path(text)  
    filepath.parent.mkdir(parents=True, exist_ok=True)  
    house_df.to_csv(filepath, encoding='utf-8-sig',index=False); 

if __name__ == '__main__':
    folder_path = os.path.join(os.path.dirname(__file__))
    dags_folder = os.path.dirname(folder_path)

    house_info_name = f"house_info({today}).csv"
    house_info_path = "dags/RawData/HousesInfo" + house_info_name

    all_files_github = get_all_files(repo_name=REPO_NAME)
    if house_info_path in all_files_github:
        processed_name = f"processed({today}).csv"
        # input_path = "https://raw.githubusercontent.com/TTAT91A/Mogi_HousePrices_Pipeline/main/dags/data/" + house_info_name
        input_path = "https://raw.githubusercontent.com/" + GITHUB_USERNAME + "/" + REPO_NAME + "/main/" + house_info_path
        
        # output_path = "/kaggle/working/" + processed_name
        dest_path = dags_folder + "/RawData/ProcessedData/" + processed_name #path on local

        #preprocessing
        house_df = read_csv(input_path)
        duplicated(house_df)
        convert_data(house_df) 
        missing_value(house_df)
        save_data(dest_path, house_df)

        #push to github
        pushToGithub(git_path="dags/RawData/ProcessedData",local_file_path=dest_path, file_name=processed_name, repo_name=REPO_NAME)
    else:
        print(f"{house_info_path} not found")