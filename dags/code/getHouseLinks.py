from extensions import *

headers = {
  "Accept": "*/*",
  "Accept-Encoding": "gzip, deflate, br",
  "Accept-Language": "vi,en;q=0.9,en-GB;q=0.8,en-US;q=0.7",
  "Referer": "https://mogi.vn/",
  "Sec-Ch-Ua": "\"Microsoft Edge\";v=\"119\", \"Chromium\";v=\"119\", \"Not?A_Brand\";v=\"24\"",
  "Sec-Ch-Ua-Mobile": "?0",
  "Sec-Ch-Ua-Platform": "\"Windows\"",
  "Sec-Fetch-Dest": "empty",
  "Sec-Fetch-Mode": "cors",
  "Sec-Fetch-Site": "same-origin",
  "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36 Edg/119.0.0.0"
}

def get_house_link(path):
    df = pd.DataFrame(columns=['name', 'district', 'price', 'bedroom', 'wc', 'acreage', 'link', 'date'])
    thu = 0
    i = 1
    col = connect_mongodb()
    while True:
        link_root = 'https://mogi.vn/ho-chi-minh/mua-nha?cp='+str(i)
        ##
        while True:
            try:
                response = requests.get(link_root, headers= headers, timeout=20)
                if response.status_code == 200:
                    break
            except:
                continue
        soup = BeautifulSoup(response.content, 'html.parser')

        content0 = soup.find('ul', class_ = 'props').find_all('a', class_ = 'link-overlay')
        content1 = soup.find_all('div', class_ = 'prop-addr')
        content2 = soup.find_all('div', class_ = 'price')
        content3 = soup.find_all('ul', class_ = 'prop-attr')
        content4 = soup.find_all('h2', class_ = 'prop-title')
        content5 = soup.find_all('div', class_ ='prop-created')

        for x, y, z, t, u, v in zip(content0, content1, content2, content3, content4, content5):
            link = x.get('href')
            id = int(re.search(r"\-id([0-9]*)", link).group(1))
            if col.find_one({"id": id}):
                continue
            else:
                district = y.text
                price = z.text
                tmp = t.text.strip().split('\n')
                bedroom, wc, acreage = tmp[1], tmp[2], tmp[0]
                name = u.text
                day = v.text
                if day != 'Hôm nay':
                    thu = 1
                    break

                new_row = pd.Series([name, district, price, bedroom, wc, acreage, link, today], index=df.columns)
                df.loc[len(df)] = new_row
                
        if thu == 1: break
        print('page:', i)
        i += 1

    df.to_csv(path,index=None)

if __name__=='__main__':
    FILE_NAME = f'house_links({today}).csv'
    folder_path = os.path.join(os.path.dirname(__file__))
    dags_folder = os.path.dirname(folder_path)
    dest_path = dags_folder + "/RawData/HousesLinks/" + FILE_NAME

    get_house_link(dest_path) #write to file
    git_path = "dags/RawData/HouseLinks/"
    pushToGithub(git_path=git_path, local_file_path=dest_path, file_name=FILE_NAME, repo_name=REPO_NAME)
