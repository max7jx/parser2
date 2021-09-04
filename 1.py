from google.oauth2 import service_account
from googleapiclient.http import MediaIoBaseDownload,MediaFileUpload
from googleapiclient.discovery import build
import pprint
import io
import os
import shutil
import boto3
from lxml import etree

pp = pprint.PrettyPrinter(indent=4)

path = os.getcwd() 



SCOPES = ['https://www.googleapis.com/auth/drive']
SERVICE_ACCOUNT_FILE = path+'/instant-node-324410-f3cf7b7e2025.json'

credentials = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SCOPES)
service = build('drive', 'v3', credentials=credentials)

results = service.files().list(pageSize=10,
                               fields="nextPageToken, files(id, name, mimeType)").execute()
pp.pprint(results)

print(len(results.get('files')))




def download_file(fileId, fileName):                             #fileId - id файла которій хотим скачать 
    filename = fileName                                          #fileName - указать путь и название файла, который будет создан
    file_id = fileId  
    try:
        request = service.files().get_media(fileId=file_id)      
        fh = io.FileIO(filename, 'wb')
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while done is False:
            status, done = downloader.next_chunk()
            print ("Download %d%%." % int(status.progress() * 100))
    except:
        return False
    return True


def parse(file_to_parse_path, nameDataFile):                       #file_to_parse_path - путь с названием файла для парсинга     
                                                                    #nameDataFile - перемнная которая задает путь при парсинге разных файлов
    index = 1
    entr = 0
    f = file_to_parse_path
    os.makedirs(path+'/dataset/'+nameDataFile+'/data'+str(index))
    os.chdir(path+'/dataset/'+nameDataFile+'/data'+str(index))

    try:
        context = etree.iterparse(f, recover=True, events=("end", ))
        for event, elem in context:
            if elem.tag =='SUBJECT':
                entr +=1
                filename = format(str(index) + ".xml")
                with open(filename, 'ab+') as f:
                    f.write("<?xml version=\"1.0\" encoding=\"utf-8\"?>\n".encode())
                    f.write(etree.tostring(elem))
                    elem.clear()
                    if entr % 20000 == 0:
                        index +=1
                        os.makedirs(path+'/dataset/'+nameDataFile+'/data'+str(index))
                        os.chdir(path+'/dataset/'+nameDataFile+'/data'+str(index))
    except:
        return False
    return True



def upload_files(path, buck):  #path - путь к загружаемым файлам, buck - корзина на s3
    try:
        session = boto3.Session(
            aws_access_key_id='AKIA3SXBDEGP5JJIGN63',
            aws_secret_access_key='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',
            region_name='eu-west-2'
        )
        s3 = session.resource('s3')
        bucket = s3.Bucket(buck)
     
        for subdir, dirs, files in os.walk(path):
            for file in files:
                full_path = os.path.join(subdir, file)
                with open(full_path, 'rb') as data:
                    bucket.put_object(Key=full_path[len(path)+1:], Body=data)
    except:
        return False
    return True 


def del_te(path_to_tree, path_to_xml):
    try:
        shutil.rmtree(path_to_tree) 
        os.remove(path_to_xml)
    except:
        return False
    return True 

def to_zero():
    try:
        index = None 
    except:
        return False
    return True

if __name__ == "__main__":
    if download_file('1QdyuyiU1qws5GDpJllgbjuICOOzfwO5_', path+'/17.1-EX_XML_EDR_UO_FULL_16.08.2021.xml') is True:
        if parse(path+'/17.1-EX_XML_EDR_UO_FULL_16.08.2021.xml', 'UO_FULL') is True:
            if upload_files(path+'/dataset', 'datauofull') is True:
                if del_te(path+'/dataset', path+'/17.1-EX_XML_EDR_UO_FULL_16.08.2021.xml') is True:
                    if to_zero() is True:
                        if download_file('1-ZfKdRetXpqffkNGpQwrmNVbI7rYkv34', path+'/17.2-EX_XML_EDR_FOP_FULL_16.08.2021.xml') is True:
                            if parse(path+'/17.2-EX_XML_EDR_FOP_FULL_16.08.2021.xml', 'FOP_FULL') is True:
                                if upload_files(path+'/dataset', 'datafop') is True:
                                    if del_te(path+'/dataset', path+'/17.2-EX_XML_EDR_FOP_FULL_16.08.2021.xml') is True:
                                        print('Получилось!!!')
