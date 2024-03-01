import warnings
warnings.filterwarnings("ignore")

from config import *

from pandas import read_excel, options
options.mode.chained_assignment = None

from datetime import datetime as dt
from requests import post, get
from urllib.parse import quote
from bs4 import BeautifulSoup
from os  import makedirs, chdir, path
from re import search
import io
import os

print('Ingestion Process')
os.makedirs(source_folder ,exist_ok = True)
RUN_MODE = 'ALL_CORES'

def validate_last_update_date(func):
    print('Checking for updates')
    def wrapper(*args, **kwgars):
        
        def read_update_date_from_url():
            text = 'Fecha de actualización de los datos:\s+(\d{1,2}/\d{1,2}/\d+\s+\d{1,2}:\d{1,2}:\d{1,2}\s+\w+)'
            response = get('https://www.contraloria.gob.pa/CGR.PLANILLAGOB.UI/Formas/Index',verify=False).text
            extract_date = search(text, response)
            return extract_date.group(1)


        file = 'last_updated_date.txt'
        webpage_date = dt.strptime(read_update_date_from_url() ,'%d/%m/%Y %H:%M:%S %p')
        try: 
            with open(file,'r+') as f:
                last_date = dt.strptime(f.read(),'%Y-%m-%d %H:%M:%S')
        
                if last_date < webpage_date:
                    func(*args, **kwgars)
                    f.write(str(webpage_date))
                else:
                    print('no updates available')
                    
        except FileNotFoundError:
            func(*args, **kwgars)
            with open(file,'w+') as f:
                f.write(str(webpage_date))
                    
    return wrapper
            

@validate_last_update_date
def execute_ingestion():
    print('starting update')
    errors = []
    def list_of_institutions():
        return list(filter(lambda x: x not in ('',"-- Seleccione una institución --"),
            BeautifulSoup(
                            post('https://www.contraloria.gob.pa/CGR.PLANILLAGOB.UI/Formas/Index',verify=False).content, 'html.parser'
            )
                .find("select", {"id": "MainContent_ddlInstituciones"})
                    .text
                    .split('\n')
               ))

    def get_source_data(INSTITUCION:str):

        try:
            fecha_consulta = dt.now()
            file = get(f'https://www.contraloria.gob.pa/CGR.PLANILLAGOB.UI/Formas/Reporte?&Ne={quote(INSTITUCION)}&N=&A=&C=&E=',verify=False)
            bytes_file_obj = io.BytesIO()
            bytes_file_obj.write(file.content)
            bytes_file_obj.seek(0)  # set file object to start
            data = read_excel(bytes_file_obj,engine='openpyxl')

            file_name = search('filename="(.*?).xlsx"',file.headers['content-disposition']).group(1)
            informe = data.iloc[0,0]
            institucion  = data.iloc[1,0]
            fecha_act = data.iloc[1,4]

            df = data.iloc[4:].copy()
            df.columns = data.iloc[3]
            df['Salario'] = df['Salario'].astype('float')
            df['Gasto'] = df['Gasto'].astype('float')
            df['Fecha Actualizacion'] = (
                dt.strptime(
                            search('.*?:\s+(\d{2}/\d{2}/\d{4}\s+\d{1,2}:\d{1,2}:\d{1,2}\s+\w{2})',fecha_act).group(1)
                    ,'%d/%m/%Y %H:%M:%S %p')
            )
            df['Fecha Consulta'] = fecha_consulta
            df['archivo'] = file_name
            df['Institucion'] = INSTITUCION
            df.to_parquet(f'''{source_folder}/{file_name}+{str(fecha_consulta.timestamp()).replace('.','_')}.parquet''')
        except Exception as e:
            errors.append(INSTITUCION)
            print(INSTITUCION,e)

    def _run(lst_of_institutions:list,threads_mode ='SINGLE' ):
        
        if threads_mode != 'SINGLE':
            import threading
            
            
            threads = []
            
            for n,i in enumerate(lst_of_institutions):
                th = threading.Thread(target=get_source_data, args=(i,))
                threads.append(th)
                threads[n].start()
            
            for th in threads:
                th.join()
        else: 
            list(map(get_source_data,lst_of_institutions))
    
    _run(list_of_institutions(),RUN_MODE)
    
    if len(errors)>0:
        _run(errors,RUN_MODE)







execute_ingestion()



