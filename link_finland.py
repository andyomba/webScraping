from bs4 import BeautifulSoup as bs
import psycopg2 as ps
from pathlib import Path
import os
from selenium import webdriver
from selenium.webdriver.common.by import By
import time
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service as ChromeService
import asyncio
import fitz
import pandas as pd
from seleniumbase import SB
import ssl
from prefect import flow, task
# from pyvirtualdisplay import Display


# utils
from utils.CursorProcedures import connection_db
from utils.pipelines import Pipeline
from utils.db_actions import delete_table, insert_into_table_by_df
from utils.logs import logger_step
from utils.requests import request_sync
import utils.solicitudes as sl
from utils.SendEmail import send_email

# parameters
newApi = False
crop_area = (15.87404, 67.212695, 826.300475, 564.09535)
query_insert = """ INSERT INTO finland (api, company, description, applicationdate, cantapplications, file_number) VALUES %s """
table_name = 'finland'
url_general = 'https://fimea.fi/en/marketing_authorisations/marketing_authorisation_application/pending_marketing_authorisation_applications'


# @task(log_prints=True)
def process_blocks(blocks):
    data = []
    api_values = []
    firstRegistry = True
    for block in blocks:
        additional_info = []
        for line in block['lines']:
            for span in line['spans']:
                if firstRegistry:
                    if span['flags'] != 20:
                        raise ValueError('El primer registro de la página no es Azul/Negrita.')
                    else:
                        firstRegistry = False
                if span['flags'] == 20:
                    newApi = True
                    api_values.append(span['text'])
                elif span['flags'] == 6:
                    api_values.append(span['text'])
                elif span['flags'] == 4:
                    if newApi:
                        api_concatenated = " ".join(api_values)
                        newApi = False
                    additional_info.append(span['text'])
                    api_values = []
        if additional_info:
            entry = [api_concatenated] + additional_info
            data.append(entry)
    columns = ['API', 'Company', 'Description', 'Date', 'Code']
    df = pd.DataFrame(data, columns=columns)
    # se agrega columna de file number para estabilizar el identificador
    df['file_number'] = df['API'].apply(lambda x: x.split(' ')[0])
    return df


# @task(log_prints=True)
def read_pdf(pdf_path):
    columns = ['API', 'Company', 'Description', 'Date', 'Code', 'file_number']
    data_1 = []
    df_acumulado = pd.DataFrame(data_1, columns=columns)
    document = fitz.open(pdf_path)
    for page_num in range(document.page_count):
        page = document.load_page(page_num)
        page.set_cropbox(crop_area)
        blocks = page.get_text("dict")["blocks"]
        df = process_blocks(blocks=blocks)
        df_acumulado = pd.concat([df_acumulado, df], ignore_index=True)
    return df_acumulado


@task(log_prints=True)
def scrape_data(context):

    os.environ["DISPLAY"] = ":99" 
    logger = context['logger']
    logger.write('INFO','Se procede a correr el scrape y actualizar la tabla Finland')
    
    # # Use PyVirtualDisplay to simulate display for headless=False
    # display = Display(visible=0, size=(1920, 1080))  # Simulate display size 1920x1080
    # display.start()

    filename = Path('Finland.pdf')
    if os.path.exists(filename):
        os.remove(filename)

    logger.write("INFO",'comienza SeleniumBase')
    
    max_retries = 3
    for attempt in range(1, max_retries + 1):
        # try:
            with SB(uc=True, incognito=True) as sb:
                url= "https://fimea.fi/en/marketing_authorisations/marketing_authorisation_application/pending_marketing_authorisation_applications"
                logger.write("INFO",'SeleniumBase reconnect')
                sb.driver.uc_open_with_reconnect(url,6)
                sb.sleep(6)
                sb.driver.uc_gui_click_captcha()
                logger.write("INFO",'SeleniumBase click captcha')
                
                sb.sleep(15)
                elemento = sb.find_element(By.XPATH, '//div[contains(@class, "attachments")]//a[contains(@class, "attachment") and contains(@class, "pdf")]').get_attribute('href')
                # elemento = sb.find_element(By.CSS_SELECTOR, 'a.attachment.pdf').get_attribute('href')
                # elemento = sb.find_element(By.CSS_SELECTOR,'#portlet_com_liferay_journal_content_web_portlet_JournalContentPortlet_INSTANCE_PNeIdxAUP6kM > div > div.portlet-content-container > div > div.clearfix.journal-content-article > div > div > a').get_attribute('href')
                Name_Pdf= sb.find_element(By.XPATH,'//*[@id="portlet_com_liferay_journal_content_web_portlet_JournalContentPortlet_INSTANCE_PNeIdxAUP6kM"]/div/div[2]/div/div/div/div/div/a')
                Name_Pdf = Name_Pdf.text 

                # time.sleep(30)
                sb.get(elemento)
                sb.execute_script("window.open('','_blank').document.write('<script>setTimeout(function(){window.close()}, 1000)</script>');")
                time.sleep(2)
                all_cookies = sb.get_cookies()
                # Inicializar una lista para almacenar las partes de la cadena `cookies`
                cookies_parts = []

                # Construir la cadena `cookies` usando un bucle `for`
                for i in all_cookies:
                    cookies_parts.append(i['name'] + "=" + i['value'])

                # Unir las partes de la cadena `cookies` usando el separador ";"
                cookies = ";".join(cookies_parts)
            break
        
        # except Exception as e:
        #     wait_time = 2 ** attempt
        #     logger.write("ERROR",f'Error en el intento {attempt}: {str(e)}. Reintentando en {wait_time} segundos...')
        #     time.sleep(wait_time)
        #     if attempt == max_retries:
        #         logger.write("ERROR",'Máximo número de reintentos alcanzado. Fallo en el seleniumbase.')
        #         raise ValueError('Máximo número de reintentos alcanzado. Fallo en el seleniumbase.')
            
    url = elemento

    payload = {}
    headers = {
    'authority': 'fimea.fi',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36\''
    }

    boundary = ''
    headers['cookie'] = cookies
    headers ['Content-Type'] = 'multipart/form-data; boundary={}'.format(boundary)

    logger.write("INFO",'inicia la request')

    for attempt in range(1, max_retries + 1):
        try:
            response_text = request_sync('GET',url=url, headers=headers, proxy=True)
            logger.write('INFO',f"Se intenta obtener el pdf")
            break
        except Exception as e:
            wait_time = 2 ** attempt
            logger.write("ERROR",f'Error en el intento {attempt}: {str(e)}. Reintentando en {wait_time} segundos...')
            time.sleep(wait_time)
            if attempt == max_retries:
                logger.write("ERROR",'Máximo número de reintentos alcanzado. Fallo en la descarga del PDF.')
                raise ValueError('Máximo número de reintentos alcanzado. Fallo en la descarga del PDF.')

    filename.write_bytes(response_text.content)
    filename = 'Finland.pdf'
    Name_Pdf = Name_Pdf.strip()

    df = read_pdf(filename)

    if not df.empty:
        df = df.applymap(lambda x: x.lower() if isinstance(x, str) else x)
    else:
        message = 'El DataFrame está vacío'
        logger.write('ERROR', message)
        send_email(subject='WARNING Finland Files', message=message)
        raise ValueError(message)

    logger.write("INFO",'Finaliza ejecucion del scraper')

    context['df'] = df 
    # Stop the virtual display at the end
    # display.stop()
    return context


@flow(log_prints=True)
async def finland1(paisLote):

    context = {
        'paisLote': paisLote, 
        'solicitudes': None, 
        'logger': None, 
        'query_insert': query_insert, 
        'table_name': table_name, 
        'flow_status': None, 
        'df': None,
        'drop_duplicates_columns': []
        }

    pipeline = Pipeline(
        ('logger', logger_step),
        ('scrape_data', scrape_data),
        ('delete_table', delete_table),
        ('insert_into_table_by_df', insert_into_table_by_df)
    )

    context = await pipeline(context)

if __name__ == "__main__":
    asyncio.run(finland1(sl.PaisLote(lote=640, pais='Finland', usuarioRpa='7', tipoBusqueda='api')))
