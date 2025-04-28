# imports
from prefect import flow, task
import psycopg2 as ps
import pandas as pd
import logging
import ssl
import aiohttp
import asyncio
from datetime import datetime
from playwright.async_api import async_playwright
import json
from psycopg2 import extras
# import datetime
from dateutil.relativedelta import relativedelta
# from pyvirtualdisplay import Display
import os

# import utils
from utils.CursorProcedures import DbConnection
from utils.logs import logger_step
from utils.requests import request_async
from utils.pipelines import Pipeline
import utils.solicitudes as sl
import utils.scrapers as scr
from utils.db_actions import delete_table, insert_into_table_by_df
from utils.scrapers import unify_dfs
from utils.SendEmail import send_email

# flows
from subflows.set_estado import set_prefect_state_scraper

# params
url_general = 'https://www.cde.org.cn/main/xxgk/listpage/9f9c74c73e0f8f56a8bfbc646055026d'
url = "https://www.cde.org.cn/main/xxgk/getMenuListHc"
batch_size = 5
limit_year = 2023
search_types = [0, 1]

months_back = 5
DATES_DELETE = datetime.now() - relativedelta(months=months_back)
CUTOFF_DATE = DATES_DELETE - relativedelta(months=1)

QUERY_INSERT = """INSERT INTO china (serial_number, file_number, drug_name, app_type, reg_category, 
                                   company, date, year, page, search_type) VALUES %s;"""

TABLE_NAME = 'china'

class Year:
    def __init__(self, year, search_type):
        self.year = year
        self.page = 1
        self.search_type = search_type
        self.pages = None
        self.total = None


class Page(Year):
    def __init__(self, year, page):
        self.year = year
        self.page = page
        self.df = None

def get_delete_query():
    return f""" DELETE FROM china where date >= '{str(DATES_DELETE.date())}' """

# esta función en una generadora que crea lotes o batches para ejecutar la función process_batch en partes
def divide_into_batches(lst):
    for i in range(0, len(lst), batch_size):
        yield lst[i:i + batch_size]


async def get_cookies(url):
    os.environ["DISPLAY"] = ":99"
    async with async_playwright() as p:
        # Launch a headless browser
        browser = await p.chromium.launch(
            headless=False,  # Run headless as in your Selenium setup
            args=[
                "--disable-blink-features=AutomationControlled",
                "--disable-extensions",
                "--no-sandbox",
                "--disable-infobars",
                "--disable-dev-shm-usage",
                "--disable-browser-side-navigation",
                "--disable-gpu"
            ]
        )

        # Create a new browser context and a new page
        context = await browser.new_context()
        page = await context.new_page()

        # Navigate to the URL
        await page.goto(url)
        # time.sleep(10)
        # Retrieve all cookies from the current session
        all_cookies = await context.cookies()

        # Format cookies into a string
        cookies = "; ".join([f"{cookie['name']}={cookie['value']}" for cookie in all_cookies])

        # Clean up
        await context.close()
        await browser.close()

        return cookies


async def fetch(item, stop_flag):
    
    if type(item) == Year:
        year = item.year
        search_type = item.search_type
    else:
        year = item.year.year
        search_type = item.year.search_type

    page = item.page

    print(str(year), str(page), str(search_type))

    cookies = await get_cookies(url_general)

    # Create an SSL context that does not verify the certificate
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE

    payload = f"statenow={search_type}&year={year}&drugtype=&applytype=&acceptid=&drugname=&company=&pageSize=50&pageNum=" + str(page)
    headers = {
        'Accept': 'application/json, text/javascript, */*; q=0.01',
        'Accept-Language': 'es-ES,es;q=0.9,en;q=0.8',
        'Connection': 'keep-alive',
        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
        'Cookie': cookies,
        'Origin': 'https://www.cde.org.cn',
        'Referer': 'https://www.cde.org.cn/main/xxgk/listpage/9f9c74c73e0f8f56a8bfbc646055026d',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-origin',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
        'X-Requested-With': 'XMLHttpRequest',
        'sec-ch-ua': '"Google Chrome";v="123", "Not:A-Brand";v="8", "Chromium";v="123"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"'
    }

    e = 1
    while True:
        try:
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=60)) as session:
                async with session.post(url, headers=headers, data=payload, ssl=ssl_context) as response:
                    if response.status != 200:
                        message = 'Request failed: ' + str(response.status)
                        raise ValueError(message)
                    response = await response.text()
                    break
        except Exception as error:
            e += 1
            if e >= 5:
                message = 'Request failed: ' + str(response.status) + '. ' + str(error)
                send_email(subject='WARNING China Filings', message=message, recipient_team='IT')
                raise ValueError(message)

    data = json.loads(response)
    pages = data["data"]["pages"]
    total = data["data"]["total"]

    df_append = pd.DataFrame(data["data"]["records"])
    df_append['page'] = str(page)
    df_append['year'] = str(year)
    df_append['type'] = str(search_type)

    if type(item) == Year:
        item.pages = pages
        item.total = total
    else:
        item.df = df_append

    df_append['createdate'] = pd.to_datetime(df_append['createdate'])
    if (df_append['createdate'] < str(CUTOFF_DATE.date())).any():
        stop_flag['stop'] = True  # Set the stop flag


# esta es la funcion que ejecuta la funcion coroutine fetch de forma asincrónica
async def process_batch(batch, stop_flag):
    tasks = [fetch(item, stop_flag) for item in batch]
    await asyncio.gather(*tasks)
    if stop_flag['stop']:
        return "stop"


@task(log_prints=True)
async def scrape_data(context):

    logger = context['logger']
    logger.write('INFO', "Comienza China")
    
    logger.write("INFO",'Fecha límite: ' + str(CUTOFF_DATE))
    
    # # Start the virtual display
    # display = Display(visible=0, size=(1024, 768))  # Headless Xvfb display
    # display.start()
    # print('Se inició el display')

    # creo la lista de años desde el año corriente hasta el límite
    current_year = datetime.now().year

    # lista de años
    years_list = list(range(current_year, limit_year - 1, -1))

    # creo las instancias de años y los apendeo a una lista
    years_instances_list = []
    for search_type in search_types:
        for year in years_list:
            year_instance = Year(year, search_type)
            years_instances_list.append(year_instance)

    # batches de años
    year_batches = divide_into_batches(years_instances_list)
    stop_flag = {'stop': False}

    # hago una request de los años para saber la cantidad de páginas a consultar
    for batch in year_batches:
        await process_batch(batch, stop_flag)

    # creo las listas de isntancias para cada tipo de búsqueda
    pages_instances_search_type_0 = []
    pages_instances_search_type_1 = []
    for year in years_instances_list:
        for page in range(1, year.pages + 1):
            page_instance = Page(year, page)
            if page_instance.year.search_type == 0:
                pages_instances_search_type_0.append(page_instance)
            else:
                pages_instances_search_type_1.append(page_instance)

    pages_batches_0 = divide_into_batches(pages_instances_search_type_0)
    stop_flag = {'stop': False}

    # se procesaN las instancias de cada tipo de búsqueda por separado y se frena cuando se encuentra una fecha más antigua a X meses

    # tipo búsqueda 0
    for batch in pages_batches_0:
        result = await process_batch(batch, stop_flag)
        if result == "stop":  # XXX: el stop este es algo tricky
            logger.write("INFO","Stopping processing as data older than 4 months was found.")
            break

    pages_batches_1 = divide_into_batches(pages_instances_search_type_1)
    stop_flag = {'stop': False}

    # tipo búsqueda 1
    for batch in pages_batches_1:
        result = await process_batch(batch, stop_flag)
        if result == "stop":
            logger.write("INFO","Stopping processing as data older than 4 months was found.")
            break

    # sumo los page.total de cada página para saber la cantidad de resultados y verificar
    total_results = 0
    for year in years_instances_list:
        total_results += year.total

    # concateno las listas de los 2 tipos de búsqueda
    concatenated_list = pages_instances_search_type_0 + pages_instances_search_type_1

    # concateno los df
    for i, page in enumerate(concatenated_list):
        if i == 0:
            df_complete = page.df
        else:
            df_complete = pd.concat([df_complete, page.df])

    # verifico si hay duplicados
    for search_type in search_types:
        for year_instance in years_instances_list:
            year = year_instance.year
            df_year = df_complete[(df_complete['year'] == str(year)) & (df_complete['type'] == str(search_type))]
            df_year = df_year['ROW_ID']
            count = df_year.duplicated(keep=False).sum()
            if count > 0:
                raise ValueError("Hay duplicados")
            else:
                logger.write("INFO",'No hay duplicados en año ' + str(year))

    df_complete['createdate'] = pd.to_datetime(df_complete['createdate'])
    df_complete = df_complete[df_complete['createdate'] >= str(DATES_DELETE.date())]

    # conexión a bd
    connection = DbConnection()

    df = df_complete.rename(columns={
        'companys': 'company',
        'registerkind': 'reg_category',
        'createdate': 'date',
        'acceptid': 'file_number',
        'drgnamecn': 'drug_name',
        'applytype': 'app_type',
        'ROW_ID': 'serial_number',
        'type': 'search_type'
    })

    # Selecting relevant columns in the correct order
    relevant_columns = ['serial_number', 'file_number', 'drug_name', 'app_type',
                        'reg_category', 'company', 'date', 'year', 'page', 'search_type']
    df = df[relevant_columns]

    context['df'] = df
    
    connection.close_db()

    # display.stop()
    return context

@flow(log_prints=True)
async def china1(paisLote):

    context = {
        'paisLote': paisLote,
        'solicitudes': None,
        'logger': None,
        'query_insert': QUERY_INSERT,
        'query_delete': get_delete_query(),
        'table_name': TABLE_NAME,
        'flow_status': None,
        'df': None,
        'drop_duplicates_columns': ['company', 'date', 'drug_name', 'serial_number']
    }

    china_pipeline = Pipeline(
        ('logger', logger_step),
        ('async_process_scrape_data', scrape_data),
        ('delete_table', delete_table),
        ('insert_into_table_by_df', insert_into_table_by_df)
    )

    context = await china_pipeline(context)

    return context['flow_status']


if __name__ == '__main__':
    asyncio.run(china1(sl.PaisLote(lote=677, pais='China1', usuarioRpa=3, tipoBusqueda='api')))
