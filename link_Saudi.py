import requests
import pandas as pd
from bs4 import BeautifulSoup as bs
import psycopg2 as ps
import json
import logging
import time
from prefect import flow, task
import asyncio

# utils
from utils.CursorProcedures import connection_db
from utils.logs import Logger
from utils.logs import logger_step
from utils.pipelines import Pipeline
import utils.solicitudes as sl
from utils.db_actions import delete_table, insert_into_table_by_df
from utils.SendEmail import send_email


#params
queryInsert = """INSERT INTO \"SaudiArabia\" ("TradeName","Application","DosageForm","Marketing","Active","Quantity","Unit") VALUES %s """
query_delete = """DELETE FROM \"SaudiArabia\""""
table_name = "SaudiArabia"


def request_url_verify(metodo,url,headers,payload,verify):
    verification = False
    re_intentos = 0 #CAMBIO DECÍA 1 (SOLO HACIA UN REINTENTO)
    while verification == False and re_intentos <= 3:
        try:
            response = requests.request(metodo, url, headers=headers, data=payload,verify=verify)
            if response.status_code != 200:
                raise ValueError(response.content)
            else:
                verification = True
                logging.info("Request: " + url)
                return (response)
        except:
            re_intentos += 1 
    if verification == False and re_intentos > 3:
        mensaje = 'Falló más de 3 veces un request: ' + url
        logging.info(mensaje)
        raise ValueError(mensaje)

@task(log_prints=True)
def scrape_data(context):
    lote = context['paisLote'].lote
    logger = context['logger']
    logger.write('INFO', 'Inicia Saudi Arabia')

    logger.write('INFO', 'Comienza inicialización de variables')

    Pagina = 1
    max_pags = 10000
    data_list = []

    while Pagina <= max_pags : 
        intentos = 1
        while intentos <= 4:
            try:    
                logger.write('INFO','inicia la request')    
                url = "https://www.sfda.gov.sa/GetUnderStudying.php?page="+str(Pagina)
                payload={}
                headers = {}
                response = request_url_verify("GET",url,headers,payload,verify = False)
                # Convertir los datos de byte a cadena
                data_str = response.content.decode('utf-8')
                # Convertir la cadena en un objeto JSON
                data_json = json.loads(data_str)
                if Pagina == 1:
                    # Obtenemos la cantidad de paginas
                    max_pags = data_json['data']['pageCount']
                    cant_register = data_json['data']['rowCount']
                    logging.info(f'Se encontraron {cant_register} resgistros en {max_pags} páginas.')

                # Acceder a los datos
                for result in data_json['data']['results']:
                    trade_name = result['requests']['tradeName']
                    app_concerns = result['requests']['application_Concerns']
                    dosage_form = result['requests']['dosage_Form']
                    marketing_auth_company = result['requests']['marketing_Authorization_Company']
                    # Crear listas para cada variable
                    active_substances = []
                    quantities = []
                    units = []

                    for product in result['rProducts']:
                        active_substance = product['activeSubstance']
                        quantity = product['quantity']
                        unit = product['unit']

                        # Agregar los datos a las listas
                        active_substances.append(active_substance)
                        quantities.append(quantity)
                        units.append(unit)

                    resAPI = '//'.join(item.strip() for item in active_substances)
                    resQ = '//'.join(item.strip() for item in quantities)
                    resU = '//'.join(item.strip() for item in units)

                    # Agregar los datos a la lista como un diccionario
                    data_list.append({
                        'Trade Name': trade_name.strip(), 
                        'Application Concerns': app_concerns.strip(), 
                        'Dosage Form': dosage_form.strip(), 
                        'Marketing Authorization Company': marketing_auth_company.strip(), 
                        'Active Substance': resAPI, 
                        'Quantity': resQ, 
                        'Unit': resU
                    })
                print('pagina: ' + str(Pagina))
                Pagina +=1
                break
            except:
                time.sleep(60*intentos)
                intentos += 1
                if intentos == 4:
                    raise ValueError('Falló el request')
                
    # convierte la lista en un dataframe de pandas
    df = pd.DataFrame(data_list)
    len_value = len(df)
    

    if len_value != 0 and len_value == cant_register : #verifico que obtuvimos la misma cantidad de resulados que informa la página
        logger.write('INFO','Se validó la integridad de la data obtenida')

    else:
        if len_value == 0:
            message = 'Esta corrida no trajo resultados.'
        elif len_value != cant_register :
            message = 'No se obtuvieron todas los resultados informados.'
        
        send_email(subject='WARNING Saudi Arabia', message=message, recipient_team='IT')
        raise ValueError(message)

    logger.write('INFO','Finaliza ejecucion script')

    df = df.applymap(lambda x: x.lower() if type(x) == str else x)
    context['df'] = df
    return(context)

@flow(log_prints=True)
async def saudi_arabia1(paisLote):
    context = {
        'paisLote': paisLote,
        'logger': None,
        'query_insert': queryInsert,
        'query_delete': query_delete,
        'table_name': table_name,
        'flow_status': None,
        'df': None,
        'drop_duplicates_columns': []
    }

    pipeline = Pipeline(
        ('logger', logger_step),
        ('scraper', scrape_data),
        ('delete_table', delete_table),
        ('insert_into_table_by_df', insert_into_table_by_df)        
    )

    context = await pipeline(context)


if __name__ == "__main__":
    asyncio.run(saudi_arabia1(sl.PaisLote(lote=683, pais='SaudiArabia', usuarioRpa='Introducir-usuariorpa', tipoBusqueda='api')))
