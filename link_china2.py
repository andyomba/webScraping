
from seleniumbase import SB
import re
import time
import asyncio
import pandas as pd

from prefect import flow, task

from utils.pipelines import Pipeline
from utils.CursorProcedures import connection_db
import utils.solicitudes as sl
import utils.scrapers as scr
from utils.logs import logger_step
from utils.db_actions import delete_table, insert_into_table_by_df
from utils.scrapers import unify_dfs

from subflows.set_estado import set_prefect_state_scraper

from utils.fxs_countries_modulates import scraper_by_section, define_status

# Params
COUNTRY = 'China1_1_approvals'
QUERY_INSERT = 'INSERT INTO data_lake_approvals (file_number, brand_name_drug, on_the_market, holder, holder_country, ff_manufacturer, ff_manufacturer_country, dosage_form, compound_name, approval_date, expire_date, observations, link_registro, status, molecula, search_value, pais, lote) VALUES %s'
TABLE_NAME = 'data_lake_approvals'
QUERY_FETCH_IDENTIFICADOR = 'select on_the_market from data_lake_approvals where pais ilike \'china1_1_approvals\' '
QUERY_FETCH_LINKS = 'select link_registro from data_lake_approvals where pais ilike \'china1_1_approvals\' '

@task(log_prints=True)
async def scrape_data(solicitud, context):
    molecula = solicitud.molecula
    #traduccion = solicitud.traduccion
    traduccion = solicitud.molecula

    logger = context['logger']
    lote = context['paisLote'].lote

    data_final_domesticDrug = scraper_by_section(seccion='domestic_drug', 
                                                 traduccion=traduccion, 
                                                 molecula=molecula, 
                                                 logger=logger, 
                                                 lote=lote,
                                                 query_fetch_identificador=QUERY_FETCH_IDENTIFICADOR,
                                                 query_fetch_links=QUERY_FETCH_LINKS)

    #data_final = {**data_final_domesticDrug, **data_final_importedDrug, **data_final_importedDrugTradeName}

    # Inicializar `data_final` vacío
    data_final = {}

    # Solo incluye los diccionarios que no sean None
    if data_final_domesticDrug is not None:
        data_final.update(data_final_domesticDrug)


    
    try:
        df = pd.DataFrame(data_final)
        if len(df) == 0:
            logger.write('WARNING', f'Df Vacio para la molecula: {molecula}, traduccion: {traduccion}')
            return
    except Exception as e:
        logger.write('ERROR', f'No concuerda la cantidad de DF \n {e}')
        return
    

    df['status'] = df.apply(define_status, axis=1)
    df['molecula'] = molecula
    df['search_value'] = traduccion
    df['pais'] = COUNTRY
    df['lote'] = lote

    logger.write('INFO', f'Se finaliza el script de China1_1_approvals para la molecula: {molecula} y traduccion: {traduccion}, con {len(df)} resultados, para el Lote: {lote}')

    solicitud.cantidad_resultados= len(df)
    solicitud.df = df

    return context

@flow(log_prints=True)
async def china1_1_approvals(paisLote):
    context = {
        'paisLote': paisLote,
        'solicitudes': None,
        'logger': None,
        'pais': COUNTRY,
        'query_insert': QUERY_INSERT,
        'table_name': TABLE_NAME,
        'flow_status': None,
        'df': None,
        'batch_size': 1,
        'drop_duplicates_columns': []
    }
    # TODO pasar por parametro el tipo de corrida 

    pipeline = Pipeline(
        ('logger', logger_step),
        ('obtener_solicitudes', sl.search_solicitudes_scrapers),
        ('process_async_scrape_data', scr.process_function_by_solicitud, scrape_data),
        #('delete_table', delete_table),
        ('unify_dfs', unify_dfs),
        ('insert_into_table_by_df', insert_into_table_by_df),
        ('set_flow_status', set_prefect_state_scraper)
    )


    context = await pipeline(context)


if __name__ == "__main__":
       
    asyncio.run(china1_1_approvals(sl.PaisLote(lote=719, pais='China1_approvals', usuarioRpa='9', tipoBusqueda='api')))