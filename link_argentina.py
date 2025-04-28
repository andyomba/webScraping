import requests
from bs4 import BeautifulSoup as bs
from pathlib import Path
from PyPDF2 import PdfReader
import psycopg2 as ps
import time
from calendar import month_name
import os
from PIL import Image
import pytesseract
from pdf2image import convert_from_path
from PyPDF2 import PdfReader
from prefect import task, flow
import asyncio
from datetime import datetime
from selectolax.parser import HTMLParser
import pandas as pd
import pdfplumber
import PyPDF2

# utils
from utils.logs import Logger 
from utils.CursorProcedures import connection_db, validate_connect_db
from utils.requests import request_sync
import utils.solicitudes as sl
from utils.logs import logger_step
from utils.pipelines import Pipeline

# parameters
meses_español = ['enero', 'febrero', 'marzo', 'abril', 'mayo', 'junio', 'julio', 'agosto', 'septiembre', 'octubre', 'noviembre', 'diciembre']
# general_url = 'https://boletin.anmat.gob.ar/'
año_limite = datetime.now().year-1
filename  = Path('argentina_.pdf')
queryInsert = """ INSERT INTO \"Argentina\" ("Id","Documento","texto","url", "Fecha", "Estado","iniciador") VALUES ((SELECT MAX("Id")+1 FROM "Argentina"),%s,%s,%s,%s,%s,%s)"""
queryInsertError = """call public."SP_IngresarErrorDocumento" (%s,%s,%s,%s,%s,%s) """
table_name = "Argentina"
URL = 'https://buscadispo.anmat.gob.ar/'

PAYLOAD = {
    "__EVENTTARGET": "ctl00$MainContent$gvDiposicion",
    "__EVENTARGUMENT": "",
    "__LASTFOCUS": "",
    "__VIEWSTATE": "",
    "__VIEWSTATEGENERATOR": "CA0B0334",
    "__VIEWSTATEENCRYPTED": "",
    "__EVENTVALIDATION": "",
    "ctl00$MainContent$txtNumDispo": "",
    "ctl00$MainContent$txtAnioDispo": "",
    "ctl00$MainContent$ddlMes": "0",
    "ctl00$MainContent$txtIniciador": "",
}

HEADERS = {
  'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
  'accept-language': 'es-AR,es;q=0.9,en-US;q=0.8,en;q=0.7',
  'cache-control': 'max-age=0',
  'content-type': 'application/x-www-form-urlencoded',
  'cookie': '_ga=GA1.3.315676383.1736177662; _ga_5FHX2M9HSB=GS1.3.1736183936.2.1.1736183956.40.0.0',
  'origin': 'https://buscadispo.anmat.gob.ar',
  'priority': 'u=0, i',
  'referer': 'https://buscadispo.anmat.gob.ar/',
  'sec-ch-ua': '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
  'sec-ch-ua-mobile': '?0',
  'sec-ch-ua-platform': '"Windows"',
  'sec-fetch-dest': 'document',
  'sec-fetch-mode': 'navigate',
  'sec-fetch-site': 'same-origin',
  'sec-fetch-user': '?1',
  'upgrade-insecure-requests': '1',
  'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36'
}
# variables env
POPPLER_PATH = os.getenv('poppler_path')
TESSERACT_PATH = os.getenv('tesseract_path')
YEAR_LIMIT = 2023


@task(log_prints=True)
def insertar_error(idsolicitud, e, url_pdf, db_connection):
    db_connection = validate_connect_db(db_connection=db_connection)
    # ingreso a tabla errores documentos
    params = (idsolicitud,None,'No se pudo cargar el documento. ' + str(e),url_pdf,None,'')
    db_connection.cursor.execute(queryInsertError, params)
    db_connection.connection.commit()

@task(log_prints=True)
def insert_pdf(file_name, text, url_pdf, fecha_insert,iniciador, queryInsert, db_connection):
    db_connection = validate_connect_db(db_connection=db_connection)
    params = (file_name, text.replace("\x00", "").lower(), url_pdf, fecha_insert, 'ok',iniciador)
    db_connection.cursor.execute(queryInsert, params)
    db_connection.connection.commit()


def extracts_keys(html_text):
    """
    Extrae los valores de __VIEWSTATE y __EVENTVALIDATION del HTML.
    """
    html = HTMLParser(html_text)
    try:
        viewstate = html.css_first("input#__VIEWSTATE").attributes["value"]
        validation = html.css_first("input#__EVENTVALIDATION").attributes["value"]
    except AttributeError:
        raise ValueError("No se encontraron los elementos __VIEWSTATE o __EVENTVALIDATION en la respuesta.")
    
    return viewstate, validation

def get_payload():
    """
    Realiza una solicitud GET para obtener los valores iniciales de __VIEWSTATE y __EVENTVALIDATION.
    """
    response = requests.get(URL, headers=HEADERS)
    if response.status_code != 200:
        raise ValueError(f"GET request failed with status code {response.status_code}")
    
    return extracts_keys(response.text), response

@flow(name='scrape_Argentina1',log_prints=True)
def scrape_data(context):

    lote = context['paisLote'].lote
    logger = context['logger']
    logger.write('INFO', 'Inicia Argentina')
    out = ''

    # Obtener los valores iniciales de __VIEWSTATE y __EVENTVALIDATION
    (viewstate, validation), response = get_payload()
    payload = PAYLOAD
    page_number = 2 

    # Diccionario para convertir nombres de meses en números
    mes_numero = {
        "Enero": "01", "Febrero": "02", "Marzo": "03", "Abril": "04",
        "Mayo": "05", "Junio": "06", "Julio": "07", "Agosto": "08",
        "Septiembre": "09", "Octubre": "10", "Noviembre": "11", "Diciembre": "12"
    }

    # listas para almacenar los datos
    disposiciones = []
    meses = []
    años = []
    iniciadores = []
    enlaces_pdf = []

    # while page_number <= 643:  # Límite de páginas(MODIFICAR)
    while True:

        soup = bs(response.text, "html.parser")

        # Buscar todas las filas de la tabla (<tr>)
        rows = soup.find_all("tr")

        # Iterar sobre las filas
        for row in rows[1:-2]:  # Suponemos que la primera fila es el encabezado
            cols = row.find_all("td")  # Encontrar todas las columnas dentro de la fila
            if len(cols) >= 5:  # Asegurarnos de que haya suficientes columnas
                disposiciones.append(cols[0].text.strip())  # Número de disposición
                meses.append(cols[1].text.strip())          # Mes
                años.append(cols[2].text.strip())           # Año
                iniciadores.append(cols[3].text.strip())    # Iniciador
                enlace = cols[4].find("a")                  # Buscar el enlace dentro de la columna
                enlaces_pdf.append(enlace["href"].strip() if enlace else "")  # Guardar el href

        # Verificar si el año es menor al límite y salir del loop
        if any((int(anio) if len(str(anio)) == 4 else 2000 + int(anio)) <= YEAR_LIMIT for anio in años):
            logger.write("INFO", f"Año menor igual que {YEAR_LIMIT} encontrado, terminando la iteracion.")
            break  # Salir del loop si encontramos un año menor que el límite

        # Actualizar payload con valores dinámicos
        payload["__VIEWSTATE"] = viewstate
        payload["__EVENTVALIDATION"] = validation
        payload["__EVENTARGUMENT"] = f"Page${page_number}"

        # Realizar la solicitud POST
        response = requests.post(URL, headers=HEADERS, data=payload)
        if response.status_code != 200:
            raise ValueError(f"POST request failed on page {page_number} with status code {response.status_code}")

        # Extraer y procesar el contenido de la respuesta
        response_text = response.text
        (viewstate, validation) = extracts_keys(response_text)

        logger.write('INFO', f"Procesada página {page_number}")

        last_page_selector = soup.find("a", href="javascript:__doPostBack('ctl00$MainContent$gvDiposicion','Page$Last')")
        if not last_page_selector:
            logger.write("INFO",f"Última página alcanzada: {page_number}")
            break

        page_number += 1  # Incrementar número de página


    # Crear un DataFrame con los datos extraídos
    data = {
        "no_dispo": disposiciones,
        "mes": meses,
        "anio": años,
        "iniciador": iniciadores,
        "link_pdf": enlaces_pdf,
    }
    df = pd.DataFrame(data)


    # Defino Paths de los utils para los pdfs
    pytesseract.pytesseract.tesseract_cmd = TESSERACT_PATH

    # path
    path = os.getcwd()

    
    if len(df) == 0:
        message= "La consulta falló ya que no hay años en " + URL + ' para conssultar'
        logger.write('ERROR', message)
        raise ValueError(message)

    # conexión a bd
    db_connection = connection_db()

    # consulta a bd para traer lista de documentos
    queryDocumentos = """select \"Documento\" from \"Argentina\" """
    db_connection.cursor.execute(queryDocumentos)
    documentos = db_connection.cursor.fetchall()
    documentos = [i[0] for i in documentos]
    # Filtrar valores None de la lista 'documentos'
    documentos = [doc for doc in documentos if doc is not None]
    documentos_normalizados = [doc.replace("_", "").lower() for doc in documentos if doc is not None]


    for index, row in df.iterrows():

        no_dispo = row["no_dispo"]
        mes = row["mes"]
        anio = row["anio"]
        iniciador = row["iniciador"]
        link_pdf = row["link_pdf"]

        if not link_pdf or not isinstance(link_pdf, str) or not link_pdf.startswith(("http://", "https://")):
            logger.write("INFO",f"Fila {index} tiene un link inválido: {link_pdf}")
            continue  # Salta esta iteración si el enlace no es válido

        anio = str(int(anio) + 2000 if int(anio) < 100 else anio)  # Si tiene 2 dígitos, sumarle 2000

        # Convertir el mes en letras al número correspondiente
        mes_num = mes_numero.get(mes, "00")  # Devuelve "00" si el mes no está en el diccionario

        # Crear la nueva fecha en formato dd/mm/yyyy (usando "01" como día por defecto)
        fecha = f"01/{mes_num}/{anio}" 

        # Crear las nuevas variables
        dispo_anio = f"{no_dispo}-{anio[-2:]}"  # Combinar no_dispo
        documento = f"{dispo_anio} {iniciador}"  # Combinar dispo_anio y iniciador

        # Extraer el nombre del archivo desde link_pdf y normalizarlo
        file_name = os.path.basename(link_pdf)  # Obtiene "10944-2025-ROEMMERS_S.A.I.C.F.-ARTICULO_5º-M.pdf"
        file_name = file_name.replace(".pdf","").replace("_"," ").lower()

        #Determinar si el documento es de septiembre 2024 o antes
        anio_int = int(anio)
        mes_num_int = int(mes_num)

        # Verificación diferenciada para fechas en septiembre 2024 o antes
        if (anio_int == 2024 and mes_num_int <= 9):
            if any(dispo_anio in doc or dispo_anio.replace("-", " ") in doc for doc in documentos_normalizados):
                logger.write("INFO", f"Disposición '{dispo_anio}' ya encontrada en la BD (Septiembre 2024 o antes).")
                continue  # Pasar al siguiente elemento porque el documento ya existe
            else:
                logger.write("INFO", f"Disposición '{dispo_anio}' no encontrada en ningún documento de la BD (Septiembre 2024 o antes).")
                # continúa para cargar el documento

        # Verificación para fechas posteriores a septiembre 2024
        else:


            # if any(dispo_normalizado in doc for doc in documentos_normalizados):
            if file_name in documentos_normalizados:
                logger.write("INFO", f"Documento '{file_name}' ya encontrado en la BD (Posterior a Septiembre 2024).")
                continue  # Pasar al siguiente elemento porque el documento ya existe
            else:
                logger.write("INFO", f"Documento '{file_name}' no encontrado en la BD (Posterior a Septiembre 2024).")
                # continúa para cargar el documento
        
        logger.write('INFO', f'Se procesará {file_name}, url {str(link_pdf)}')          

        try:                       

            pdf = requests.get(link_pdf)

            if pdf.status_code != 200:
                if pdf.status_code == 404:
                    insertar_error(lote, 'Error 404', link_pdf, db_connection)
                    continue
                else:
                    message = 'Falló el request del pdf con n error diferente a 404'
                    logger.write('ERROR', message)
                    raise ValueError(message)

            filename.write_bytes(pdf.content)

            # borro pngs
            for root, dirs, files in os.walk(path):
                for file in files:
                # condition to check the file name
                    if 'argentina_.png' in file:
                        # delete the file
                        os.remove(os.path.join(root, file))

            # pausa para descargar pdf
            time.sleep(1)

            try:
                reader = PdfReader(filename)
            except PyPDF2.errors.EmptyFileError:
                logger.write("INFO",f"El archivo {filename} está vacío. Se omite el procesamiento.")
                continue 
            min_pages = min(len(reader.pages),3)
            text = ""

            # se guardan los pdfs como imágenes
            images = convert_from_path(pdf_path=filename, poppler_path=POPPLER_PATH, first_page=0, last_page=min_pages)
            for count, img in enumerate(images):
                img_name = f"{count} argentina_.png"  
                img.save(img_name, "png")

            # se extrae el texto
            png_files = [f for f in os.listdir(".") if f.endswith("argentina_.png")]
            for png_file in png_files:
                text += pytesseract.image_to_string(Image.open(png_file))

            # Si Tesseract no extrajo texto, intentar con pdfplumber
            if not text.strip():
                with pdfplumber.open(filename) as pdf_doc:
                    text = "\n".join([page.extract_text() or "" for page in pdf_doc.pages[:min_pages]])
                logger.write('WARNING', 'Tesseract falló, se usó pdfplumber')

            if text == "" or text== " ":
                # ingreso a tabla errores documentos
                params = (lote, None, 'No se pudo cargar el documento', link_pdf, None, '')
                db_connection.cursor.execute(queryInsertError, params)
                db_connection.connection.commit()
                logger.write('ERROR', f'Falló la carg del pdf, el texto está vacío')    
                continue
            elif text != '' and text != ' ':
                # inserto en bd tabla Argentina files
                insert_pdf(file_name, text.lower(), link_pdf, fecha,iniciador.lower(), queryInsert, db_connection)
                logger.write('INFO', 'Se inserto en la bd: ' + link_pdf)
                logger.write('INFO', f'Se cargó el pdf correctamente')    
                
            documentos.append(documento)
            
        except Exception as e:

            logger.write('ERROR', 'No se pudo cargar el documento. ' + str(e)+' para el link : '+link_pdf)

            raise ValueError('No se pudo cargar el documento. ' + str(e))
            # continue

  
    # close connection
    db_connection.cursor.close()
    logger.write('INFO', 'Finaliza ejecucion script')
    return context

@flow(log_prints=True)
async def argentina1(paisLote):
    context = {
        'paisLote': paisLote,
        'logger': None,
        'query_insert': queryInsert,
        'table_name': table_name,
        'flow_status': None,
        'df': None,
        'drop_duplicates_columns': ['brand', 'date', 'api', 'molecula']
    }

    pipeline = Pipeline(
        ('logger', logger_step),
        ('scraper', scrape_data),
        # ('delete_table', delete_table),
        # ('insert_into_table_by_df', insert_into_table_by_df),
        # ('set_flow_status', set_prefect_state_scraper)
    )

    context = await pipeline(context)

if __name__ == "__main__":
    #argentina() #'192.168.0.149'
    asyncio.run(argentina1(sl.PaisLote(lote=673, pais='Argentina', usuarioRpa='8', tipoBusqueda='api')))