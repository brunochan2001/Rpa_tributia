from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import xml.etree.ElementTree as ET
from selenium.webdriver.common.by import By
from selenium import webdriver
from dotenv import load_dotenv
import pyarrow.parquet as pq
import pyarrow as pa
import pandas as pd
import shutil
import boto3
import gzip
import time
import os
import xmltodict
import json
from more_itertools import flatten
from flatten_dict import flatten

current_path = os.getcwd()
print("Current Path:", current_path)

load_dotenv()

USERNAME = os.getenv("USERNAME")
PASSWORD = os.getenv("PASSWORD")
CLIENT_ID = os.getenv("CLIENT_ID")
LANDING_PAGE = os.getenv("LANDING_PAGE")
DOWNLOAD_PAGE = os.getenv("DOWNLOAD_PAGE")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION")

# Definir la carpeta donde se guardará el archivo descargado
download_dir = os.path.expanduser(current_path) 

# Configurar cliente S3
s3 = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION,
)
bucket_name = "dev-sg-datalake"

# Configurar opciones para Chrome
options = webdriver.ChromeOptions()
options.binary_location = "/usr/bin/chromium"  # Adjust if needed
service = Service("/usr/bin/chromedriver")  # Use system-installed chromedriver
options.arguments.extend(["--no-sandbox", "--disable-setuid-sandbox"])
options.add_argument("--headless")  # Ejecutar sin interfaz gráfica
options.add_experimental_option("prefs", {
    "download.default_directory": download_dir,
    "download.prompt_for_download": False,
    "download.directory_upgrade": True,
    "safebrowsing.enabled": True,
    "plugins.always_open_pdf_externally": True
})

# Iniciar WebDriver
driver = webdriver.Chrome(service=service, options=options)

# Abrir la página de autenticación
driver.get(LANDING_PAGE)

# Ajustar tamaño de ventana
driver.set_window_size(550, 692)  

# Elementos a esperar en la página
elementos = {
    "rutcntr": "Página cargada correctamente, botón rutcntr.",
    "clave": "Página cargada correctamente, botón clave.",
    "bt_ingresar": "Página cargada correctamente, botón bt_ingresar."
}

# Esperar los elementos con un solo bucle
for elem_id, mensaje in elementos.items():
    try:
        WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.ID, elem_id)))
        print(mensaje)
    except:
        print(f"Error: La página tardó demasiado en cargar o el botón {elem_id} no se encontró.")

# Guardar la URL antes del login
old_url = driver.current_url

# Ingresar credenciales
driver.find_element(By.ID, "rutcntr").send_keys(USERNAME)
driver.find_element(By.ID, "clave").send_keys(PASSWORD)
driver.find_element(By.ID, "bt_ingresar").click()

# Esperar hasta que la URL cambie
WebDriverWait(driver, 20).until(EC.url_changes(old_url))
print("Página cambiada correctamente. Nueva URL:", driver.current_url)

# Archivos antes de la descarga
before_files = set(os.listdir(download_dir)) 

# Navegar a la página de descarga
DOWNLOAD_URL = f"{DOWNLOAD_PAGE}RUT_EMP={CLIENT_ID[:-1]}&DV_EMP={CLIENT_ID[-1]}&ORIGEN=RCP&RUT_RECP=&FOLIO=&FOLIOHASTA=&RZN_SOC=&FEC_DESDE=&FEC_HASTA=&TPO_DOC=&ESTADO=&ORDEN=&DOWNLOAD=XML"
driver.get(DOWNLOAD_URL)

# Esperar la descarga con límite de tiempo
download_file = None
timeout = 30  # Tiempo máximo de espera en segundos
start_time = time.time()

while time.time() - start_time < timeout:
    time.sleep(1)  # Espera 1 segundo para reducir el uso de CPU
    after_files = set(os.listdir(download_dir))  

    # Identificar el nuevo archivo descargado
    new_files = after_files - before_files  
    valid_files = [f for f in new_files if not f.endswith((".crdownload", ".part", ".tmp"))]

    if valid_files:  # Si hay un archivo válido nuevo, la descarga terminó
        download_file = valid_files[0] 
        break

# Si no se descargó ningún archivo, lanzar error
if not download_file:
    raise Exception("Error: No se detectó el archivo descargado dentro del tiempo de espera.")

# Cerrar el navegador
driver.quit()

# Ruta del archivo descargado
file_path = os.path.join(download_dir, download_file)

# Comprimir el archivo en GZIP
compressed_path = file_path + ".gz"
with open(file_path, "rb") as f_in, gzip.open(compressed_path, "wb") as f_out:
    shutil.copyfileobj(f_in, f_out)

# Subir a S3 el archivo comprimido
s3.upload_file(compressed_path, bucket_name, f"gzip/recibos/{download_file}.gz")
print(f"Gzip file saved successfully!")

# Define schema using PyArrow
schema = pa.schema([
    ('DocumentoId', pa.string()),
    ('TipoDTE', pa.int64()),
    ('Folio', pa.int32()),
    ('FechaEmision', pa.date32()),
    ('FechaVencimiento', pa.date32()),
    ('RazonSocial', pa.string()),
    ('RutEmisor', pa.string()),
    ('MontoNeto', pa.int32()),
    ('MontoExe', pa.int32()),
    ('TasaIVA', pa.int32()),
    ('IVA', pa.int32()),
    ('MontoTotal', pa.int32()),
])

# Leer el contenido del archivo
xml_content = open(download_file, "r").read()

# Convertir XML a diccionario
data_dict = xmltodict.parse(xml_content)["SetDTE"]["DTE"]

# Extraer y normalizar los datos
data = {
    "DocumentoId": [],
    "TipoDTE": [],
    "Folio": [],
    "FechaEmision": [],
    "FechaVencimiento": [],
    "RazonSocial": [],
    "RutEmisor": [],
    "MontoNeto": [],
    "MontoExe": [],
    "TasaIVA": [],
    "IVA": [],
    "MontoTotal": []
}

# Extraer y normalizar los datos
for child in data_dict:
    documento = child.get("Documento", {})
    detalles = documento.pop("Detalle", [])

    if not isinstance(detalles, list):
        detalles = [detalles]  # Asegura que sea una lista para iterar

    for detalle in detalles or [{}]:  # Si no hay detalles, agregar el documento solo
        data["DocumentoId"].append(documento.get("@ID"))
        data["TipoDTE"].append(documento.get("Encabezado", {}).get("IdDoc", {}).get("TipoDTE"))
        data["Folio"].append(documento.get("Encabezado", {}).get("IdDoc", {}).get("Folio"))
        data["FechaEmision"].append(documento.get("Encabezado", {}).get("IdDoc", {}).get("FchEmis"))
        data["FechaVencimiento"].append(documento.get("Encabezado", {}).get("IdDoc", {}).get("FchVenc"))
        data["RazonSocial"].append(documento.get("Encabezado", {}).get("Emisor", {}).get("RznSoc"))
        data["RutEmisor"].append(documento.get("Encabezado", {}).get("Emisor", {}).get("RUTEmisor"))
        data["MontoExe"].append(documento.get("Encabezado", {}).get("Totales", {}).get("MntExe"))
        data["MontoNeto"].append(documento.get("Encabezado", {}).get("Totales", {}).get("MntNeto"))
        data["TasaIVA"].append(documento.get("Encabezado", {}).get("Totales", {}).get("TasaIVA"))
        data["IVA"].append(documento.get("Encabezado", {}).get("Totales", {}).get("IVA"))
        data["MontoTotal"].append(documento.get("Encabezado", {}).get("Totales", {}).get("MntTotal"))

# Convertir a DataFrame
df = pd.DataFrame(data)

# Convertir columnas numéricas y manejar valores NaN de forma eficiente
cols_int = ["TipoDTE", "Folio", "MontoNeto", "MontoExe", "TasaIVA", "IVA", "MontoTotal"]
df[cols_int] = df[cols_int].apply(pd.to_numeric, errors="coerce").fillna(0).astype(int)

# Convertir fechas
df["FechaEmision"] = pd.to_datetime(df["FechaEmision"], format="%Y-%m-%d", errors="coerce")
df["FechaVencimiento"] = pd.to_datetime(df["FechaVencimiento"], format="%Y-%m-%d", errors="coerce")

table = pa.Table.from_pandas(df, schema=schema)

# Save to a Parquet file
parquet_path = file_path + ".parquet"
pq.write_table(table, parquet_path)

s3.upload_file(parquet_path, bucket_name, f"parquet/recibos/{download_file}.parquet")
print("Parquet file saved successfully!")
