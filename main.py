from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
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

try:
    # Esperar hasta que el botón de inicio de sesión esté presente
    WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.ID, "rutcntr")))
    print("Página cargada correctamente, botón rutcntr.")
except:
    print("Error: La página tardó demasiado en cargar o el botón no se encontró.")

try:
    # Esperar hasta que el botón de inicio de sesión esté presente
    WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.ID, "clave")))
    print("Página cargada correctamente, botón clave.")
except:
    print("Error: La página tardó demasiado en cargar o el botón no se encontró.")

try:
    # Esperar hasta que el botón de inicio de sesión esté presente
    WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.ID, "bt_ingresar")))
    print("Página cargada correctamente, botón bt_ingresar.")
except:
    print("Error: La página tardó demasiado en cargar o el botón no se encontró.")

driver.set_window_size(550, 692)

# Ingresar credenciales
driver.find_element(By.ID, "rutcntr").send_keys(USERNAME)
driver.find_element(By.ID, "clave").send_keys(PASSWORD)
driver.find_element(By.ID, "bt_ingresar").click()

# Guardar la URL antes del login
old_url = driver.current_url

# Esperar hasta que la URL cambie
WebDriverWait(driver, 20).until(EC.url_changes(old_url))
print("Página cambiada correctamente. Nueva URL:", driver.current_url)

# Archivos antes de la descarga
before_files = set(os.listdir(download_dir)) 

print("Archivos antes de la descarga:", before_files)

# Navegar a la página de descarga
DOWNLOAD_URL = f"{DOWNLOAD_PAGE}RUT_EMP={CLIENT_ID[:-1]}&DV_EMP={CLIENT_ID[-1]}&ORIGEN=RCP&RUT_RECP=&FOLIO=&FOLIOHASTA=&RZN_SOC=&FEC_DESDE=&FEC_HASTA=&TPO_DOC=&ESTADO=&ORDEN=&DOWNLOAD=XML"
driver.get(DOWNLOAD_URL)

download_file = None

while download_file is None:
    time.sleep(1)  # Espera 1 segundo para reducir el uso de CPU
    after_files = set(os.listdir(download_dir))  
    print("Archivos después de la descarga:", after_files)

    # Identificar el nuevo archivo descargado
    new_files = after_files - before_files  
    valid_files = [f for f in new_files if not f.endswith((".crdownload", ".part", ".tmp"))]

    if valid_files:  # Si hay un archivo válido nuevo, la descarga terminó
        download_file = os.path.join(download_dir, valid_files[0])  

print("Archivo descargado:", download_file)

# Archivos después de la descarga
after_files = set(os.listdir(download_dir)) 
print("after_files", after_files)

# Identificar el nuevo archivo
new_files = after_files - before_files  

# Obtener el archivo descargado
downloaded_file = new_files.pop()  

# Cerrar el navegador
driver.quit()

# Ruta del archivo descargado
file_path = os.path.join(download_dir, downloaded_file)

# Comprimir el archivo en GZIP
compressed_path = file_path + ".gz"
with open(file_path, "rb") as f_in, gzip.open(compressed_path, "wb") as f_out:
    shutil.copyfileobj(f_in, f_out)

# Subir a S3 el archivo comprimido
s3.upload_file(compressed_path, bucket_name, f"gzip/recibos/{downloaded_file}.gz")
print(f"Gzip file saved successfully!")

# Define schema using PyArrow
schema = pa.schema([
    ('id', pa.int32()),
    ('name', pa.string()),
    ('age', pa.int32()),
    ('salary', pa.float64()),
    ('is_manager', pa.bool_())
])

# Create a DataFrame that matches the schema
data = {
    'id': [1, 2, 3],
    'name': ['Alice', 'Bob', 'Charlie'],
    'age': [25, 30, 35],
    'salary': [50000.0, 60000.5, 70000.2],
    'is_manager': [False, True, False]
}

df = pd.DataFrame(data)

# Convert DataFrame to Arrow Table with the defined schema
table = pa.Table.from_pandas(df, schema=schema)

# Save to a Parquet file
pq.write_table(table, file_path)

s3.upload_file(file_path, bucket_name, f"parquet/recibos/{downloaded_file}.parquet")
print("Parquet file saved successfully!")
