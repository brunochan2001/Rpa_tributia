from selenium import webdriver
from selenium.webdriver.common.by import By
import time
import os
from dotenv import load_dotenv
from selenium.webdriver.chrome.service import Service
import gzip
import boto3
import shutil

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
download_dir = os.path.expanduser(current_path)  # Cambia esto si quieres otra ruta

print("LANDING_PAGE:", LANDING_PAGE)

# Configurar cliente S3
s3 = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION,
)
bucket_name = "dev-sg-datalake"

# Crear la carpeta si no existe
os.makedirs(download_dir, exist_ok=True)
os.chmod(download_dir, 0o777) 
print("Directorio de descarga:", download_dir)

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
driver.set_window_size(550, 692)

# Ingresar credenciales
driver.find_element(By.ID, "rutcntr").send_keys(USERNAME)
driver.find_element(By.ID, "clave").send_keys(PASSWORD)
driver.find_element(By.ID, "bt_ingresar").click()
time.sleep(40)

before_files = set(os.listdir(download_dir))  # Archivos antes de la descarga

# Navegar a la página de descarga
DOWNLOAD_URL = f"{DOWNLOAD_PAGE}RUT_EMP={CLIENT_ID[:-1]}&DV_EMP={CLIENT_ID[-1]}&ORIGEN=RCP&RUT_RECP=&FOLIO=&FOLIOHASTA=&RZN_SOC=&FEC_DESDE=&FEC_HASTA=&TPO_DOC=&ESTADO=&ORDEN=&DOWNLOAD=XML"
driver.get(DOWNLOAD_URL)

time.sleep(30)

after_files = set(os.listdir(download_dir))  # Archivos después de la descarga
new_files = after_files - before_files  # Identificar el nuevo archivo
downloaded_file = new_files.pop()  # Obtener el archivo descargado
print("downloaded_file",downloaded_file)

# Cerrar el navegador
driver.quit()

# Ruta del archivo descargado
file_path = os.path.join(download_dir, downloaded_file)
compressed_path = file_path + ".gz"

# Comprimir el archivo en GZIP
with open(file_path, "rb") as f_in, gzip.open(compressed_path, "wb") as f_out:
    shutil.copyfileobj(f_in, f_out)

# Subir a S3 el archivo comprimido
s3.upload_file(compressed_path, bucket_name, f"TributIA_Test/{downloaded_file}.gz")

print(f"Archivo descargado en: {download_dir}")