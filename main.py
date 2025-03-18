from selenium import webdriver
from selenium.webdriver.common.by import By
import time
import os
from dotenv import load_dotenv
current_path = os.getcwd()
print("Current Path:", current_path)

load_dotenv()
USERNAME = os.getenv("USERNAME")
PASSWORD = os.getenv("PASSWORD")
CLIENT_ID = os.getenv("CLIENT_ID")
LANDING_PAGE = os.getenv("LANDING_PAGE")
DOWNLOAD_PAGE = os.getenv("DOWNLOAD_PAGE")

# Definir la carpeta donde se guardará el archivo descargado
download_dir = os.path.expanduser(current_path)  # Cambia esto si quieres otra ruta

# Crear la carpeta si no existe
os.makedirs(download_dir, exist_ok=True)

# Configurar opciones para Firefox
options = webdriver.FirefoxOptions()
options.set_preference("browser.download.folderList", 2)  # Usar una carpeta personalizada
options.set_preference("browser.download.dir", download_dir)  # Establecer la carpeta de descargas
options.set_preference("browser.helperApps.neverAsk.saveToDisk", "text/xml,application/xml")  # Evitar diálogos de descarga
options.set_preference("pdfjs.disabled", True)  # Deshabilitar visor de PDF en Firefox

# Iniciar WebDriver de Firefox
driver = webdriver.Firefox(options=options)

# Abrir la página de autenticación
driver.get(LANDING_PAGE)

driver.set_window_size(550, 692)

# Ingresar credenciales
driver.find_element(By.ID, "rutcntr").send_keys(USERNAME)
driver.find_element(By.ID, "clave").send_keys(PASSWORD)

driver.find_element(By.ID, "bt_ingresar").click()

# Navegar a la página de descarga
driver.get(f"{DOWNLOAD_PAGE}RUT_EMP={CLIENT_ID[:-1]}&DV_EMP={CLIENT_ID[-1]}&ORIGEN=RCP&RUT_RECP=&FOLIO=&FOLIOHASTA=&RZN_SOC=&FEC_DESDE=&FEC_HASTA=&TPO_DOC=&ESTADO=&ORDEN=&DOWNLOAD=XML")

# Cerrar el navegador
driver.quit()

print(f"Archivo descargado en: {download_dir}")
