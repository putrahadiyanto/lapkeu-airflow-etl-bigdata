from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
import time
import csv
import os
import zipfile
import json
from urllib.parse import quote
import datetime
import glob
import shutil
import tempfile
import xml.etree.ElementTree as ET
from collections import defaultdict
from pymongo import MongoClient  # Tambahkan import untuk MongoDB

# Set up the Chrome WebDriver
options = webdriver.ChromeOptions()

# Menetapkan lokasi untuk menyimpan file yang diunduh
download_directory = "C:\\DataTamish\\File Abi\\SEMESTER 4\\BigData\\idx-to-mongodb-main\\idx-to-mongodb-main\\download"
json_directory = "C:\\DataTamish\\File Abi\\SEMESTER 4\\BigData\\idx-to-mongodb-main\\idx-to-mongodb-main\\json_data"

# Memastikan direktori ada
os.makedirs(download_directory, exist_ok=True)
os.makedirs(json_directory, exist_ok=True)

# Mengatur preferensi untuk lokasi unduhan
prefs = {"download.default_directory": download_directory}
options.add_experimental_option("prefs", prefs)

# Ambil tahun saat ini
current_year = str(datetime.datetime.now().year)
current_month = datetime.datetime.now().month

# Menentukan triwulan menggunakan bulan
if current_month <= 3:
    quarter = 4
    current_year = str(int(current_year) - 1)  # Jika bulan <= 3, gunakan tahun sebelumnya
elif current_month <= 6:
    quarter = 1
elif current_month <= 9:
    quarter = 2
else:
    quarter = 3

# Konfigurasi MongoDB
mongo_uri = "mongodb://localhost:27017/"
db_name = "stock_data"
collection_name = f"Lapkeu{current_year}TW{quarter}"

# Fungsi untuk mengunggah data ke MongoDB
def upload_to_mongodb(data):
    try:
        # Buat koneksi ke MongoDB
        client = MongoClient(mongo_uri)
        db = client[db_name]
        collection = db[collection_name]
        
        # Cek apakah data dengan ticker yang sama sudah ada
        existing = collection.find_one({"ticker": data["ticker"]})
        if existing:
            # Update data jika sudah ada
            result = collection.replace_one({"ticker": data["ticker"]}, data)
            return f"Data diperbarui dengan ID: {data['ticker']}"
        else:
            # Insert data baru jika belum ada
            result = collection.insert_one(data)
            return f"Data ditambahkan dengan ID: {result.inserted_id}"
    except Exception as e:
        return f"Error saat mengunggah ke MongoDB: {str(e)}"

# Fungsi untuk membuat URL berdasarkan kode ticker
def generate_url(ticker):
    return f"https://www.idx.co.id/Portals/0/StaticData/ListedCompanies/Corporate_Actions/New_Info_JSX/Jenis_Informasi/01_Laporan_Keuangan/02_Soft_Copy_Laporan_Keuangan/Laporan%20Keuangan%20Tahun%20{current_year}/TW{quarter}/{ticker}/instance.zip"

# Fungsi untuk mendapatkan file terakhir yang diunduh
def get_latest_downloaded_file(download_dir):
    list_of_files = glob.glob(os.path.join(download_dir, '*'))
    if not list_of_files:
        return None
    latest_file = max(list_of_files, key=os.path.getctime)
    return latest_file

# Fungsi untuk mengekstrak file instance.xbrl dari ZIP
def extract_instance_xbrl(zip_path, extract_to):
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        for file_info in zip_ref.infolist():
            if file_info.filename.lower() == 'instance.xbrl':
                # Ekstrak instance.xbrl ke direktori sementara
                zip_ref.extract(file_info.filename, extract_to)
                return os.path.join(extract_to, file_info.filename)
    return None

# Fungsi untuk mengkonversi XBRL ke JSON
def xbrl_to_json(xbrl_path, ticker):
    try:
        # Parse file XBRL
        tree = ET.parse(xbrl_path)
        root = tree.getroot()
        
        # Temukan semua namespace yang digunakan dalam dokumen
        namespaces = {k: v for k, v in root.attrib.items() if k.startswith('xmlns:')}
        
        # Inisialisasi dict untuk menyimpan data
        data = {
            "file_info": {
                "instance_filename": "instance.xbrl",
                "taxonomy_filename": "Taxonomy.xsd",
                "file_size": os.path.getsize(xbrl_path)
            },
            "ticker": ticker,
            "tahun": current_year,
            "triwulan": f"TW{quarter}",
            "facts": {}
        }
        
        # Temukan semua elemen (facts) dalam dokumen
        for elem in root.findall(".//*"):
            # Jika elemen memiliki atribut contextRef, ini adalah fact
            context_ref = elem.attrib.get('contextRef')
            if context_ref:
                # Ambil nama dan nilai dari elemen
                tag = elem.tag
                if '}' in tag:  # Nama dengan namespace
                    tag = tag.split('}', 1)[1]  # Ambil bagian setelah namespace
                
                value = elem.text
                if value is None:
                    continue
                
                # Buat key untuk JSON
                key = f"{tag}_{context_ref}"
                
                # Simpan ke dict facts
                data["facts"][key] = {
                    "value": value,
                    "context": context_ref,
                    "name": tag
                }
        
        # Tambahkan timestamp pengambilan data
        data["processed_date"] = {"$date": datetime.datetime.now().isoformat()}
        
        return data
    except Exception as e:
        print(f"Error saat mengkonversi XBRL ke JSON: {str(e)}")
        return None

# Memulai WebDriver dengan Chrome
driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

# Baca file CSV yang berisi daftar ticker
csv_file_path = "C:\\DataTamish\\File Abi\\SEMESTER 4\\BigData\\idx-to-mongodb-main\\idx-to-mongodb-main\\emiten.csv"

# Mengunduh dan mengkonversi file untuk setiap ticker
try:
    with open(csv_file_path, 'r') as file:
        csv_reader = csv.reader(file)

        for row in csv_reader:
            ticker = row[0].strip()  # Ambil ticker dari kolom pertama dan hapus spasi
            
            print(f"Mengunduh data untuk ticker: {ticker} tahun {current_year} TW{quarter}")
            url = generate_url(ticker)
            
            try:
                # Catat file terakhir sebelum mengunduh
                files_before = set(glob.glob(os.path.join(download_directory, '*')))
                
                # Akses halaman pengunduhan
                driver.get(url)
                
                # Tunggu beberapa detik agar file dapat diunduh
                print(f"Sedang mengunduh file untuk {ticker}...")
                time.sleep(10)  # Waktu tunggu dapat disesuaikan
                
                # Catat file baru setelah mengunduh
                files_after = set(glob.glob(os.path.join(download_directory, '*')))
                new_files = files_after - files_before
                
                zip_file_path = None
                if new_files:
                    # Ambil file baru yang diunduh
                    zip_file_path = list(new_files)[0]
                else:
                    # Cara alternatif: coba dapatkan file terakhir diunduh
                    zip_file_path = get_latest_downloaded_file(download_directory)
                
                if zip_file_path and zipfile.is_zipfile(zip_file_path):
                    with tempfile.TemporaryDirectory() as temp_dir:
                        # Ekstrak instance.xbrl
                        print(f"Mengekstrak instance.xbrl dari {os.path.basename(zip_file_path)}...")
                        xbrl_path = extract_instance_xbrl(zip_file_path, temp_dir)
                        
                        if xbrl_path:
                            # Konversi XBRL ke JSON
                            print(f"Mengonversi instance.xbrl ke JSON untuk {ticker}...")
                            json_data = xbrl_to_json(xbrl_path, ticker)
                            
                            if json_data:
                                # Simpan JSON ke file
                                json_path = os.path.join(json_directory, f"{ticker}.json")
                                with open(json_path, 'w', encoding='utf-8') as json_file:
                                    json.dump(json_data, json_file, indent=2, ensure_ascii=False)
                                
                                print(f"Data {ticker} berhasil disimpan sebagai {json_path}")
                                
                                # Upload ke MongoDB
                                result = upload_to_mongodb(json_data)
                                print(f"MongoDB: {result}")
                                
                                # Hapus file ZIP yang telah diproses
                                os.remove(zip_file_path)
                            else:
                                print(f"Gagal mengkonversi file XBRL untuk {ticker}")
                        else:
                            print(f"Tidak dapat menemukan instance.xbrl dalam zip untuk {ticker}")
                else:
                    print(f"File ZIP tidak valid atau tidak dapat ditemukan untuk {ticker}")
                        
            except Exception as e:
                print(f"Gagal memproses data untuk {ticker}: {str(e)}")
                
            # Tunggu sejenak sebelum mengunduh file berikutnya
            time.sleep(2)
            
except FileNotFoundError:
    print(f"File CSV tidak ditemukan di: {csv_file_path}")
except Exception as e:
    print(f"Terjadi kesalahan: {str(e)}")
finally:
    # Menutup WebDriver setelah proses selesai
    driver.quit()

print("Proses pengunduhan, konversi, dan upload ke MongoDB selesai!")