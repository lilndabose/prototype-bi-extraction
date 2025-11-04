import os
import mysql.connector
from datetime import datetime
import math

from dotenv import load_dotenv

load_dotenv()

DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_PORT = os.getenv("DB_PORT", 3306)

DB_CONFIG = {
    "host": DB_HOST,
    "user": DB_USER,
    "password": DB_PASSWORD,
    "database": DB_NAME
}

def get_filepath(filename="ERIS_Report_Extraction_15_09_2025.xlsx"):
    backend_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    input_dir = os.path.join(backend_dir, "input")
    file_path = os.path.join(input_dir, filename)

    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Could not find Excel file at: {file_path}")

    return file_path

def get_filepath_to_execute():
    try:
      filename = get_latest_pending_file()
      backend_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
      input_dir = os.path.join(backend_dir, "uploads")
      file_path = os.path.join(input_dir, filename)

      if not os.path.exists(file_path):
        raise FileNotFoundError(f"fichier {filename} pas trouver dans: {file_path}")

      return file_path
    except:
      print('Aucun fichier deposer ...')

def calculate_station_scores(data):
    station_scores = {}

    for row in data:
        station_code = row.get("station code")
        d02 = row.get("d.02")
        ep11 = row.get("ep11")

        d02_str = str(d02).strip().lower() if d02 is not None and str(d02).lower() != 'nan' else None
        ep11_str = str(ep11).strip().lower() if ep11 is not None and str(ep11).lower() != 'nan' else None

        if d02_str == "yes":
            score = 0
        elif d02_str == "no":
            score = 100
        elif d02_str is None:
            if ep11_str == "yes":
                score = 100
            elif ep11_str == "no":
                score = 0
            else:
                score = '' 
        else:
            score = ''

        if station_code:
            station_scores[station_code] = score

    return station_scores

def get_station_code_by_name(data):
    station_name_codes = {}

    for row in data:
        station_name = row.get("station name")
        station_name_codes[station_name] = row.get("station code")

    return station_name_codes

def create_extractions_table_if_not_exists(table_columns,table_name='extractions'):
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()

    columns_sql = ", ".join([f"`{col}` VARCHAR(255)" for col in table_columns])
    create_sql = f"""
    CREATE TABLE IF NOT EXISTS `{table_name}` (
        {columns_sql},
        `date` DATE,
        `timestamp` DATETIME
    )
    """
    cursor.execute(create_sql)
    conn.commit()
    cursor.close()
    
def insert_extraction_data(data, table_name='extractions'):
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()
    file_creation_date = get_latest_pending_file_date()
    current_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    for row in data:
        row["date"] = file_creation_date
        row["timestamp"] = current_timestamp

        columns_to_check = [col for col in row.keys() if col != "timestamp"]

        where_conditions = []
        where_values = []
        for col in columns_to_check:
            if row[col] is None:
                where_conditions.append(f"`{col}` IS NULL")
            else:
                where_conditions.append(f"`{col}` = %s")
                where_values.append(row[col])

        where_clause = " AND ".join(where_conditions)

        check_sql = f"SELECT COUNT(*) FROM `{table_name}` WHERE {where_clause}"
        cursor.execute(check_sql, where_values)
        count = cursor.fetchone()[0]

        if count == 0:
            columns = ", ".join([f"`{col}`" for col in row.keys()])
            placeholders = ", ".join(["%s"] * len(row))
            values = list(row.values())
            insert_sql = f"INSERT INTO `{table_name}` ({columns}) VALUES ({placeholders})"
            cursor.execute(insert_sql, values)
        else:
            print(f"Skipped duplicate row for date {file_creation_date}")

    conn.commit()
    cursor.close()
    conn.close()
    
    
def create_extractions_questions_table_if_not_exists(table_columns,table_name='extraction_questions'):
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()

    columns_sql = ", ".join([f"`{col}` VARCHAR(255)" for col in table_columns])
    create_sql = f"""
    CREATE TABLE IF NOT EXISTS `{table_name}` (
        {columns_sql},
        `date` DATE,
        `timestamp` DATETIME
    )
    """
    cursor.execute(create_sql)
    conn.commit()
    cursor.close()

def insert_extraction_questions_data(data, table_name='extraction_questions'):
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()
    file_creation_date = get_latest_pending_file_date()
    current_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    for row in data:
        row["date"] = file_creation_date
        row["timestamp"] = current_timestamp

        columns_to_check = [col for col in row.keys() if col != "timestamp"]

        where_conditions = []
        where_values = []
        for col in columns_to_check:
            value = row[col]
            if value is None or (isinstance(value, float) and math.isnan(value)):
                where_conditions.append(f"`{col}` IS NULL")
            else:
                where_conditions.append(f"`{col}` = %s")
                where_values.append(value)

        where_clause = " AND ".join(where_conditions)

        # Check for duplicates
        check_sql = f"SELECT COUNT(*) FROM `{table_name}` WHERE {where_clause}"
        cursor.execute(check_sql, where_values)
        count = cursor.fetchone()[0]

        if count == 0:
            # Convert NaN to None for insert
            cleaned_values = [None if (v is None or (isinstance(v, float) and math.isnan(v))) else v for v in row.values()]
            columns = ", ".join([f"`{col}`" for col in row.keys()])
            placeholders = ", ".join(["%s"] * len(row))
            insert_sql = f"INSERT INTO `{table_name}` ({columns}) VALUES ({placeholders})"
            cursor.execute(insert_sql, cleaned_values)
        else:
            print(f"Skipped duplicate row for date {file_creation_date}")

    conn.commit()
    cursor.close()
    conn.close()
    
def create_hse_variant_table_if_not_exists(table_columns,table_name='hse_variants'):
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()

    columns_sql = ", ".join([f"`{col}` VARCHAR(255)" for col in table_columns])
    create_sql = f"""
    CREATE TABLE IF NOT EXISTS `{table_name}` (
        {columns_sql},
        `date` DATE,
        `timestamp` DATETIME
    )
    """
    cursor.execute(create_sql)
    conn.commit()
    cursor.close()
    
def insert_hse_variant_data(data, table_name='hse_variants'):
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()
    file_creation_date = get_latest_pending_file_date()
    current_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    for row in data:
        row["date"] = file_creation_date
        row["timestamp"] = current_timestamp

        columns_to_check = [col for col in row.keys() if col != "timestamp"]

        where_conditions = []
        where_values = []
        for col in columns_to_check:
            value = row[col]
            if value is None or (isinstance(value, float) and math.isnan(value)):
                where_conditions.append(f"`{col}` IS NULL")
            else:
                where_conditions.append(f"`{col}` = %s")
                where_values.append(value)

        where_clause = " AND ".join(where_conditions)

        check_sql = f"SELECT COUNT(*) FROM `{table_name}` WHERE {where_clause}"
        cursor.execute(check_sql, where_values)
        count = cursor.fetchone()[0]

        if count == 0:
            cleaned_values = [None if (v is None or (isinstance(v, float) and math.isnan(v))) else v for v in row.values()]
            columns = ", ".join([f"`{col}`" for col in row.keys()])
            placeholders = ", ".join(["%s"] * len(row))
            insert_sql = f"INSERT INTO `{table_name}` ({columns}) VALUES ({placeholders})"
            cursor.execute(insert_sql, cleaned_values)
        else:
            print(f"Skipped duplicate row for date {file_creation_date}")

    conn.commit()
    cursor.close()
    conn.close()
    
    
def get_latest_pending_file():
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor(dictionary=True)
    
    try:
        query = """
        SELECT filename, upload_date, file_size, filetype
        FROM file_uploads
        WHERE file_status = 'pending'
        ORDER BY upload_date DESC
        LIMIT 1
        """
        cursor.execute(query)
        result = cursor.fetchone()
        
        if result:
            return result['filename']
        else:
            return None
            
    except Exception as e:
        print(f"Error fetching latest pending file: {e}")
        return None
    finally:
        cursor.close()
        conn.close()
        
def get_latest_pending_file_date():
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor(dictionary=True)
    
    try:
        query = """
        SELECT filename, upload_date, date_created, file_size, filetype
        FROM file_uploads
        WHERE file_status = 'pending'
        ORDER BY upload_date DESC
        LIMIT 1
        """
        cursor.execute(query)
        result = cursor.fetchone()
        
        if result:
            return result['date_created']
        else:
            return None
            
    except Exception as e:
        print(f"Error fetching latest pending file creation date: {e}")
        return None
    finally:
        cursor.close()
        conn.close()
        

def update_file_status(filename, new_status):
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()
    
    try:
        query = """
        UPDATE file_uploads
        SET file_status = %s
        WHERE filename = %s
        """
        cursor.execute(query, (new_status, filename))
        conn.commit()
        
        if cursor.rowcount > 0:
            print(f"Mise a jour du status du fichier '{filename}' to '{new_status}'")
            return True
        else:
            print(f"Aucun fichier avec le nom '{filename}'")
            return False
            
    except Exception as e:
        print(f"Error de mise a jour sur le status du fichier d'extraction: {e}")
        conn.rollback()
        return False
    finally:
        cursor.close()
        conn.close()
        
def get_real_filename(path):
    arr = path.split('\\')
    if len(arr) > 0:
        return arr[len(arr)-1]
    else:
        return ''
    
def format_to_k(num: int) -> str:
    if num >= 1000:
        return str(round(num / 1000)) + 'k'
    return str(num)
