import os
from dotenv import load_dotenv
from sqlalchemy import text
import pandas as pd

load_dotenv()

DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_PORT = os.getenv("DB_PORT", 3306)

def get_db_uri():
    return f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

def get_db_config():
    DB_CONFIG = {
        "host": DB_HOST,
        "user": DB_USER,
        "password": DB_PASSWORD,
        "database": DB_NAME
    }
    
    return DB_CONFIG


def delete_data_by_date(db, date_to_delete):
    try:
        if hasattr(date_to_delete, 'strftime'):
            date_str = date_to_delete.strftime('%Y-%m-%d')
        else:
            date_str = str(date_to_delete)
        
        deletion_counts = {}
        
        hse_delete_query = text("""
            DELETE FROM hse_variants 
            WHERE DATE(`date`) = :date_to_delete
        """)
        hse_result = db.session.execute(hse_delete_query, {"date_to_delete": date_str})
        deletion_counts['hse_variants'] = hse_result.rowcount
        
        extractions_delete_query = text("""
            DELETE FROM extractions 
            WHERE DATE(`date`) = :date_to_delete
        """)
        extractions_result = db.session.execute(extractions_delete_query, {"date_to_delete": date_str})
        deletion_counts['extractions'] = extractions_result.rowcount
        
        questions_delete_query = text("""
            DELETE FROM extraction_questions 
            WHERE DATE(`date`) = :date_to_delete
        """)
        questions_result = db.session.execute(questions_delete_query, {"date_to_delete": date_str})
        deletion_counts['extraction_questions'] = questions_result.rowcount
        
        db.session.commit()
        
        return deletion_counts
        
    except Exception as e:
        db.session.rollback()
        print(f"❌ Error deleting data of date {date_str} from extractions, hse_variants and extraction_questions: {str(e)}")
        raise e

def get_filepath(filename="Invariants.xlsx"):
    backend_dir = os.path.dirname(os.path.abspath(__file__))
    input_dir = os.path.join(backend_dir, "input")
    file_path = os.path.join(input_dir, filename)

    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Could not find Excel file at: {file_path}")

    return file_path

def get_lists_of_cost_centers_by_management_mode(db, management_mode):
    try:
        # Fetch distinct station codes from hse_variants table
        station_codes_query = text("""
            SELECT DISTINCT `station code` FROM hse_variants
            WHERE `station code` IS NOT NULL
        """)
        station_codes_result = db.session.execute(station_codes_query).fetchall()
        cost_centers_list = [row[0] for row in station_codes_result]

        if not cost_centers_list:
            print("⚠️ No station codes found in hse_variants table")
            return []

        invariants_path = get_filepath("Invariants.xlsx")
        df = pd.read_excel(invariants_path, header=4)

        df.columns = df.columns.str.strip()

        # Find the relevant columns (case-insensitive search)
        cost_center_col = None
        management_method_col = None

        for col in df.columns:
            col_lower = col.lower()
            if "cost center" in col_lower or "cost centre" in col_lower:
                cost_center_col = col
            elif "management method" in col_lower or "management mode" in col_lower:
                management_method_col = col

        # Check if required columns were found
        if not cost_center_col or not management_method_col:
            print(f"Warning: Required columns not found!")
            print(f"Cost Center: {cost_center_col}, Management Method: {management_method_col}")
            return []

        mode_lower = str(management_mode).lower().strip()

        cost_centers_lower = [str(cc).lower().strip() for cc in cost_centers_list]

        matching_cost_centers = []

        for _, row in df.iterrows():
            cost_center = row[cost_center_col]
            management_method = row[management_method_col]

            if pd.isna(cost_center) or pd.isna(management_method):
                continue

            if str(management_method).lower().strip() == mode_lower:
                cost_center_trimmed = str(cost_center).strip()
                if cost_center_trimmed.lower() in cost_centers_lower:
                    matching_cost_centers.append(cost_center_trimmed)

        # Remove duplicates while preserving order
        seen = set()
        result = []
        for cc in matching_cost_centers:
            cc_lower = cc.lower()
            if cc_lower not in seen:
                seen.add(cc_lower)
                result.append(cc)

        return result

    except Exception as e:
        print(f"❌ Error in get_lists_of_cost_centers_by_management_mode: {str(e)}")
        return []


def get_lists_of_cost_centers_by_segmentation(db, segmentation):
    try:
        # Fetch distinct station codes from hse_variants table
        station_codes_query = text("""
            SELECT DISTINCT `station code` FROM hse_variants
            WHERE `station code` IS NOT NULL
        """)
        station_codes_result = db.session.execute(station_codes_query).fetchall()
        cost_centers_list = [row[0] for row in station_codes_result]

        if not cost_centers_list:
            print("⚠️ No station codes found in hse_variants table")
            return []

        # Read the Excel file
        invariants_path = get_filepath("Invariants.xlsx")
        df = pd.read_excel(invariants_path, header=4)

        df.columns = df.columns.str.strip()

        # Find the relevant columns (case-insensitive search)
        cost_center_col = None
        segmentation_col = None

        for col in df.columns:
            col_lower = col.lower()
            if "cost center" in col_lower or "cost centre" in col_lower:
                cost_center_col = col
            elif "segmentation" in col_lower:
                segmentation_col = col

        # Check if required columns were found
        if not cost_center_col or not segmentation_col:
            print(f"Warning: Required columns not found!")
            print(f"Cost Center: {cost_center_col}, Segmentation: {segmentation_col}")
            return []

        # Normalize the segmentation to lowercase for comparison
        seg_lower = str(segmentation).lower().strip()

        # Convert input cost centers to lowercase for comparison
        cost_centers_lower = [str(cc).lower().strip() for cc in cost_centers_list]

        # Filter data based on segmentation and cost centers
        matching_cost_centers = []

        for _, row in df.iterrows():
            cost_center = row[cost_center_col]
            segmentation_value = row[segmentation_col]

            # Skip rows with missing values
            if pd.isna(cost_center) or pd.isna(segmentation_value):
                continue

            # Check if segmentation matches and cost center is in the list
            if str(segmentation_value).lower().strip() == seg_lower:
                cost_center_trimmed = str(cost_center).strip()
                if cost_center_trimmed.lower() in cost_centers_lower:
                    matching_cost_centers.append(cost_center_trimmed)

        # Remove duplicates while preserving order
        seen = set()
        result = []
        for cc in matching_cost_centers:
            cc_lower = cc.lower()
            if cc_lower not in seen:
                seen.add(cc_lower)
                result.append(cc)

        return result

    except Exception as e:
        print(f"❌ Error in get_lists_of_cost_centers_by_segmentation: {str(e)}")
        return []