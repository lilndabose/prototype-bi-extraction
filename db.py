import os
from dotenv import load_dotenv
from sqlalchemy import text

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