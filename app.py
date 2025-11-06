from flask import Flask, request, jsonify
from flask_sqlalchemy import SQLAlchemy
from flask_bcrypt import Bcrypt
from db import get_db_uri, delete_data_by_date, get_lists_of_cost_centers_by_segmentation, get_lists_of_cost_centers_by_management_mode
from init import init_db
from flask_cors import CORS
import os
from werkzeug.utils import secure_filename
from datetime import datetime
from flask import send_from_directory
from werkzeug.security import safe_join
import subprocess
import sys
from sqlalchemy import text


app = Flask(__name__)
CORS(app)

UPLOAD_FOLDER = os.path.join(os.getcwd(), 'uploads')
ALLOWED_EXTENSIONS = {'csv', 'xlsx'}

app.config['SQLALCHEMY_DATABASE_URI'] = get_db_uri()
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

if not os.path.exists(UPLOAD_FOLDER):
    os.makedirs(UPLOAD_FOLDER)

db = SQLAlchemy(app)
bcrypt = Bcrypt(app)

class Users(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    email = db.Column(db.String(255), unique=True, nullable=False)
    firstname = db.Column(db.String(255), nullable=True)
    lastname = db.Column(db.String(255), nullable=True)
    password = db.Column(db.String(255), nullable=False)
    
class FileUploads(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, nullable=False)
    filename = db.Column(db.String(255), nullable=False)
    filetype = db.Column(db.String(100), nullable=False)
    file_size = db.Column(db.String(50), nullable=False)
    date_created = db.Column(db.Date, nullable=False)
    upload_date = db.Column(db.DateTime, server_default=db.func.current_timestamp())
    updated_at = db.Column(db.DateTime, server_default=db.func.current_timestamp(), server_onupdate=db.func.current_timestamp())

# initializer la base de données
with app.app_context():
    init_db(app, db, bcrypt)

def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS
           
def format_to_k(num: int) -> str:
    if num >= 1000:
        return f"{num / 1000:.2f}k"
    return str(num)

# point d'entrée pour cree un utilisateur
@app.route('/register', methods=['POST'])
def register():
    data = request.get_json()
    email = data.get('email')
    password = data.get('password')

    if Users.query.filter_by(email=email).first():
        return jsonify({"error": "Email deja enregistre"}), 400

    hashed_pw = bcrypt.generate_password_hash(password).decode('utf-8')
    new_user = Users(email=email, password=hashed_pw, firstname=data.get('firstname'), lastname=data.get('lastname'))
    db.session.add(new_user)
    db.session.commit()

    return jsonify({"message": "Utilisateur cree avec succes"}), 201

# point d'entrée pour connecter un utilisateur
@app.route('/login', methods=['POST'])
def login():
    data = request.get_json()
    email = data.get('email')
    password = data.get('password')

    user = Users.query.filter_by(email=email).first()
    print("User found: " + str(user))
    if user and bcrypt.check_password_hash(user.password, password):
        return jsonify({"message": "Connexion reussie", "data": {"email": user.email, "id": user.id}}), 200
    else:
        return jsonify({"error": "Email ou mot de passe invalide"}), 401
    
@app.route('/uploads/<path:filename>')
def serve_file(filename):
    safe_path = safe_join(UPLOAD_FOLDER, filename)
    if not safe_path or not os.path.exists(safe_path):
        return jsonify({"error": "Fichier non trouvé"}), 404
    return send_from_directory(UPLOAD_FOLDER, filename)
    
@app.route('/files', methods=['GET'])
def list_files():
    user_id = request.args.get('user_id')
    if not user_id:
        return jsonify({"error": "L'utilisateur n'est pas connecté. Veuillez vous connecter puis réessayer."}), 400

    files = FileUploads.query.filter_by(user_id=user_id).all()
    files_data = [
        {
            "id": f.id,
            "filename": f.filename,
            "filetype": f.filetype,
            "file_size": f.file_size,
            "date_created": f.date_created.strftime('%Y-%m-%d'),
            "upload_date": f.upload_date.strftime('%Y-%m-%d %H:%M:%S'),
            "updated_at": f.updated_at.strftime('%Y-%m-%d %H:%M:%S')
        } for f in files
    ]
    return jsonify({"files": files_data}), 200
    
@app.route('/upload', methods=['POST'])
def upload_file():
    if 'file' not in request.files:
        return jsonify({"error": "Aucun fichier dans la requête"}), 400
    
    file = request.files['file']
    user_id = request.args.get('user_id')
    date_created_str = request.args.get('date_creation')

    if file.filename == '':
        return jsonify({"error": "Nom de fichier vide"}), 400

    if file and allowed_file(file.filename):
        filename = secure_filename(file.filename)
        filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
        file.save(filepath)
        stat_info = os.stat(filepath)
        
        # Use the date from query params if provided, otherwise use file creation time
        if date_created_str:
            try:
                created_at = datetime.strptime(date_created_str, '%Y-%m-%d').strftime('%Y-%m-%d %H:%M:%S')
            except ValueError:
                return jsonify({"error": "Format de date invalide. Utilisez YYYY-MM-DD"}), 400
        else:
            created_at = datetime.fromtimestamp(stat_info.st_ctime).strftime('%Y-%m-%d %H:%M:%S')
        
        size_mb = stat_info.st_size / (1024*1024)
        file_size_str = f"{size_mb:.4f} MB"

        new_upload = FileUploads(
            user_id=user_id,
            filename=filename,
            filetype=file.content_type,
            file_size=file_size_str,
            date_created=created_at
        )
        db.session.add(new_upload)
        db.session.commit()

        return jsonify({
            "message": "Fichier uploadé avec succès",
            "filename": filename,
            "path": filepath
        }), 201
    else:
        return jsonify({"error": "Une erreur s'est produite. Veuillez vérifier le type/taille (csv uniquement) du fichier puis réessayer."}), 400

@app.route('/delete-file/<int:file_id>', methods=['DELETE'])
def delete_file(file_id):
    user_id = request.args.get('user_id', type=int)

    if not user_id:
        return jsonify({"error": "Impossible de supprimer le fichier. Reconnectez-vous puis réessayez."}), 400

    file_record = FileUploads.query.filter_by(id=file_id).first()

    if not file_record:
        return jsonify({"error": "Fichier introuvable"}), 404

    if file_record.user_id != user_id:
        return jsonify({"error": "Vous n'êtes pas autorisé à supprimer ce fichier"}), 403

    filepath = os.path.join(UPLOAD_FOLDER, secure_filename(file_record.filename))
    if os.path.exists(filepath):
        try:
            os.remove(filepath)
            delete_data_by_date(db, file_record.date_created)
        except Exception as e:
            return jsonify({"error": f"Impossible de supprimer le fichier physique: {str(e)}"}), 500
    else:
        return jsonify({"warning": "Fichier physique déjà supprimé"}), 200

    try:
        db.session.delete(file_record)
        db.session.commit()
    except Exception as e:
        return jsonify({"error": f"Impossible de supprimer l'enregistrement en base: {str(e)}"}), 500

    return jsonify({"message": "Fichier supprimé avec succès"}), 200


# recuperer les donnees pour les filtres de la bd
@app.route('/get-filters', methods=['GET'])
def get_filters():
    try:
        dates_query = db.session.query(FileUploads.date_created).distinct().all()
        files_creation_date = [date[0].strftime('%Y-%m-%d') for date in dates_query if date[0]]
        
        station_codes = []
        station_names = []
        
        try:
            # Get distinct station codes
            codes_query = db.session.execute(
                text("SELECT DISTINCT `station code` FROM hse_variants WHERE `station code` IS NOT NULL ORDER BY `station code`")
            ).fetchall()
            station_codes = [row[0] for row in codes_query if row[0]]
            
            # Get distinct station names
            names_query = db.session.execute(
                text("SELECT DISTINCT `station name` FROM hse_variants WHERE `station name` IS NOT NULL ORDER BY `station name`")
            ).fetchall()
            station_names = [row[0] for row in names_query if row[0]]
            
        except Exception as e:
            print(f"Une Erreur c'est produit lors que la recuperations des filtres: {str(e)}")
        
        dates = sorted(files_creation_date)
        return jsonify({
            "files_creation_date": [' '] + dates,  
            "station_code": station_codes,
            "station_name": station_names
        }), 200
        
    except Exception as e:
        return jsonify({
            "error": f"Erreur lors de la récupération des filtres: {str(e)}"
        }), 500
 
 
# get statistiques pour le dashboard
@app.route('/get-statistics-by-filter', methods=['GET'])
def get_stats():
    query_params = request.args.to_dict()
    
    date_param = query_params.get('date')
    if not date_param:
        return jsonify({"error": "Le paramètre 'date' est requis"}), 400
    
    try:
        query_date = datetime.strptime(date_param, '%Y-%m-%d').date()
        
    except ValueError:
        return jsonify({"error": "Format de date invalide. Utilisez YYYY-MM-DD"}), 400
    
    sub_zone_param = query_params.get('sub_zone')
    zone_param = query_params.get('zone')
    affiliate_param = query_params.get('affiliate')
    station_code_param = query_params.get('station_code')
    station_name_param = query_params.get('station_name')
    management_mode_param = query_params.get('management_mode')
    segmentation_param = query_params.get('segmentation')
    selected_station_codes = []
    selected_segmentation_station_codes = []

    if( management_mode_param ):
        selected_station_codes = get_lists_of_cost_centers_by_management_mode(db, management_mode_param)
    if( segmentation_param ):
        selected_segmentation_station_codes = get_lists_of_cost_centers_by_segmentation(db, segmentation_param)

    try:
        where_conditions = ["DATE(`date`) = :query_date"]
        query_params_dict = {"query_date": query_date}

        if sub_zone_param:
            where_conditions.append("`sub-zone` = :sub_zone")
            query_params_dict["sub_zone"] = sub_zone_param
            
        if zone_param:
            where_conditions.append("`zone` = :zone")
            query_params_dict["zone"] = zone_param
            
        if affiliate_param:
            where_conditions.append("`affiliate` = :affiliate")
            query_params_dict["affiliate"] = affiliate_param

        if station_code_param:
            where_conditions.append("`station code` = :station_code")
            query_params_dict["station_code"] = station_code_param

        if station_name_param:
            where_conditions.append("`station name` = :station_name")
            query_params_dict["station_name"] = station_name_param
        
        # Filter by management mode station codes
        if management_mode_param and selected_station_codes:
            if len(selected_station_codes) > 0:
                placeholders = ','.join([f':station_code_{i}' for i in range(len(selected_station_codes))])
                where_conditions.append(f"`station code` IN ({placeholders})")
                for i, code in enumerate(selected_station_codes):
                    query_params_dict[f'station_code_{i}'] = code
        
        # Filter by segmentation station codes
        if segmentation_param and selected_segmentation_station_codes:
            if len(selected_segmentation_station_codes) > 0:
                placeholders = ','.join([f':seg_station_code_{i}' for i in range(len(selected_segmentation_station_codes))])
                where_conditions.append(f"`station code` IN ({placeholders})")
                for i, code in enumerate(selected_segmentation_station_codes):
                    query_params_dict[f'seg_station_code_{i}'] = code
        
        where_clause = " AND ".join(where_conditions)
        
        # Query to get averages for each EP, ES, ET column
        query = text(f"""
            SELECT 
                AVG(`ep01`) as ep01_mean, AVG(`ep02`) as ep02_mean, AVG(`ep03`) as ep03_mean,
                AVG(`ep04`) as ep04_mean, AVG(`ep05`) as ep05_mean, AVG(`ep06`) as ep06_mean,
                AVG(`ep07`) as ep07_mean, AVG(`ep08`) as ep08_mean, AVG(`ep09`) as ep09_mean,
                AVG(`ep10`) as ep10_mean, AVG(`ep11`) as ep11_mean,
                AVG(`es01`) as es01_mean, AVG(`es02`) as es02_mean, AVG(`es03`) as es03_mean,
                AVG(`es04`) as es04_mean, AVG(`es05`) as es05_mean, AVG(`es06`) as es06_mean,
                AVG(`es07`) as es07_mean, AVG(`es08`) as es08_mean, AVG(`es09`) as es09_mean,
                AVG(`et01`) as et01_mean, AVG(`et02`) as et02_mean, AVG(`et03`) as et03_mean,
                AVG(`et04`) as et04_mean, AVG(`et05`) as et05_mean,
                SUM(CASE WHEN `zone` = 'AFR' THEN 1 ELSE 0 END) as afr_count,
                COUNT(*) as total_records
            FROM hse_variants
            WHERE {where_clause}
        """)
        
        result = db.session.execute(query, query_params_dict).fetchone()
        
        # Query to calculate total score mean per station, then average across all stations
        total_score_query = text(f"""
            SELECT AVG(station_mean) as total_score_mean
            FROM (
                SELECT 
                    `station code`,
                    (
                        AVG(`ep01`) + AVG(`ep02`) + AVG(`ep03`) + AVG(`ep04`) + AVG(`ep05`) + 
                        AVG(`ep06`) + AVG(`ep07`) + AVG(`ep08`) + AVG(`ep09`) + AVG(`ep10`) + AVG(`ep11`) +
                        AVG(`es01`) + AVG(`es02`) + AVG(`es03`) + AVG(`es04`) + AVG(`es05`) + 
                        AVG(`es06`) + AVG(`es07`) + AVG(`es08`) + AVG(`es09`) +
                        AVG(`et01`) + AVG(`et02`) + AVG(`et03`) + AVG(`et04`) + AVG(`et05`)
                    ) / 25 as station_mean
                FROM hse_variants
                WHERE {where_clause}
                GROUP BY `station code`
            ) as station_scores
        """)
        
        total_score_result = db.session.execute(total_score_query, query_params_dict).fetchone()
        total_score_mean = round(float(total_score_result[0]), 2) if total_score_result and total_score_result[0] is not None else 0
        
        # Query to get management modes statistics from extractions table
        # Filter by date and zone='All' and sub-zone='All'
        management_modes_query = text("""
            SELECT 
                `coco inspected`,
                `codo inspected`,
                `dodo inspected`,
                `stations inspected`
            FROM extractions
            WHERE DATE(`date`) = :query_date 
            AND `zone` = 'All' 
            AND `sub-zone` = 'All'
            LIMIT 1
        """)
        
        management_result = db.session.execute(management_modes_query, query_params_dict).fetchone()
        
        # Calculate percentages
        management_modes = {
            "COCO": 0,
            "CODO": 0,
            "DODO": 0
        }
        
        if management_result:
            try:
                # Convert to integers, handling potential None values
                coco_inspected = int(management_result[0]) if management_result[0] is not None else 0
                codo_inspected = int(management_result[1]) if management_result[1] is not None else 0
                dodo_inspected = int(management_result[2]) if management_result[2] is not None else 0
                stations_total = int(management_result[3]) if management_result[3] is not None else 0
                
                if stations_total > 0:
                    management_modes["COCO"] = round((coco_inspected / stations_total) * 100)
                    management_modes["CODO"] = round((codo_inspected / stations_total) * 100)
                    management_modes["DODO"] = round((dodo_inspected / stations_total) * 100)
            except (ValueError, TypeError) as e:
                print(f"Error converting management modes data: {e}")
                # Keep default values of 0
                
        # query to get the total records and afr records
        total_records_query = text("""
            SELECT SUM(CASE WHEN `zone` = 'AFR' THEN 1 ELSE 0 END) as afr_count,
                COUNT(*) as total_records from hse_variants WHERE DATE(`date`) = :query_date 
        """)
        
        total_records_result = db.session.execute(total_records_query, query_params_dict).fetchone()
        total_afr_records = total_records_result[0] if total_records_result else 0
        
        if not result or result[-1] == 0:
            return jsonify({
                "date": date_param,
                "zone": zone_param,
                "sub_zone": sub_zone_param,
                "station_code": station_code_param,
                "station_name": station_name_param,
                "affiliate": affiliate_param,
                "message": "Aucune donnée trouvée pour ces filtres",
                "data": None,
                "management_modes": management_modes
            }), 200

        ep_data = {}
        es_data = {}
        et_data = {}
        
        # EP columns (indices 0-10)
        for i in range(11):
            ep_num = f"EP{str(i+1).zfill(2)}"
            value = result[i]
            ep_data[ep_num] = round(float(value), 2) if value is not None else 0
        
        # ES columns (indices 11-19)
        for i in range(9):
            es_num = f"ES{str(i+1).zfill(2)}"
            value = result[11 + i]
            es_data[es_num] = round(float(value), 2) if value is not None else 0
        
        # ET columns (indices 20-24)
        for i in range(5):
            et_num = f"ET{str(i+1).zfill(2)}"
            value = result[20 + i]
            et_data[et_num] = round(float(value), 2) if value is not None else 0
        
        return jsonify({
            "date": date_param,
            "zone": zone_param,
            "sub_zone": sub_zone_param,
            "station_code": station_code_param,
            "station_name": station_name_param,
            "affiliate": affiliate_param,
            "total_records": format_to_k(result[-1]),
            "total_afr_records": format_to_k(total_afr_records),
            "total_score_mean": total_score_mean,
            "management_modes": management_modes,
            "ep": ep_data,
            "es": es_data,
            "et": et_data
        }), 200
        
    except Exception as e:
        return jsonify({
            "error": f"Erreur lors de la récupération des statistiques: {str(e)}"
        }), 500
                
# declancher l'extraction des fichier
@app.route('/extract', methods=['GET'])
def execute_test():
    env = os.environ.copy()
    env['PYTHONIOENCODING'] = 'utf-8'
    
    try:
        # Execute extraction.py en utilisant les subprocess
        result = subprocess.run(
            [sys.executable, 'scripts/extraction.py'],
            capture_output=True,
            text=True,
            timeout=60,  # 1 minute -> 60s
            env=env,
            encoding='utf-8',
            errors='replace'
        )
        
        # verifier si extraction terminer
        if result.returncode == 0:
            return jsonify({
                'status': 'success',
                'message': 'done',
                'output': result.stdout
            }), 200
        else:
            return jsonify({
                'status': 'error',
                'message': "Un problème est survenu lors de l’exécution du script",
                'error': result.stderr
            }), 500
            
    except subprocess.TimeoutExpired:
        return jsonify({
            'status': 'error',
            'message': "Une erreur s’est produite pendant l’extraction des données."
        }), 500
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': f"Une erreur inattendue s'est produite: {str(e)}"
        }), 500

if __name__ == '__main__':
    app.run(debug=True)

