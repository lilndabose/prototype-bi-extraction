from difflib import get_close_matches
import unidecode

countries_map = {
    "south africa": "ZA", "afrique du sud": "ZA",
    "mauritius": "MU", "maurice": "MU",
    "cameroon": "CM", "cameroun": "CM",
    "cote divoire": "CI", "cote d'ivoire": "CI", "ivory coast": "CI", "cote ivoire": "CI",
    "senegal": "SN", "sénégal": "SN",
    "reunion": "RE", "réunion": "RE",
    "eswatini": "SZ",
    "togo": "TG",
    "ghana": "GH",
    "uganda": "UG",
    "congo": "CG", "congo brazzaville": "CG","congo brazza": "CG",
    "ethiopia": "ET", "ethiopie": "ET",
    "tanzania": "TZ", "tanzanie": "TZ",
    "gabon": "GA",
    "guinea": "GN", "guinée": "GN",
    "equatorial guinea": "GQ", 
    "guinée équatoriale": "GQ",
    "guinée equitoriale": "GQ",
    "kenya": "KE",
    "mayotte": "YT",
    "malawi": "MW",
    "morocco": "MA", "maroc": "MA",
    "mozambique": "MZ",
    "tunisia": "TN", "tunisie": "TN",
    "namibia": "NA", "namibie": "NA",
    "nigeria": "NG", "nigéria": "NG",
    "zambia": "ZM", "zambie": "ZM",
    "zimbabwe": "ZW",
    "madagascar": "MG",
    "madagasikara": "MG",
    "rdc": "CD", "democratic republic of congo": "CD",
    "burkina faso": "BF",
    "erythree": "ER", "érythrée": "ER",
    "chad": "TD", "tchad": "TD",
    "mali": "ML",
    "angola": "AO",
    "egypt": "EG", "egypte": "EG",
    "botswana": "BW",
    "centafrique": "CE", 
    "central african republic": "CE",
}

def get_country_map():
    return countries_map

def normalize_country(affiliate_name):
    if not isinstance(affiliate_name, str):
        return None
    name_lower = affiliate_name.strip().lower()
    
    if name_lower in countries_map:
        return name_lower.title()
    
    matches = get_close_matches(name_lower, countries_map.keys(), n=1, cutoff=0.7)
    if matches:
        return matches[0].title() 
    
    return affiliate_name

def get_country_code(country_name):
    if not isinstance(country_name, str):
        return None
    code = countries_map.get(country_name.lower())
    return code

def extract_country(affiliate_name):
    if not isinstance(affiliate_name, str):
        return None
    affiliate_clean = unidecode.unidecode(affiliate_name.lower())
    for country_name in countries_map.keys():
        country_clean = unidecode.unidecode(country_name.lower())
        if country_clean in affiliate_clean:
            return country_name.title()
    return None