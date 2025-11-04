SUB_ZONES = {
    "ACE": [
        "Angola",
        "Congo",
        "Kenya",
        "Malawi",
        "RDC",
        "République Démocratique du Congo",
        "Tanzania",
        "Uganda",
        "Zambie",
        "Zimbabwe"
    ],
    "ASNE": [
        "Eswatini",
        "Namibie",
        "S. Africa",
        "South Africa",
        "Afrique du sud"
    ],
    "AFA": [
        "Eswatini",
        "Namibia",
        "S. Africa",
        "South Africa",
        "Afrique du sud"
    ],
    "AFO": [
        "Cameroun",
        "CIV",
        "Côte d'Ivoire",
        "Gabon",
        "Ghana",
        "Guinée Co.",
        "Guinée Eq",
        "Guinée Conakry",
        "Guinée Équatoriale",
        "Sénégal",
        "Togo",
        "Ivory Coast"
    ],
    "MOI": [
        "Ethiopie",
        "Ethiopia",
        "Mada.",
        "Madagascar",
        "Maroc",
        "Mauritius",
        "Mozambique",
        "Tunisie",
        "Tunisia",
    ],
    "NIGERIA": [
        "NIGERIA"
    ]
}

def get_countries_by_subzone(sub_zone: str) -> list[str]:
    return SUB_ZONES.get(sub_zone, [])