import re
import numpy as np
import pandas as pd
from datetime import datetime
from dateutil import parser

class DataCleaner:
    def __init__(self):
        pass

    def clean_numeros(self, value):
        """Nettoie les numéros selon plusieurs critères."""
        if pd.isna(value) or str(value).strip().lower() in {"non renseigné", "non", "nan", "none", ""}:
            return np.nan, 0
        if isinstance(value, float) and value.is_integer():
            value = str(int(value))       
        value = str(value).strip().replace("\xa0", "").replace("\t", "").replace("\n", "")
        
        # Cas 1 : Lettres invalides
        if any(c.isalpha() for c in value) and not re.search(r'[eE]\+?\d', value):
            return np.nan, 8
        
        # Cas 2 : Notation scientifique mal écrite
        if re.match(r"^\d+,\d+E\+\d+$", value):
            value = value.replace(',', '.')
            try:
                cleaned_value = str(int(float(value)))
                if len(cleaned_value) > 14:
                    return np.nan, 9
                return cleaned_value.zfill(14), 1
            except (OverflowError, ValueError):
                return np.nan, 9
        
        # Cas 3 : Nombre à 14 chiffres
        if re.match(r"^\d{14}$", value):
            return value, 6
        
        # Cas 4 : Suppression des espaces
        if ' ' in value:
            return value.replace(" ", "").zfill(14), 2
        
        # Cas 5 : Suppression des décimales
        if re.match(r"^\d+[.,]\d{2}$", value):
            return value.split(',')[0].zfill(14) if ',' in value else value.split('.')[0].zfill(14), 3
        
        # Cas 6 : Nombre à 12 chiffres
        if value.isdigit() and len(value) < 14:
            return value.zfill(14), 5
        # if value.endswith('.0'):
        #     value = value.split('.')[0]       

        return value, 7

    def extract_siren_from_siret(self, siret):
        """Extrait les 9 premiers chiffres du SIRET pour obtenir le SIREN."""
        if isinstance(siret, str) and len(siret) == 14 and siret.isdigit():
            return siret[:9]
        return np.nan

    def detect_and_fix_dates(self, dates):
        """Détecte et corrige les erreurs de format de date, renvoie une liste de dates formatées et l'année extraite."""
        fixed_dates, years = [], []
        
        for date in dates:
            if pd.isna(date):
                fixed_dates.append(None)
                years.append(None)
                continue

            date = str(date).strip()
            if date.isdigit() and len(date) == 4:  # Cas d'une année seule
                year = int(date)
                fixed_dates.append(None if 2016 <= year <= datetime.now().year else None)
                years.append(int(year) if 2016 <= year <= datetime.now().year else None)
                continue

            try:
                parsed_date = parser.parse(date, fuzzy=True)
                if 2000 <= parsed_date.year <= datetime.now().year:
                    fixed_dates.append(parsed_date.strftime("%Y-%m-%d"))
                    years.append(int(parsed_date.year))
                else:
                    fixed_dates.append(None)
                    years.append(None)
            except Exception:
                fixed_dates.append(None)
                years.append(None)

        return fixed_dates, years
    

    def filter_and_log_removals(self, df, montant_column, annee_column, id_columns):
        """
        Supprime les lignes où au moins une des valeurs dans les colonnes spécifiées (montant, année, idAttribuant, idAcheteur, idBeneficiaire)
        est nulle (NaN). Conserve ces lignes supprimées dans un DataFrame et affiche le nombre de lignes supprimées.
        """
        condition_annee = df[annee_column].isna().any(axis=1) if isinstance(annee_column, list) else df[annee_column].isna()
        condition_ids = df[id_columns].isna().any(axis=1) if isinstance(id_columns, list) else df[id_columns].isna()

        condition = df[montant_column].isna() | condition_annee | condition_ids

        removed_rows = df[condition]
        df_cleaned = df[~condition]

        print(f"Lignes supprimées où au moins une des valeurs ({montant_column}, {annee_column}, {', '.join(id_columns)}) est nulle : {removed_rows.shape[0]} lignes")

        return df_cleaned, removed_rows
    
    def drop_duplicates_except(self, df, exclude_columns):
        """
        Supprime les doublons tout en excluant certaines colonnes.
        """
        return df.drop_duplicates(subset=[col for col in df.columns if col not in exclude_columns])


    def clean_montant(self, df, montant_column='montant'):
        
        """Nettoie et transforme la colonne 'montant'."""
        df[montant_column] = df[montant_column].apply(clean_numeros).astype(float)
        df[montant_column] = df[montant_column].map(abs)
        
        return df
    
    def clean_codecpv(self, value):
        """Nettoie la valeur codecpv pour la convertir en chaîne valide."""
        if pd.isna(value):
            return 'Unknown'
        return str(value)
    

    def apply_cleaning(self, df, date_columns=None, id_columns=None):
        """Applique le nettoyage des numéros et la gestion des dates sur un DataFrame."""
        for col, clean_col, siren_col in id_columns:
            df[[clean_col, f"{clean_col}_conversion_type"]] = df[col].apply(lambda x: pd.Series(self.clean_numeros(x)))
            df[siren_col] = df[clean_col].apply(self.extract_siren_from_siret)

        # Nettoyer les dates (dateColumns) pour 'dateConvention', 'datenotification' etc.
        for col, formatted_col, year_col in date_columns:
            df[formatted_col], df[year_col] = self.detect_and_fix_dates(df[col])

        
        return df
    



    #Utilisation de fonction de la classe DataCleaner: 

# date_columns_mp_final = [
#     ('datenotification', 'datenotificationFormatted', 'annéesnotification'),
#     ('datepublicationdonnees', 'datepublicationdonneesFormatted', 'annéespublicationdonnees')
# ]
# id_columns_mp_final = [
#     ('acheteur_id', 'acheteur_id_clean', 'siren_acheteur')
# ]

# mp_clean = data_cleaner.apply_cleaning(normalized_data_marches_publics, date_columns_mp_final, id_columns_mp_final)


# mp_clean['codecpv'] = mp_clean['codecpv'].apply(DataCleaner().clean_codecpv)


