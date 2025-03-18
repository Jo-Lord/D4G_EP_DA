import numpy as np
import pandas as pd
import re
from datetime import datetime
import unidecode
from cleanup.cleanup_siret_functions import clean_numeros

def create_collec_staging(collectivities:pd.DataFrame):
    """Clean collectivities dataframe,data source: all_communities_data.csv"""

    #Setup
    cols_name = collectivities.columns
    collectivities_clean = collectivities.copy()
    
     ### 0. Duplicates
    print(f"Cleaning duplicates: {len(collectivities_clean)} entries")
    collectivities_clean.drop_duplicates(inplace=True)
    print(f"After removing duplicates: {len(collectivities_clean)} entries")

    ### 1. Remove all columns that contain too many missing values: over 90%
    print(f'drop colummns: {collectivities_clean.shape[1]} columns')
    collectivities_clean.drop(['cog_3digits','url_ptf','url_datagouv','id_datagouv','merge','ptf',"Unnamed: 0"], axis=1, inplace=True)
    print(f' After droping columns : {collectivities_clean.shape[1]} columns')

    ### 2. Removing rows where the 'nom', 'type', and 'siren' fields are missing or duplicated.
    print(f"Cleaning duplicates 'nom', 'type', and 'siren': {len(collectivities_clean)} entries")
    collectivities_clean.drop_duplicates(subset=["nom","siren","type"], inplace=True)
    print(f"After removing duplicates: {len(collectivities_clean)} entries")

    ### 3. Siren,epci
    # clean
    collectivities_clean["siren"] = collectivities_clean["siren"].apply(clean_numeros)
    collectivities_clean["epci"] = collectivities_clean["epci"].apply(clean_numeros)

    ### 4. String code columns
    # siren, type, code_departement,code_region, cog,code_departement_3digits
    def uper_strip(code):
        if pd.notna(code) : 
           return str(code).strip().upper()
        else : return code
    collectivities_clean["siren"] = collectivities_clean["siren"].apply(uper_strip)
    collectivities_clean["type"] = collectivities_clean["type"].apply(uper_strip)
    collectivities_clean["code_departement"] = collectivities_clean["code_departement"].apply(uper_strip)
    collectivities_clean["cog"] = collectivities_clean["cog"].apply(uper_strip)
    collectivities_clean["code_departement_3digits"] = collectivities_clean["code_departement_3digits"].apply(uper_strip)

    ### 5. Population, trancheeffectifsunitelegale
    # clean and recode
    collectivities["population"] = collectivities["population"].apply(clean_numeros).apply(lambda x:int(x) if pd.notna(x) else x)
    collectivities["trancheeffectifsunitelegale"] = collectivities["trancheeffectifsunitelegale"].apply(lambda x: int(x) if pd.notna(x) else x)

    ### 6. effectifssup50
    # type bool
    collectivities["effectifssup50"] = collectivities["effectifssup50"].astype(bool)
    
    ### 7. rename columns 
    collectivities_clean = collectivities_clean.rename(columns={"code_departement": "code_dept"})

    ### to do
    # transforme code_dept to list or delete exinsting list
    
    return collectivities_clean











