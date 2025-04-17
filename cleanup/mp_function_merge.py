def cpv_to_long(cpv):
    """ (as a df) to create a long list of the CPV codes at differente levels of aggreagtion
    This can be used to aggregate the data at different levels later. The output is saved as a df (second parameter)"""

    # Rename variables
    cpv['cpv_10'] = cpv['CODE']
    cpv['cpv_label'] = cpv['FR']

    # Extract the codes at different levels of aggregation
    cpv_columns = [2, 3, 4, 5, 8]
    for length in cpv_columns:
        cpv[f'cpv_{length}'] = cpv['CODE'].str[:length]


    # Create the numeric values to filter the labels by tier
    cpv['digit_3'] = cpv['CODE'].apply(lambda x: x[2:3]).astype(int)
    cpv['digit_4'] = cpv['CODE'].apply(lambda x: x[3:4]).astype(int)
    cpv['digit_5'] = cpv['CODE'].apply(lambda x: x[4:5]).astype(int)
    cpv['digit_6_8'] = cpv['CODE'].apply(lambda x: x[5:8]).astype(int)

    cpv['cpv_2_bin'] = (cpv[['digit_3', 'digit_4', 'digit_5', 'digit_6_8']].sum(axis=1) == 0).astype(int)
    cpv['cpv_3_bin'] = (cpv[['digit_4', 'digit_5', 'digit_6_8']].sum(axis=1) == 0).astype(int) - cpv['cpv_2_bin']
    cpv['cpv_4_bin'] = (cpv[['digit_5', 'digit_6_8']].sum(axis=1) == 0).astype(int) - cpv['cpv_3_bin'] - cpv['cpv_2_bin']
    cpv['cpv_5_bin'] = (cpv[['digit_6_8']].sum(axis=1) == 0).astype(int) - cpv['cpv_4_bin'] - cpv['cpv_3_bin'] - cpv['cpv_2_bin']
    cpv['cpv_8_bin'] = (cpv['digit_6_8'] > 0).astype(int)

    # Save the CPV groups is different DF to merge labels later
    levels = [2, 3, 4, 5, 8, 10]
    cpv_dfs = []

    for level in levels:
        cpv_bin_column = f'cpv_{level}_bin'
        cpv_column = f'cpv_{level}'

        cpv_temp = cpv[cpv[cpv_bin_column] == 1][[cpv_column, 'cpv_label']].reset_index(drop=True)
        cpv_temp['level'] = level
        cpv_temp.rename(columns={cpv_column: 'code'}, inplace=True)

        cpv_dfs.append(cpv_temp)

    return pd.concat([cpv_2, cpv_3, cpv_4, cpv_5, cpv_8, cpv_10], ignore_index=True)



def create_mp_staging(mp, cpv_long, missing_string='Unknown'):
    """Nettoie toutes les colonnes du dataframe des marchés publics normalisés."""

    # Setup
    cols_name = mp.columns
    mp_clean = mp.copy()


    ### 1. CPV codes
    # Clean and recode
    mp_clean['codecpv'] = mp_clean['codecpv'].fillna(missing_string)
    mp_clean['cpv_8'] = mp_clean['codecpv'].apply(lambda x: x[:8] if len(str(x)) >= 8 else missing_string)
    mp_clean['cpv_2'] = mp_clean['cpv_8'].apply(lambda x: x[:2] if x != missing_string else missing_string)

    # Add CPV labels (2 digit and 8 digit)
    mp_clean['cpv_2_label'] = mp_clean['cpv_2'].map(cpv_long.set_index('code')['cpv_label'])
    mp_clean['cpv_8_label'] = mp_clean['cpv_8'].map(cpv_long.set_index('code')['cpv_label'])

    ### # 2. Drop acheteur nom: 83% missing
    del mp_clean['acheteur_nom']



    # Create a new column with whether the disclosure was mandatory
    mp_clean['obligation_publication'] = pd.cut(
        mp_clean['montant'],
        bins=[-float('inf'), 40000, float('inf')],
        labels=['Optionnel', 'Obligatoire'],
        right=False
    )

    # Check on the delay to publish
    mp_clean['delaipublication_jours'] = (mp_clean['datepublication'] - mp_clean['datenotification']).dt.days


    ### 5. Buyer information
    # Recode buyer data
    mp_clean['acheteur_sirene'] = mp_clean['acheteur_id']
    mp_clean['acheteur_siren'] = mp_clean['siren']
    mp_clean['acheteur_type'] = mp_clean['type']
    mp_clean['acheteur_nom'] = mp_clean['nom']

    # Recode seller data

    def clean_seller_list(seller_list):
        '''Turns string into list
        If 'nan' value then returns empty list'''
        #If Null value, return None
        if str(seller_list).lower() in ['nan', 'none', 'null']:
            return []
        return list(eval(str(seller_list)))


    mp_clean['titulaires_liste_noms'] = mp_clean['titulaires'].map(clean_seller_list)
    mp_clean['titulaires_nombre'] = mp_clean['titulaires_liste_noms'].map(len)
    # mp_clean['seller_name_list'] = mp_clean['titulaires']
    # mp_clean['titulaires_cleaned'] = mp_clean['titulaires'].str.replace(r"[\[\]']", "", regex=True)


    # # TO DO: Add seller_siren and seller_sirene

    ### 6. Recode formeprix
    recode_dict = {
        'Ferme et actualisable': 'Mixte',
        'Ferme': 'Fermé',
        'Révisable': 'Révisable',
        'Unitaire': 'Fermé',
        'Forfaitaire': 'Fermé',
        'Mixte': 'Mixte',
        'Ferme, actualisable': 'Mixte',
        'None': np.nan}

    # Apply the recoding to the 'formeprix' column
    mp_clean['formeprix'] = mp_clean['formeprix'].replace(recode_dict).fillna(missing_string)

    ### 7. Recode nature
    recode_dict = {
        'MARCHE': 'Marché',
        'Marche': 'Marché',
        'ACCORD-CADRE': 'Accord-cadre',
        'MARCHE SUBSEQUENT': 'Marché subséquent',
        'MARCHE DE PARTENARIAT': 'Marché de partenariat',
        'Accord-cadre': 'Accord-cadre',
        'Marché': 'Accord-cadre',
        'Marché subséquent': 'Marché subséquent',
        'Marché de partenariat': 'Marché de partenariat',
        'Marché hors accord cadre': 'Marché hors accord cadre',
        'None': np.nan}

    # Apply the recoding to the 'nature' column
    mp_clean['nature'] = mp_clean['nature'].replace(recode_dict).fillna(missing_string)

    ### 8. Recode mp['dureemois']
    print(f"Cleaning duration of contract in months: {len(mp_clean)}")
    # Convert to numeric
    mp_clean['dureemois'] = pd.to_numeric(mp_clean['dureemois'], errors='coerce')

    # # Drop values above 300
    # mp_clean.loc[mp_clean['duration_months_numeric'] > 300, 'duration_months_numeric'] = np.nan
    # mp_clean = mp_clean[mp_clean['duration_months_numeric'].notna()]
    # print(f"    After dropping durations over 25 years: {len(mp_clean)}")

    # Replace 0 with NA
    # mp_clean.loc[mp_clean['duration_months_numeric'] == 0, 'duration_months_numeric'] = np.nan
    # mp_clean = mp_clean[mp_clean['duration_months_numeric'].notna()]
    # print(f"    After dropping zeros: {len(mp_clean)}")

    # 9. Recode procedure
    recode_dict = {'Procédure adaptée': 'Procédure adaptée',
    "Appel d'offres ouvert": "Appel d'offres ouvert",
    'Marché négocié sans publicité ni mise en concurrence préalable': 'Marché public négocié sans publicité ni mise en concurrence préalable',
    'Procédure négociée avec mise en concurrence préalable': 'Procédure négociée avec mise en concurrence préalable',
    'Marché public négocié sans publicité ni mise en concurrence préalable': 'Marché public négocié sans publicité ni mise en concurrence préalable',
    'Procédure concurrentielle avec négociation': 'Procédure concurrentielle avec négociation',
    "Appel d'offres restreint": "Appel d'offres restreint",
    'Marché passé sans publicité ni mise en concurrence préalable': 'Marché passé sans publicité ni mise en concurrence préalable',
    'Procédure avec négociation': 'Procédure avec négociation',
    'Dialogue compétitif': 'Dialogue compétitif',
    'NC': np.nan,
    'ProcÃ©dure adaptÃ©e': 'Procédure adaptée',
    'Appel d’offres restreint': "Appel d'offres restreint"}

    # Drop NaN values
    print(f"Cleaning procedure: {len(mp_clean)}")
    mp_clean['procedure'] = mp_clean['procedure'].replace(recode_dict).fillna("Non spécifié")
    # mp_clean = mp_clean[mp_clean['procedure_clean'].notna()]
    # print(f"    After dropping NaNs: {len(mp_clean)}")

    print(f"Share of dropped observations: {1 - len(mp_clean) / len(mp)}")
    mp_clean = mp_clean[['acheteur_siren', 'acheteur_type', 'acheteur_nom','acheteur_sirene',
                         'titulaires_liste_noms', 'titulaires_nombre',
                         'objet', 'nature', '_type', 'formeprix', 'lieuexecution_typecode', 'uid',
                         'montant', 'id', 'lieuexecution_code', 'dureemois',
                         'procedure', 'lieuexecution_nom',
                         'codecpv', 'cpv_8', 'cpv_2', 'cpv_2_label', 'cpv_8_label',
                         'obligation_publication', 'datenotification', 'datenotification_annee',
                         'datepublication', 'datepublication_annee', 'delaipublication_jours'
                         ]]
    return mp_clean
