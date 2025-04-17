import numpy as np                
import matplotlib.pyplot as plt
import pandas as pd
import os
import datetime as dt


# Uniformisation des formats SIREN de subventions_staging_sep.csv
def adaptSirenFormat(x) :
    if x.find("225300000") == 0 :
        return "225300011"      # Mayenne parfois associée à 225300000 dans subventions_staging_sep.csv, mais ce numéro n'existe pas
    elif x.find(";") > 0 :
        if x == "2;335E+13" :
            return "223500018"
        if x == "2;28E+13" :
            return "228000014"
        else :
            return "None"
        #return float(x.replace(";", "."))/1e5
    elif len(x.split()) == 1 :
        return x[:9]
    elif len(x.split()) >= 3 :
        if x.find(u'\xa0') > 0 :
            return x.replace(u'\xa0', u'')[:9]
        return x.replace(" ", "")[:9]




# Chargement des données, budget et dépenses
dfBudget = pd.read_pickle("data/budgetCleanAfter2016.pickle")   # Sortie du script proposé pour la tâche BACK00133
dfSubSpent = pd.read_csv("data/subventions_staging_sep.csv", dtype = {"idattribuant": str} )    # Fichier fourni par Chloé

# Restriction aux colonnes d'intérêt pour gagner en temps d'exécution, formatage, clean des numéros SIREN
dfBudget["exercice"] = pd.to_datetime(dfBudget["exercice"], format='mixed')
dfSubSpent["attribuant_siren"] = dfSubSpent[ "idattribuant" ].dropna().apply(lambda x: int(adaptSirenFormat(x)) )

dfBudget = dfBudget[ ["exercice", "updatedSiren", "subventions"] ]
dfSubSpent = dfSubSpent[ [ "attribuant_siren", "montant", "nomattribuant", "year" ] ]
dfSubSpent = dfSubSpent[ dfSubSpent["year"] >= 2016 ]


# Comparaison des subventions budgétées et déclarées
tauxPublication = []
for year in dfSubSpent["year"].unique() :   # On traite toutes les années présentes dans les subventions versées
    groupedDf = dfSubSpent.where(dfSubSpent["year"] == year)[ ["attribuant_siren", "montant"] ].groupby("attribuant_siren").sum()       # On somme, par numéro SIREN, les montants versés
    for siren in groupedDf.index :
        collec = dfSubSpent[ dfSubSpent["attribuant_siren"] == int(siren) ]["nomattribuant"].iloc[0]    # On récupère le nom de la collectivité
        # On récupère le montant des subventions versées, puis le budget lors de l'année et pour la collectivité correspondantes
        subSpent = (groupedDf.loc[siren]["montant"])    
        subBudg = (dfBudget[ (dfBudget["exercice"] >= dt.datetime(year, 1, 1)) & (dfBudget["exercice"] < dt.datetime(year+1, 1, 1) ) & (dfBudget["updatedSiren"] == str( int(siren) ) ) ]["subventions"])

        # On calcule le taux de publication, on attribue la note du barème correspondant à ce taux et on remplit une liste pour construire un dataFrame par la suite
        if not subBudg.empty :
            tp = (subSpent/ (subBudg.values[0]*1000.) )*100.
            score = "E"
            if tp < 25 :
                score = "E"
            elif tp <= 50 :
                score = "D"
            elif tp <= 75 :
                score = "C"
            elif tp <= 95 :
                score = "B"
            elif tp <= 105 :
                score = "A"
            tauxPublication.append([collec, int(siren), year, "{:1.2e}".format(subSpent), "{:1.2e}".format(subBudg.values[0]*1000.), "{:2.2f}".format(tp), score])
        else :
            tauxPublication.append( [collec, int(siren), year, "{:1.2e}".format(subSpent), 0., np.nan, "E"] )

# Construction du DataFrame
tauxPubDict = pd.DataFrame(tauxPublication, columns=["nom", "siren", "year", "subSpent", "subBudg", "taux", "score"]).sort_values(by=["siren", "year"]) 

# Ecriture pour consultation des résultats (il n'y en a pas des masses... Et certains résultats sont très étonnants (genre Bretagne), mais pas trouvé de loup via cherry-picking)
#with open("details.dat", "w") as f :
#    print(tauxPubDict.sort_values(by=["siren", "year"]).to_string(), file=f)

