import numpy as np                
import pandas as pd
import os
import datetime as dt


# Associe le numéro SIREN correspondant à la collectivité 
def associateCorrespondingSiren(allSiren_dict, sType, sSiren, sReg, sDept, sIcom) :
    if sType == "Groupement" : # Si info déjà présente
        return sSiren
    if sType == "Region" :   # S'il s'agit d'une région
        return allSiren_dict["Reg"][sReg]
    if sType == "Departement" :   # S'il s'agit d'un département
        return allSiren_dict["Dep"][sDept]
    else :  # S'il s'agit d'une commune
        corrsDept = sDept
        if sDept[0] == "1" :    # Cas des départements dans les CTU => 1xx dans les financial_accounts, 97 dans Collectivity_data
            corrsDept = "97"
        if corrsDept.lstrip("0")+sIcom in allSiren_dict["Com"] :    # Adaptation des formats
            return allSiren_dict["Com"][ corrsDept.lstrip("0")+sIcom ]
        else :
            #print(sDept+sIcom+" not resolved")     # Permet d'avoir la liste des communes non présentes dans Collectivity_data
            return None


# Permet d'obtenir le type de collectivite (région, département, groupement, commune)  
def associateCorrespondingType(sSiren, sReg, sDept, sIcom) :
    if sSiren != None : # Si info déjà présente
        return "Groupement"
    elif sReg != None and (sDept == None and sIcom == None) :   # S'il s'agit d'une région
        return "Region"
    elif sDept != None and (sReg == None and sIcom == None) :   # S'il s'agit d'un département
        return "Departement"
    else :  # S'il s'agit d'une commune
        return "Commune"




# Construction d'un dictionnaire de correspondance entre numéro SIREN et numéros de région/département/commune
allSiren = pd.read_csv("data/Collectivity_data.csv")

allCogs = allSiren[ "cog" ].values.flatten()
allTypes = allSiren[ "type" ].values.flatten()
allSir = allSiren[ "siren" ].values.flatten()
allSiren_dict = {"Reg": {}, "Dep": {}, "Com": {}}

for c, s, t in zip(allCogs, allSir, allTypes) :
    if t == "REG" or t == "CTU" :
        if len(c) == 1 :    # Manip pour cohérence de format avec les financial_accounts
            c = "10"+c
        elif len(c) == 2 :    # Manip pour cohérence de format avec les financial_accounts
            c = "0"+c
        allSiren_dict["Reg"][c] = str(s)
    if t == "DEP" or t == "CTU" :
        if len(c) == 1 :    # Manip pour cohérence de format avec les financial_accounts
            c = "10"+c
        elif len(c) < 3 :    # Manip pour cohérence 3 digits vs 2 digits (061 vs 61 p. ex.)
            c = "0"+c
        allSiren_dict["Dep"][c] = str(s)
    if t == "COM":
        allSiren_dict["Com"][c] = str(s)

# Manip particulière pour la Corse : CTU à partir de 2018, associé à 02A dans les financial_accounts.
# Or, 02A également associé à la Corse du Sud (jusqu'à 2017). Donc conflit avant 2018, sauf si on modifie les financial_accounts (genre 02A_ pour la Corse du Sud p. ex.)
# A noter qu'on le remplit à la main, car dans Collectivity_data.csv la CTU Corse est associée à 94
allSiren_dict["Dep"]["02A"] = 200076958  # Collectivité de Corse
allSiren_dict["Dep"]["02B"] = 172020018  # Haute-Corse
#allSiren_dict["Dep"]["02A_"] = 222000028 # Corse du Sud, Conflit avec la CTU

# Manip particulière : bas-rhin 226700011 avant 2021, collectivité européenne d'alsace 200094332 ensuite. Mais les deux sont associés à 067 dans les financial_accounts
# A noter qu'on le remplit à la main, car dans Collectivity_data.csv le code associé est 67A
# Possibilité de différencier bas-rhin et collectivité européenne d'alsace si on modifie les financial_accounts (genre 067_ pour le bas-rhin p.ex.)
allSiren_dict["Dep"]["067"] = 226700011  # Collectivité européenne d'Alsace
#allSiren_dict["Dep"]["067_"] = 200094332  # Bas-Rhin
allSiren_dict["Dep"]["068"] = 226800019  # Haut-rhin

# Manip particulière : pas de correspondance entre les valeurs des financial_accounts et de Collectivity_data, on le fait à la main
allSiren_dict["Dep"]["101"] = 229710017  # CTU Guadeloupe
allSiren_dict["Dep"]["102"] = 200052678  # CTU Guyane
allSiren_dict["Dep"]["104"] = 229740014  # CTU Réunion
allSiren_dict["Dep"]["106"] = 229850003  # CTU Mayotte

# Manip particulière : il manque des communes dans Collectivity_data.csv (on voit qu'il y a des 'trous', comme pour l'exemple ci-dessous)
# Il en manque quelques centaines, je n'ai pas cherché à aller plus loin
allSiren_dict["Com"]["1039"] = 211700414  # Bénon 




# Récupération des données budget
dfBudget = pd.read_parquet("data/financial_accounts.parquet")

# Association des SIREN manquants aux groupements 
# Important => limité à 2016 par les données de Collectivity_data (pas de numéros des régions pré-2016)
#           => Avant 2018, Corse du Sud pour 02A. Post 2018, Collectivité de Corse
#           => Avant 2021, Bas-Rhin. Post 2021, Collectivité Européenne d'Alsace
dfBudgetAfter2016 = dfBudget[ pd.to_datetime(dfBudget["exercice"], format='mixed') >= dt.datetime(2016, 1, 1) ]


print("SIREN inialement présents :" , dfBudgetAfter2016[ dfBudgetAfter2016["siren"].notnull() ].shape[0])
print("SIREN inialement manquants :" , dfBudgetAfter2016[ dfBudgetAfter2016["siren"].isnull() ].shape[0])
      
# Rajout des SIREN 
pd.options.mode.copy_on_write = True    # Utilisé pour se débarasser de 2 warnings : vérifier que c'est inoffensif
dfBudgetAfter2016["type"] = dfBudgetAfter2016.apply(lambda x: associateCorrespondingType(x.siren, x.region, x.dept, x.insee_commune), axis=1 )
dfBudgetAfter2016["updatedSiren"] = dfBudgetAfter2016.apply(lambda x: associateCorrespondingSiren(allSiren_dict, x.type, x.siren, x.region, x.dept, x.insee_commune), axis=1 )

print("SIREN présents après rajouts :" , dfBudgetAfter2016[ dfBudgetAfter2016["updatedSiren"].notnull() ].shape[0])
print("SIREN manquants après rajouts :" , dfBudgetAfter2016[ dfBudgetAfter2016["updatedSiren"].isnull() ].shape[0])

print(dfBudgetAfter2016)
dfBudgetAfter2016.to_pickle("data/budgetCleanAfter2016.pickle")  


# Liste des communes manquantes dans Collectivity_data, après rajout :
#print(dfBudgetAfter2016[ dfBudgetAfter2016["siren"].isnull() ])
