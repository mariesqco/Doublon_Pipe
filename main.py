import re
import config
import requests
import pandas as pd
import urllib.parse
import time


def marie():
    filename = "waalaxy_exported_prospects_1 - waalaxy_exported_prospects_1 (1).csv"

    # Charger les fichiers CSV
    waalaxy_data = pd.read_csv(f"csv/{filename}",delimiter=",")

    # Définir la partie fixe de l'URL Pipedrive
    base_url = "https://api.pipedrive.com/v1/organizations/search?"

    orga_id=[]
    orga_label_id=[]
    # Boucle à travers toutes les company_name
    for x in waalaxy_data['company_name']:
        try:
            x_lower = x.lower()
            xp = x_lower.split(" (")[0].strip().capitalize()
        except:
            xp = ""

        if xp.startswith('groupe'):
            xp = xp.replace("groupe ", "").strip().capitalize()
        elif xp.endswith('groupe'):
            xp = xp.replace(" groupe", "").strip().capitalize()
        elif xp.endswith('group'):
            xp = xp.replace(" group", "").strip().capitalize()
        elif xp.endswith(' sa'):
            xp = xp.replace(" sa", "").strip().capitalize()
        elif xp.endswith(' sas'):
            xp = xp.replace(" sas", "").strip().capitalize()
        elif xp.endswith(' sarl'):
            xp = xp.replace(" sarl", "").strip().capitalize()
        elif xp.endswith(' ltd.'):
            xp = xp.replace(" ltd.", "").strip().capitalize()
        elif xp.endswith(' ltd'):
            xp = xp.replace(" ltd", "").strip().capitalize()
        else:
            pass  # Si aucun des cas ci-dessus n'est vrai, laissez-le inchangé


        base_url = "https://api.pipedrive.com/v1/itemSearch?"

        url = f"{base_url}term={xp}&item_types=organization&api_token={config.API_Pipedrive}"

        response = requests.get(url)
        response_json = response.json()
        time.sleep(1)

        try :
            org_id = response_json["data"]["items"][0]["item"]["id"]
        except :
            org_id = "vide"
        print(xp, ">", org_id)


        orga = requests.get(f'https://supertripper.pipedrive.com/api/v1/organizations/{org_id}/?api_token={config.API_Pipedrive}')
        orga = orga.json()
        print(orga)
        try :
            org_label_id = (orga['data']["label"])
        except: org_label_id = ""

        orga_id.append(org_id)
        orga_label_id.append(org_label_id)
    waalaxy_data["org_id"]=orga_id
    waalaxy_data["org_label_id"]=orga_label_id
    waalaxy_data.to_csv("csv/check_pipe.csv")


    # récupération des id labels et noms des labels sur Pipe
def label_pipe():
    orgafield = f"https://api.pipedrive.com/v1/organizationFields?api_token={config.API_Pipedrive}"
    response = requests.get(orgafield)
    response_json = response.json()
    org_label_id = (response_json['data'])
    l_name=[]
    l_id=[]
    for i in org_label_id:
        if "label" in i["key"]:
            options = i["options"]
            for x in options:
                org_label_name = (x["label"])
                l_name.append(org_label_name)
                org_label_id = (x["id"])
                l_id.append(org_label_id)
                print(org_label_name, org_label_id)
    df = pd.DataFrame(list(zip(l_id,l_name)),columns=['id','name'])
    df.to_csv("csv/keep/liste.csv")


#comparer les labels du pipe avec les labels de la liste (du csv)
def comparaison():
    df_label = pd.read_csv("csv/keep/liste.csv")
    df_check_pipe = pd.read_csv("csv/check_pipe.csv")
    l_label=[]
    for c in range(len(df_check_pipe)):
        id_pipe = (df_check_pipe["org_label_id"][c])
        try :
            search = df_label.loc[df_label['id'] == id_pipe, 'name'].values[0]
        except :
            search = ""
        l_label.append(search)

    df_check_pipe["label_name"] = l_label
    df_check_pipe.to_csv("csv/results.csv")


marie()
label_pipe()
comparaison()






