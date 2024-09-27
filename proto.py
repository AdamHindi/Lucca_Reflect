import os
import time
import requests
import json

import aiohttp
import asyncio

import csv

#header request pour l'api
headers = {
    'Accept': 'application/json',
    'Authorization': 'lucca application=caf8058b-b7ec-4df2-85e3-a673b5466e97',
}

#Initialiser les ids de tout les utilisateurs présents dans la base
#Toutes les données récupérées seront enregistrées dans le dossier /data/ local
#Enfin retourne une liste des ids dans la base de donnée
def init():
    current_directory = os.getcwd()
    data = os.path.join(current_directory, r'data')
    ids = os.path.join(data, r'ids')
    if not os.path.exists(data):
        os.makedirs(data)
        os.makedirs(ids)

    response = requests.get('https://reflect2-sandbox.ilucca-demo.net/api/v3/users/', headers=headers )

    data = response.json()
    with open('data/users.json', 'w') as f:
        json.dump(data, f,indent=4)
    
    ids = []
    for item in data.get("data",{}).get("items",{}):
        ids.append(item.get("id",{}))
    return ids

#Fonction asynchrone qui permet de récuperer les données de chaque connexion d'une manière indépendante
#Permet aussi de prendre en compte le nombre maximum de requêtes par minutes imposé par la demo du API
#Permet aussi de savoir quel lien donne une mauvaise réponse à partir des status responses dans la documentation du API
async def fetch_json(session, url, delay=60):
    try:
        async with session.get(url) as response:
            if response.status == 200:
                return await response.json()
            elif response.status == 429:
                print(f"Rate limited. Retrying after {delay} seconds for {url}.")
                await asyncio.sleep(delay)  # Wait for the delay
                return await fetch_json(session, url, delay)  # Retry request after delay
            else:
                print(f"Error {response.status} for URL: {url}")
    except aiohttp.ClientError as e:
        print(f"Client error: {e} for URL {url}")
    return None

#Permet d'envoyer des requêtes d'une manière asynchrone à partir de la liste de liens des utilisateurs
async def fetch_all(urls):
    async with aiohttp.ClientSession(headers=headers) as session:
        tasks = []
        for url in urls:
            task = asyncio.ensure_future(fetch_json(session, url))
            tasks.append(task)
        
        # Rassemble les réponses des requêtes
        json_responses = await asyncio.gather(*tasks)
        return [response for response in json_responses if response is not None]  # Filtrer les réponses None

# Import tout les infos des salariés selon leur id
# Optimisation asynchrone puisque c'est des requêtes GET
def import_workers(ids):
    urls = ['https://reflect2-sandbox.ilucca-demo.net/api/v3/users/'+str(id) for id in ids]
    loop = asyncio.get_event_loop()
    json_data_list = loop.run_until_complete(fetch_all(urls))
    for worker in json_data_list:
        id = worker.get('data',{}).get('id',{})
        with open('data/ids/id'+str(id)+'.json', 'w') as f:
            json.dump(worker, f,indent=4)

# Première visualisation des données des salariés sous forme de CSV
def workers_info():
    current_directory = os.getcwd()
    data = os.path.join(current_directory, r'data')
    workers = os.path.join(data, r'workers')
    ids = os.path.join(data, r'ids')

    if not os.path.exists(workers):
        os.makedirs(workers)
    workers_list=[]
    for id in os.listdir(ids):
        filename = os.fsdecode(id)
        file = 'data/ids/'+filename
        json_data = json.loads(open(file).read())
        worker = json_data.get('data',{})
        wId= worker.get('id',{})
        fName = worker.get('firstName',{})
        lName = worker.get('lastName',{})
        gender = worker.get('gender',{})
        address = worker.get('address',{})
        nat = worker.get('nationalityId',{})
        dep = worker.get('department',{}).get('name',{})
        job = worker.get('jobTitle',None)
        dContractStart = worker.get('dtContractStart',None)[:10]
        dContractEnd = str(worker.get('dtContractEnd',None))[:10]
        legalEntity = worker.get('legalEntity',{}).get('name',{})
        workers_list.append([wId,lName,fName,gender,nat,address,legalEntity,dep,job,dContractStart,dContractEnd])
    
    workers_list.sort(key=lambda worker : worker[0])

    with open('data/workers/workers', 'w', newline='') as csvfile:
        spamwriter = csv.writer(csvfile, delimiter=' ',
                        quotechar='|', quoting=csv.QUOTE_ALL)
        for w in workers_list:
            spamwriter.writerow(w)

# Visualise les salariés selon les départements
def dep_info():
    current_directory = os.getcwd()
    data = os.path.join(current_directory, r'data')
    dep = os.path.join(data, r'dep')
    ids = os.path.join(data, r'ids')

    if not os.path.exists(dep):
        os.makedirs(dep)

    departments={}
    for id in os.listdir(ids):
        filename = os.fsdecode(id)
        file = 'data/ids/'+filename
        json_data = json.loads(open(file).read())
        worker = json_data.get('data',{})
        wId= worker.get('id',{})
        name = worker.get('displayName',{})
        gender = worker.get('gender',{})
        dep = worker.get('department',{}).get('name',{}).replace('/', "_")
        legalEntity = worker.get('legalEntity',{}).get('name',{})
        job = worker.get('jobTitle','Non')
        manager = worker.get('manager',{}).get('name',{})


        depEntity =  legalEntity + "_" + dep
        if depEntity not in departments:
            departments[depEntity]=[]
        
        departments[depEntity].append([wId,name,gender,job,manager])

    for k in departments:
        department = departments[k]
        with open('data/dep/'+k, 'w', newline='') as csvfile:
            for i in range(len(department)):
                spamwriter = csv.writer(csvfile, delimiter=' ',
                                quotechar='|', quoting=csv.QUOTE_ALL)
                spamwriter.writerow(department[i])


# Implémentation d'une requête générale qui permet de trier selon une colonne de la base de donnée, avec les "info" demandées
# Enregistre ces données dans folder
# Assez limité vu que c'est pour une requête générale, néanmoins j'ai essayé le plus pour qu'elle soit capable de repliquer un SQL
# Permet aussi de naviguer selon les profondeurs d'un objet en json avec l'utilisation de '/'
def getRequest(folder, along, info=['id']):
    current_directory = os.getcwd()
    data = os.path.join(current_directory, r'data')
    fol = os.path.join(data, folder)
    ids = os.path.join(data, r'ids')
    if not os.path.exists(fol):
        os.makedirs(fol)
    
    request={}
    
    for id in os.listdir(ids):
        filename = os.fsdecode(id)
        file = 'data/ids/'+filename
        json_data = json.loads(open(file).read())
        worker = json_data.get('data',{})



        tempAlong = along.split('/')

        alongOne = worker.get(tempAlong[0],{})
        for nest in tempAlong[1:]:
            alongOne = alongOne.get(nest,{})
        
        if alongOne not in request:
            request[alongOne]=[]
        
        L = []
        for i in info:
            iNests = i.split('/')
            iOne = worker.get(iNests[0],{})
            for nest in iNests[1:]:
                iOne = iOne.get(nest,{})
            L.append(iOne)
        request[alongOne].append(L)

    for k in request:
        req = request[k]
        with open('data/'+folder+'/'+k.replace('/', "_"), 'w', newline='') as csvfile:
            for i in range(len(req)):
                spamwriter = csv.writer(csvfile, delimiter=' ',
                                quotechar='|', quoting=csv.QUOTE_ALL)
                spamwriter.writerow(req[i])

# Une implémentation alternative utilisant la requête générale
def dep_info_2():
    getRequest("dep2","department/name",['id','displayName','jobTitle','manager/name'])

# Enregistre les infos selon les managers
def manager_info():
    getRequest('managers','manager/name',['id','displayName','jobTitle','legalEntity/name'])

# Initialisation importante pour démarer la base de données
ids=init()
import_workers(ids)

#Suggestions d'implémentation
workers_info()
dep_info()
manager_info()
