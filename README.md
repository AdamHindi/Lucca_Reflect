# Solutions Engineer - Coding Challenge

## Solution

Ma solution utilise un seul fichier python, avec une phase d'initialisation et ensuite de récupération des données générale.
- Implémentation d'une requête générale qui imite un peu SQL
- Implémentation de requêtes asynchrones afin d'optimiser la vitesse de récupération (surtout dans un vrai contexte sans limite aussi basse de 50/min)
- Implémentation d'un timeout quand un maximum de requêtes est atteint

## Contexte

Afin de garantir des données à jour dans les dashboards de la solution Reflect, l'équipe Solutions & Data Engineer a développé une architecture permettant de collecter les données de sources externes et de les intégrer dans le data warehouse de Reflect ([BigQuery](https://cloud.google.com/bigquery?hl=en)).

En tant que Solutions Engineer, vous serez amené à faire évoluer ces jobs pour :

- mettre à jour un job existant afin d'intégrer de nouveaux types de données, ou s'assurer que les données sont toujours correctement collectées
- créer de nouveaux jobs pour intégrer de nouvelles sources de données

Le but de cet exercice est de vous mettre dans la situation d'un membre de l'équipe Solutions & Data Engineer, et développer un job en Python permettant la collecte de données depuis une API externe.

## Description de l'exercice

### Périmètre des données à collecter

Vous devez développer un job Python qui collecte les données de l'[API de Lucca](https://developers.lucca.fr/docs/lucca-legacyapi/011f7e77fd583-list-users), afin de récupérer les salariés d'une entreprise cliente de Reflect.

> [!NOTE]
> Lucca est un logiciel SIRH permettant la gestion des salariés, contrats de travail, paie, absences... Il est utilisé par de nombreuses entreprises et start-ups et représente l'une des sources de données les plus courantes pour les dashboards de Reflect.

Le job devra permettre de récupérer les informations suivantes :

- l'ensemble des salariés de l'entreprise, en considérant ceux qui sont actuellement dans l'entreprise mais aussi ceux qui l'ont quittés ou ceux qui vont la rejoindre dans le futur, avec les informations qui vous sembleront pertinentes sur ces salariés (nom, prénom, genre, etc...)
- les contrats de travail de chacun de ces salariés, avec notamment la date de début, date de fin si elle existe, titre du poste, département...
- les détails sur les départements de l'entreprise, pour notamment pouvoir reconstruire une hiérarchie des départements de l'entreprise

Une fois les données collectées, elles devront être stockées en local, au format que vous souhaitez (CSV, JSON, base de données, etc.).

> [!IMPORTANT]
> Afin de vous connecter à l'API de Lucca, vous aurez besoin d'un token d'authentification, ainsi que de l'URL de l'API. Le token d'authentification vous a été envoyé par mail, quant à l'URL de l'API, elle est la suivante : `https://reflect2-sandbox.ilucca-demo.net`.

### Architecture du job

L'architecture du projet est laissé volontairement libre. Vous pouvez utiliser les librairies et outils que vous souhaitez pour développer le job, et vous êtes libre de structurer le projet comme vous le souhaitez.

Quelques critères à garder en tête :

- la récupération des données devra se faire dans un temps relativement raisonnable (quelques dizaines de minutes) #Done, async + delay (limitation en récupération, 60s par 50 requetes içi)
- le code devra être lisible, maintenable et exécutable sur une machine d'un autre membre de l'équipe
- certaines parties de votre code pourraient être réutilisées par d'autres jobs de collecte de données
- la récupération des données se fait quotidiennement pour tous les clients, votre code peut tenir compte de cette itération.


## Conseils

- Ne vous focalisez pas sur la qualité des données récupérées de l'API Lucca; l'objectif ici est principalement sur la structure et qualité du job d'ingestion des données.
- Si vous avez un point de bloquage, n'hésitez pas à contacter l'équipe Reflect pour obtenir de l'aide !
