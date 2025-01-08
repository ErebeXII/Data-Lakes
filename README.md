# TP5 - Pipeline de traitement des données avec Airflow
Ce projet implémente une pipeline de traitement de données en trois étapes utilisant Airflow pour l'orchestration.

Vous pouvez toujours lancer la pipeline en local avec DVC si vous le désirez nonobstant.

## Architecture
La pipeline est composée de trois étapes principales :

* Raw : Extraction des données sources vers une zone brute
* Staging : Transformation et chargement dans MySQL
* Curated : Traitement final et stockage dans MongoDB


## Prérequis
Python 3.8+
DVC
MySQL
MongoDB
LocalStack (pour simuler S3)


## Trigger le DAG Airflow du TP4

* Lancez un **docker-compose build** pour installer l'environnement virtuel qui sera utilisé par Airflow. Airflow utilise son propre environnement d'exécution, c'est pourquoi nous ne passons plus simplement par un venv conda ou similaire, mais par un dockerfile.
* Lancez un **docker-compose up -d** pour lancer tous les services.
* Accédez à l'interface Airflow sur localhost:8081. Si besoin, remplacez localhost par l'ip locale de la VM ou du WSL 2 sur lequel vous faites tourner votre stack. 
* J'ai paramétré le docker-compose pour que l'identifiant et le mot de passe soient **airflow**
* Depuis votre terminal, lancez le script pipeline.py du dossier dags. Cela fera apparaître le DAG dans l'interface web de Airflow
* Vous pouvez maintenant trigger le DAG depuis l'interface graphique. Il sera automatiquement relancé à l'intervalle défini dans pipeline.py ...
* ... ce que nous allons utiliser pour remplir une base de données en continu depuis une API dans le TP5

## Votre objectif

* Suivez le sujet du TP5 pour récupérer des données depuis l'API de HackerNews
* Pour le faire vous avez 3 scripts à remplir : src/hn_api.py, src/es_handler.py, et dags/hackernews_dag.py
* Vous pouvez vous inspirer de l'exemple du TP4 pour faire fonctionner votre DAG

## TP5 
Pour accéder à la page airflow : (http://localhost:8081/)[here]
id : airflow
mdp : airflow

