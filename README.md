
# dbt Airbnb Amsterdam

Projet de transformation de données **Airbnb Amsterdam** utilisant **dbt** et **Snowflake**.

## Description

Ce projet implémente un pipeline ELT pour transformer et analyser des données
Airbnb de la ville d'Amsterdam (listings, hôtes, reviews) en suivant
les bonnes pratiques dbt : snapshots → curation → analyse.
Il inclut également des données sur la **fréquentation touristique d'Amsterdam**
de 2012 à 2023.

## Source des données

Les données proviennent de [Inside Airbnb](https://insideairbnb.com/get-the-data/)
— un projet open source qui collecte des données publiques Airbnb par ville.

Données utilisées :
- **Listings** : annonces de logements à Amsterdam
- **Hosts** : informations sur les hôtes
- **Reviews** : avis des voyageurs
- **Tourists per year** : fréquentation touristique Amsterdam 2012–2023 (seed)

## Stack technique

| Outil | Usage |
|-------|-------|
| **dbt Cloud** | Transformation & orchestration |
| **Snowflake** | Data warehouse (base : `airbnb`) |
| **SQL / Jinja** | Modélisation des données |


## Structure du projet

dbt_airbnb/
├── models/
│ └── curation/
│ ├── curation_hosts.sql
│ ├── curation_listings.sql
│ ├── curation_listings_inc.sql ← modèle incrémental
│ ├── curation_reviews.sql
│ └── curation_tourists_per_year.sql
├── snapshots/
│ ├── listings_snapshot.sql ← SCD strategy: check all cols
│ └── hosts_snapshot.sql ← SCD strategy: check all cols
├── seeds/
│ └── tourists_per_year.csv ← fréquentation 2012-2023
├── sources.yaml ← définition sources + tests + unit tests
├── packages.yml
└── dbt_project.yml



