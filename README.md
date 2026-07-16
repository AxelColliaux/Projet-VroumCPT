# VroumCPT - Analyse d'états de véhicules en temps réel

Projet ESGI M2 AL — Introduction au traitement distribué.

L'idée : récupérer en continu les rapports d'état envoyés par les véhicules,
calculer des taux d'erreur avec Spark Structured Streaming, et lever des alertes
dans Kafka quand ça dépasse les seuils.

## Contributeurs

- Noureddine BEN SADOK
- Axel COLLIAUX
- Nouhaila MOUKADDIME
- Ilias EZEROUALI

## Ce qu'il y a dans le projet

- `generator/generator.py` : génère des rapports d'état (affichage console ou envoi Kafka)
- `streaming/streaming_analysis.py` : le job Spark Streaming qui calcule les 3 métriques et produit les alertes
- `batch/threshold_calculator.py` : job Spark batch qui calcule les seuils pour que les alertes ne se déclenchent qu'environ 1% du temps
- `docker-compose.yml` : l'infra de dev (Kafka + une UI Kafka)
- `docker-compose.secure.yml` : la version "comme en prod" avec SASL/SCRAM + TLS
- `scripts/` : génération des certificats TLS et création des topics
- `docs/` : la doc détaillée

## Pré-requis

- Docker et Docker Compose v2
- Python 3.10 ou plus
- Java 11+ et Apache Spark 3.5 (avec `spark-submit` accessible) si on veut lancer
  les jobs Spark en dehors de Docker

## Lancer le projet (mode dev)

D'abord on démarre Kafka :

```bash
docker compose up -d
```

L'interface Kafka est dispo sur http://localhost:8080. Les deux topics
(`vehicle-states` et `alerts`) sont créés tout seuls au démarrage.

Ensuite on installe les dépendances Python :

```bash
pip install -r generator/requirements.txt
pip install -r streaming/requirements.txt
```

Et on lance la chaîne complète. Dans un premier terminal, le générateur qui
envoie vers Kafka :

```bash
python generator/generator.py --kafka-broker localhost:29092 --rate 10 \
    --error-vehicle-pct 0.3 --high-error-rate 0.7
```

Dans un second terminal, le job Spark :

```bash
spark-submit \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
    streaming/streaming_analysis.py \
    --source kafka --kafka-broker localhost:29092 --sink kafka
```

Les alertes arrivent alors dans le topic `alerts`, visibles depuis l'UI Kafka.

Sinon, tout peut aussi tourner directement en conteneurs : un simple
`docker compose up` démarre le générateur et le job Spark en plus de Kafka.

### Version minimale (sans Kafka)

Le générateur affiche les rapports, on les passe à Spark via une socket :

```bash
# Terminal 1 — générateur vers netcat
python generator/generator.py --no-kafka --rate 5 | nc -lk 9999

# Terminal 2 — Spark lit la socket et écrit les alertes dans des fichiers
spark-submit streaming/streaming_analysis.py \
    --source socket --sink file --output-dir output/alerts
```

## Calculer les seuils (batch)

On génère un jeu de données, puis on calcule les seuils (99e percentile, soit
environ 1% d'alertes) :

```bash
mkdir -p output/data
python generator/generator.py --no-kafka --rate 20 --duration 120 > output/data/sample.json

spark-submit batch/threshold_calculator.py \
    --source files --input-dir output/data --output config/thresholds.json
```

Il suffit ensuite de repasser le fichier au job streaming avec
`--thresholds config/thresholds.json`.

## Mode sécurisé

```bash
# 1. Créer le fichier local de secrets et remplacer ses valeurs
cp .env.example .env

# 2. Générer les certificats TLS et les keystores (crée config/ssl/)
./scripts/generate_certs.sh

# 3. Démarrer la stack sécurisée (SASL/SCRAM + TLS, ACLs, générateur + Spark)
docker compose -f docker-compose.secure.yml up -d
```

Le détail (modèle de menace, choix faits et justifiés) est dans
[`docs/security.md`](docs/security.md).

## Doc détaillée

- [`docs/README.md`](docs/README.md) — l'archi et la structure du projet
- [`docs/generator.md`](docs/generator.md) — options du générateur et modèle d'erreurs
- [`docs/streaming.md`](docs/streaming.md) — le job streaming, les métriques, les options
- [`docs/security.md`](docs/security.md) — authentification, chiffrement, déploiement

## Arrêter

```bash
docker compose down
docker compose -f docker-compose.secure.yml down
```
