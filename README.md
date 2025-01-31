## Description
Ce projet permet de gérer une flotte de taxis, de calculer des statistiques et d'analyser les données des taxis.  
Ce projet est uniquement à but éducatif et ne doit pas être utilisé en production.
Il comprend un affichage dans le terminal uniquement

## Prérequis
- Python 3.x
- Docker
- Docker Compose
- uv
- direnv

La plus part des installations qui vont suivre peuvent etre facultatives en fonction de votre environnement deja en place.  

## Installation recommandée

### Installation de l'environnement

```bash
cp .envrc.dist .envrc
```

```


```bash
# Installation de direnv
sudo apt install direnv # Ubuntu
brew install direnv # MacOS
```

```bash
# Configuration de direnv
echo 'eval "$(direnv hook bash)"' >> ~/.bashrc
source ~/.bashrc
```

Assurez vous que direnv est bien installé en tapant la commande `direnv` dans le terminal.

```bash
direnv allow
```

Referez vous à la [documentation](https://direnv.net/docs/installation.html) de direnv pour plus d'informations sur la configuration ou l'installation de direnv.

```bash
# Installation de uv
curl -LsSf https://astral.sh/uv/install.sh | sh # Linux & MacOS
powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex" # Windows
```
Une fois uv installé

```bash
uv python install 3.13 # Installation de l'interpreter Python 3.13
```


### Installation des dépendances
```bash
# Installation des dépendances
uv venv # Création de l'environnement virtuel
uv sync # Installation des dépendances
```


## Installation manuelle

Libre a vous d'installer les dépendances manuellement en vous referent au version dans pyproject.toml.
Les variables d'environnement sont à configurer dans le fichier .envrc et sont necessaire au fonctionnement de l'application.

## Utilisation

### Lancement de la base de données
```bash
docker-compose up -d
```


### Lancement de l'application
```bash
python main.py
```

