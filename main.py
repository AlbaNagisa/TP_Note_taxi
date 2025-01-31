import os
import subprocess
import time

from aggregation import CurrentlyStoppedCabs, MostStoppedTaxis, CompanyWithMostLevel5Autonomy, CompanyFleetAverageAge, \
    StoppedPositionByCity, StoppedCabsCitiesAsc
from aggregation.moving_taxis_count import MovingTaxisCount
from database import Database
import json


def main():
    db = Database()
    db.connect()
    db.set_database('Alban_Girardi_Taxis')
    db.create_collection(os.getenv("COLLECTION_NAME"), validator={
        '$jsonSchema': {
            'bsonType': 'object',
            'required': ["name", "city", "address", "founded_year", "fleet_size", "taxis"],
            'properties': {
                'name': {
                    'bsonType': 'string',
                    'description': 'Company name'
                },
                'city': {
                    'bsonType': 'string',
                    'description': 'City where the company is located'
                },
                'address': {
                    'bsonType': 'string',
                    'description': 'Company address'
                },
                'founded_year': {
                    'bsonType': 'int',
                    'description': 'Year when the company was founded'
                },
                'fleet_size': {
                    'bsonType': 'int',
                    'description': 'Number of taxis in the company'
                },
                'contact_email': {
                    'bsonType': 'string',
                    'description': 'Company contact email'
                },
                'contact_phone': {
                    'bsonType': 'string',
                    'description': 'Company contact phone'
                },
                'taxis': {
                    'bsonType': 'array',
                    'description': 'List of taxis',
                    'items': {
                        'bsonType': 'object',
                        'required': ['autonomy_level', "year_of_production", "license_plate", "manufacturer",
                                     "battery_capacity_kwh", "current_status", "location"],
                        'properties': {
                            'autonomy_level': {
                                'bsonType': 'int',
                                'description': 'Autonomy level of the taxi'
                            },
                            'year_of_production': {
                                'bsonType': 'int',
                                'description': 'Year when the taxi was produced'
                            },
                            'license_plate': {
                                'bsonType': 'string',
                                'description': 'License plate of the taxi'
                            },
                            'manufacturer': {
                                'bsonType': 'string',
                                'description': 'Manufacturer of the taxi'
                            },
                            'battery_capacity_kwh': {
                                'bsonType': 'int',
                                'description': 'Battery capacity in kWh'
                            },
                            'current_status': {
                                'bsonType': 'string',
                                'description': 'Current status of the taxi'
                            },
                            'location': {
                                'bsonType': 'object',
                                'required': ['lat', 'lon'],
                                'properties': {
                                    'lat': {
                                        'bsonType': 'double',
                                        'description': 'Latitude of the taxi'
                                    },
                                    'lon': {
                                        'bsonType': 'double',
                                        'description': 'Longitude of the taxi'
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    })
    db.create_timeseries_collection("location", timeseries={
        'timeField': 'timestamp',
        'metaField': 'license_plate',
        'granularity': 'seconds'
    })
    if db[os.getenv("COLLECTION_NAME")].count_documents({}) == 0:
        print("Seeding...")
        db.seed_database(os.getenv("COLLECTION_NAME"), json.load(open('taxis.json')))

    db.create_index('location', {'license_plate': 1})
    db.create_index('location', {'status': 1})
    db.create_index(os.getenv("COLLECTION_NAME"), {'city': 1})
    db.create_index(os.getenv("COLLECTION_NAME"), {'name': 1})

    try:
        subprocess.Popen(["python", "mqtt/mqtt_listener.py"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        print("Listener lancé avec succès.")
    except Exception as e:
        print(f"Erreur lors du lancement : {e}")
    try:
        while True:
            choice = input(
                """\n
A - Comptez, par ville, le nombre d'entrées représentant les taxis en mouvement dans la dernière heure.
B - Le taxi qui s'est arrêté le plus de fois (avec les informations de son entreprise)
C - Identifiez l’entreprise ayant le plus grand nombre de taxis avec un niveau d’autonomie de 5.
D - Pour chaque entreprise, calculez la moyenne d’âge de leur flotte (basez-vous sur year_of_production).
E - Déterminez les zones (basées sur des quadrants de coordonnées GPS) où les taxis s’arrêtent le plus fréquemment dans une ville donnée.
F - Les villes avec les taxis ayant le plus tendances à s'arreter, par ordre croissant
G - Identifiez les taxis actuellement à l'arret et créez en une vue.

[Q pour quitter]
""")
            print("\n\n")
            match choice.lower():
                case "a":
                    [print(f"{city}: {count}") for city, count in MovingTaxisCount().get_result()]
                case "b":
                    [print(
                        f"Le taxi qui c'est arreté le plus de fois est: {taxi['license_plate']} ({taxi['count']} arrêts) - {taxi['agency']['name']} ({taxi['agency']['city']})")
                     for taxi in MostStoppedTaxis().get_result()]
                case "c":
                    [print(
                        f"L'agence de taxis avec le plus de batterie niveau 5 est: {company['_id']} ({company['city']}) avec {company['count']} taxis")
                     for company in CompanyWithMostLevel5Autonomy().get_result()]
                case "d":
                    [print(
                        f"L'age de la flotte de l'agence {company['_id']} ({company['city']}) est de: {company['average_age']} ans")
                     for company in CompanyFleetAverageAge().get_result()]

                case "e":
                    city_choice = input(
                        """\n
Selectionnez une ville:
A - Paris
B - Nice
C - Lyon
D - Marseille
Q - Revenir en arrière
""")
                    city_string = None
                    print("\n\n")
                    match city_choice.lower():
                        case "a":
                            city_string = "Paris"
                        case "b":
                            city_string = "Nice"
                        case "c":
                            city_string = "Lyon"
                        case "d":
                            city_string = "Marseille"
                        case "q":
                            continue
                        case _:
                            print("Merci d'entrer A, B, C ou D.")
                    if city_string is not None:
                        [print(
                            f"Pour {company['_id']} la zone où les taxis s'arrete le plus a pour latitude: {company['lat']} et longitude: {company['lon']}")
                         for company in StoppedPositionByCity(city_string).get_result()]
                case "f":
                    [print(f"{company['_id']}: {company['totalStoppedCabs']} taxis arretés") for company in
                     StoppedCabsCitiesAsc().get_result()]
                case "g":
                    CurrentlyStoppedCabs()
                case "q":
                    print("Au revoir !")
                    break
                case _:
                    print("Merci d'entrer A, B, C, D, E, F, G ou Q.")

            time.sleep(3)

    except KeyboardInterrupt:
        print("Au revoir !")
        return


if __name__ == "__main__":
    main()
