# 911 Calls avec MongoDB

## Import du jeu de données

Pour importer le jeu de données, complétez le script `import.js` (cherchez le `TODO` dans le code :wink:).

Exécutez-le ensuite :

```bash
npm install
node import.js
```

Vérifiez que les données ont été importées correctement grâce au shell (le nombre total de documents doit être `153194`) :

```
use 911-calls
db.calls.count()
```

## Index géographique et index textuel

Afin de répondre aux différents problèmes, vous allez avoir besoin de créer deux index particuliers sur la collection des appels :

* Un index géographique de type `2dsphere` pour les coordonnées GPS des appels.
  * https://docs.mongodb.com/manual/core/2dsphere/#create-a-2dsphere-index
* Un index textuel sur le titre des appels pour pouvoir faire des recherches full-text sur ce champ (recherche des overdoses par exemple)
  * https://docs.mongodb.com/manual/core/index-text/#create-text-index

## Requêtes

À vous de jouer ! Écrivez les requêtes MongoDB permettant de résoudre les problèmes posés.

```
db.calls.createIndex( { loc : "2dsphere" } ) //index géographique

// l'index textuel n'est pas utile pour la recherche des overdoses car l'organisation des données fait en sorte que le champ contienne directement "OVERDOSE"


// nombre d'appel par catégorie
db.calls.aggregate([
  { $group: { _id: "$title.type", count : { $sum: 1 } } }
])

// les 3 mois ayant comptabilisés le plus d'appels
db.calls.aggregate([
  { $group: {
     _id: {
        month: { $month: "$timeStamp" },
        year: { $year: "$timeStamp" }
      },
      count: { $sum: 1 }
    } },
  { $sort: { count: -1 } },
  { $limit: 3 }
])

// top 3 des villes avec le plus d'appels pour overdose
db.calls.aggregate([  
  { $match: { "title.detail" : "OVERDOSE" } },
  { $group: { _id: "$twp", count : { $sum: 1 } } },
  { $sort: { count: -1 } },
  { $limit: 3 }
])

// nombre d'appels autour de Lansdale dans un rayon de 500 mètres
db.calls.find(
  { loc :
    { $near :
      { $geometry :
        { type : "Point" , coordinates : [ -75.283783 , 40.241493 ] },
        $maxDistance : 500
      }
    }
  }
).count()


```

Vous allez sûrement avoir besoin de vous inspirer des points suivants de la documentation :

* Proximity search : https://docs.mongodb.com/manual/tutorial/query-a-2dsphere-index/#proximity-to-a-geojson-point
* Text search : https://docs.mongodb.com/manual/text-search/#text-operator
* Aggregation Pipeline : https://docs.mongodb.com/manual/core/aggregation-pipeline/
* Aggregation Operators : https://docs.mongodb.com/manual/reference/operator/aggregation-pipeline/
