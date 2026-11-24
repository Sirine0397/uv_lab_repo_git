# Lab Spark / Iceberg : réponses

## Environnement
**Que devient un job si le service redémarre en cours de lab ?**
Le job meurt. En mode `local[*]`, driver et executors sont dans une seule JVM sur le pod. Même sans redémarrage, les credentials STS expirent : la session garde les clés fixées au démarrage et S3 répond 403 (token expiré).

**Pourquoi `SimpleAWSCredentialsProvider` échoue ?**
Il ne lit que l'access key et la secret key. Des credentials STS exigent aussi le session token, sinon la signature est rejetée. `TemporaryAWSCredentialsProvider` envoie les trois.

## Silver
**Sortie de la requête par état.**
`AE` (3 users) et `AP` (2) ont `missing_city` égal à leur total : ce sont des adresses militaires (APO/DPO) sans ville, donc `city` vaut NULL. `FM` et `VI` sont des territoires, pas des États.

**DataFrame ou SQL ?**
Les deux donnent le même plan Catalyst. Pour une exploration ponctuelle, je choisis SQL, plus rapide à écrire. Pour un modèle partagé, je choisis l'API DataFrame dans des fonctions : testable, composable, refactorable. SQL reste préférable si l'équipe ne connaît que SQL.

**Où `regexp_extract` renvoie-t-il `""` ailleurs ?**
Sur `state` et `zip_code`, qui valent `""` sans correspondance. Le `groupBy("state")` les regrouperait comme une valeur, alors qu'un NULL serait ignoré.

## Parquet
**Combien de part-files et pourquoi ?**
Un par table. Les données sont petites : le CSV est lu en une partition, et après le shuffle de la fenêtre, AQE fusionne les partitions. Il y a un fichier par partition de sortie.

**Que perd-on avec `coalesce(1)` ?**
Le parallélisme d'écriture : une seule tâche écrit tout, d'où goulot d'étranglement, risque mémoire et temps qui augmente avec la taille. Ça ne passe pas à l'échelle.

## Iceberg
**Quels fichiers contiennent les données, lesquels décrivent la table ?**
Dans `warehouse/silver/orders/`, `data/` contient les Parquet. `metadata/` décrit la table : `v1.metadata.json` (schéma, snapshots), le manifest `-m0.avro`, le fichier snapshot `snap-*.avro` et `version-hint.text`. Le dossier Parquet, lui, n'a que les données et `_SUCCESS`.

**Écrasement pendant une lecture.**
Avec Parquet, le lecteur peut voir des fichiers manquants ou un mélange d'anciens et de nouveaux, ou échouer. Avec Iceberg, l'isolation par snapshot fait que le lecteur voit sa version jusqu'à la fin et que le commit est atomique.

**Deux commits simultanés, rôle d'un catalogue.**
Le pointeur est un fichier sur S3 sans compare-and-swap atomique. Deux jobs peuvent écrire la même version et l'un écrase l'autre, ce qui perd un commit. Un catalogue (REST, JDBC, Glue) fait un swap atomique, gère la concurrence et refuse ou relance le perdant.

## Comparaison avec dbt
**Différences de schéma.**
Spark n'a pas de type UUID : j'ai casté en `string`. Les consommateurs doivent donc re-caster pour joindre avec des UUID et on perd la validation de format. (À confirmer avec `DESCRIBE`, notamment pour `ordered_at`.)

**Layout.**
Un objet unique est plus simple à lire. Un dossier de part-files est un peu moins pratique, mais c'est lui qui passe à l'échelle.

**Validation.**
J'ai utilisé des `assert` sur les comptages. Pour égaler dbt, il faudrait écrire des vérifications d'unicité, de non-nullité, de valeurs acceptées et de clés étrangères, à la main ou avec Great Expectations, Soda ou Deequ.

**Graphe de dépendances.**
Dans le notebook, c'est seulement l'ordre des cellules. Un orchestrateur comme Airflow ou Dagster jouerait ce rôle.

**Temps de build.**
Non comparables : démarrage de la JVM et résolution des packages Ivy pour Spark, `dbt build` qui inclut les tests, DuckDB en process, et des tailles de données minuscules.

**Quel outil choisir ?**
10 Go par jour : dbt avec DuckDB. 10 To par jour : Spark. Équipe SQL uniquement : dbt.
