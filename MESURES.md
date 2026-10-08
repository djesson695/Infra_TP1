# Relevés et Mesures — TP1 ICU Monitor

## Partie 1 — Docker
- **Question 1.1 :** Le second conteneur ne voit pas le fichier car chaque conteneur possède sa propre couche inscriptible isolée au-dessus de l'image de base.
- **Question 1.2 :** Un `COPY ./` placé avant `RUN pip install` réexécuterait l'installation des dépendances à chaque modification d'une seule ligne de code Python, ce qui invaliderait le cache et ralentirait le build.

## Partie 2 — Docker Compose
- **Question 2.1 :** Un volume nommé est géré directement par Docker (meilleures performances de lecture/écriture sous macOS) et garantit la portabilité de l'environnement sans dépendre de l'arborescence du poste hôte.

## Partie 3 — Kafka KRaft
- **Question 3.1 :** Le facteur de réplication ne peut pas valoir 2 car il n'y a qu'un seul broker Kafka en local ; créer 2 répliques nécessiterait au moins 2 nœuds distincts.
- **Question 3.2 :** L'ordre strict des messages n'est garanti qu'à l'intérieur d'une même partition. Les messages d'un même patient (identifiés par leur clé) sont envoyés sur la même partition pour préserver leur ordre chronologique.

## Partie 5 — Mesures et Nettoyage
- **Question 5.1 :** L'écart de débit vient du temps d'exécution de la boucle Python (`time.sleep`) et du surcoût de sérialisation JSON.
- **Question 5.2 :** Pendant la coupure du broker, le producteur conserve les messages dans son buffer en mémoire et réessaie d'émettre (`retries`). Si la coupure dépasse la durée limite de rétention du buffer, des messages finissent par être rejetés.
- **Question 6.1 :** `docker compose down` supprime les conteneurs et les réseaux, mais conserve les données stockées dans les volumes nommés. `docker compose down -v` supprime également les volumes nommés, réinitialisant complètement l'état du broker au prochain démarrage.
