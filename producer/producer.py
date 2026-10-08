import json
import os
import random
import signal
import sys
import time
from confluent_kafka import Producer

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:29092")
TOPIC = os.getenv("VITALS_TOPIC", "mimic-vitals")
NB_PATIENTS = int(os.getenv("NB_PATIENTS", "12"))
INTERVAL_S = float(os.getenv("INTERVAL_S", "1.0"))

PLAGES = {
    "HR": (55.0, 115.0, "bpm"),
    "SPO2": (90.0, 100.0, "%"),
    "ABP_SYS": (95.0, 145.0, "mmHg")
}

running = True

def gerer_signal(signum, frame):
    global running
    running = False

signal.signal(signal.SIGINT, gerer_signal)
signal.signal(signal.SIGTERM, gerer_signal)

def accuse_reception(err, msg):
    if err is not None:
        sys.stderr.write(f"Échec de livraison: {err}\n")

def creer_producteur():
    conf = {
        "bootstrap.servers": BOOTSTRAP,
        "client.id": "vitals-simulator-s1",
        "acks": "all",
        "linger.ms": 10,
        "retries": 5,
        "enable.idempotence": True,
        "partitioner": "consistent_random"
    }
    return Producer(conf)

def generer_releve(patient_id):
    vital_type = random.choice(list(PLAGES.keys()))
    v_min, v_max, unit = PLAGES[vital_type]
    value = round(random.uniform(v_min, v_max), 1)
    
    return {
        "patient_id": patient_id,
        "event_time": int(time.time() * 1000),
        "vital_type": vital_type,
        "value": value,
        "unit": unit
    }

def main():
    producteur = creer_producteur()
    patients = [f"P{i:03d}" for i in range(1, NB_PATIENTS + 1)]
    
    messages_envoyes = 0
    t_debut = time.time()
    
    print(f"Démarrage de la production vers {TOPIC} ({BOOTSTRAP})...")
    
    while running:
        for patient_id in patients:
            releve = generer_releve(patient_id)
            try:
                producteur.produce(
                    topic=TOPIC,
                    key=patient_id.encode("utf-8"),
                    value=json.dumps(releve).encode("utf-8"),
                    on_delivery=accuse_reception
                )
                messages_envoyes += 1
            except Exception as e:
                sys.stderr.write(f"Erreur d'envoi: {e}\n")
            
            producteur.poll(0)
        
        time.sleep(INTERVAL_S)

    print("\nArrêt demandé. Vidage du buffer...")
    producteur.flush(10)
    
    duree = time.time() - t_debut
    debit = messages_envoyes / duree if duree > 0 else 0
    print(f"Bilan: {messages_envoyes} msgs envoyés en {duree:.1f}s ({debit:.2f} msg/s)")
    return 0

if __name__ == "__main__":
    sys.exit(main())
