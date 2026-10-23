"""Productor de Kafka para la ingesta de FIRMS.

El servicio de ingesta obtiene registros de FIRMS (mediante el adaptador FirmsClient)
y los publica en Kafka. Estrategia de idempotencia:

- El productor de Kafka opera con ``enable_idempotence=True`` para que los reintentos
  no generen mensajes duplicados en la sesión de producción.
- El procesador realiza un upsert basado en la clave natural
  ``(acq_date, latitude, longitude)``, de modo que volver a ejecutar la ingesta
  para la misma fecha/coordenada nunca duplica filas en PostgreSQL.
"""

from __future__ import annotations

import json
import logging
from typing import Iterable, Dict

from kafka import KafkaProducer

from firewatch.config import settings

logger = logging.getLogger(__name__)


def create_producer(bootstrap_servers: str | None = None) -> KafkaProducer:
    """Crea y devuelve un productor de Kafka configurado."""
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers or settings.KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        enable_idempotence=True,
        max_in_flight_requests_per_connection=5,
        acks="all",
    )


def publish_ingest(producer, record: Dict, topic: str | None = None) -> None:
    """
    Publica un registro de FIRMS en el topic de ingesta y espera la confirmación (ack).

    Funciona con el ``KafkaProducer`` real y con cualquier doble de prueba que implemente
    ``send(topic, value)`` retornando un objeto tipo future con ``.get(timeout=...)`` (usado en tests).
    """
    t = topic or settings.KAFKA_INGEST_TOPIC
    future = producer.send(t, record)
    metadata = future.get(timeout=10)
    logger.info(
        "Published FIRMS record topic=%s partition=%s offset=%s",
        getattr(metadata, "topic", t),
        getattr(metadata, "partition", "?"),
        getattr(metadata, "offset", "?"),
    )


def run_ingestion(
    records: Iterable[Dict],
    producer,
    topic: str | None = None,
) -> int:
    """
    Publica un lote de registros de FIRMS en Kafka.

    Devuelve el número de registros publicados. Es seguro volver a ejecutarlo con los
    mismos registros: el upsert en la base de datos hace que el pipeline sea idempotente.
    """
    count = 0
    for record in records:
        publish_ingest(producer, record, topic=topic)
        count += 1

    # Every record was already acked by publish_ingest, so this buffer is empty;
    # the call stays as a guard in case publishing ever becomes asynchronous.
    producer.flush()
    logger.info("Ingested %s FIRMS records into Kafka", count)
    return count
