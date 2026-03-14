"""
Central configuration loader.
All pipeline components import from here — never read env vars directly.
"""
import os
from dataclasses import dataclass
from dotenv import load_dotenv

load_dotenv()


@dataclass(frozen=True)
class KafkaConfig:
    bootstrap_servers: str
    raw_topic: str
    validated_topic: str
    dead_letter_topic: str
    consumer_group: str


@dataclass(frozen=True)
class MinIOConfig:
    endpoint: str
    access_key: str
    secret_key: str
    bucket: str


@dataclass(frozen=True)
class PostgresConfig:
    host: str
    port: int
    database: str
    user: str
    password: str


@dataclass(frozen=True)
class SimulatorConfig:
    events_per_second: float
    start_date: str
    fred_api_key: str


@dataclass(frozen=True)
class PipelineConfig:
    kafka: KafkaConfig
    minio: MinIOConfig
    postgres: PostgresConfig
    simulator: SimulatorConfig
    checkpoint_dir: str


def load_config() -> PipelineConfig:
    return PipelineConfig(
        kafka=KafkaConfig(
            bootstrap_servers=os.environ["KAFKA_BOOTSTRAP_SERVERS"],
            raw_topic=os.environ["KAFKA_RAW_TOPIC"],
            validated_topic=os.environ["KAFKA_VALIDATED_TOPIC"],
            dead_letter_topic=os.environ["KAFKA_DEAD_LETTER_TOPIC"],
            consumer_group=os.environ["KAFKA_CONSUMER_GROUP"],
        ),
        minio=MinIOConfig(
            endpoint=os.environ["MINIO_ENDPOINT"],
            access_key=os.environ["MINIO_ACCESS_KEY"],
            secret_key=os.environ["MINIO_SECRET_KEY"],
            bucket=os.environ["MINIO_BUCKET"],
        ),
        postgres=PostgresConfig(
            host=os.environ["POSTGRES_HOST"],
            port=int(os.environ["POSTGRES_PORT"]),
            database=os.environ["POSTGRES_DB"],
            user=os.environ["POSTGRES_USER"],
            password=os.environ["POSTGRES_PASSWORD"],
        ),
        simulator=SimulatorConfig(
            events_per_second=float(
                os.environ.get("SIMULATOR_EVENTS_PER_SECOND", "1")
            ),
            start_date=os.environ.get("SIMULATOR_START_DATE", "2020-01-01"),
            fred_api_key=os.environ["FRED_API_KEY"],
        ),
        checkpoint_dir=os.environ.get(
            "CHECKPOINT_DIR", "/tmp/mortgage-pipeline/checkpoints"
        ),
    )