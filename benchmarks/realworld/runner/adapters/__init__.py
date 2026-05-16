from .mongodb import MongoAdapter
from .opensearch import OpenSearchAdapter
from .pgvector import PgVectorAdapter
from .postgres import PostgresAdapter
from .qdrant import QdrantAdapter
from .tracedb import TraceDbAdapter


def all_adapters():
    return [
        TraceDbAdapter(),
        PostgresAdapter(),
        PgVectorAdapter(),
        MongoAdapter(),
        QdrantAdapter(),
        OpenSearchAdapter(),
    ]
