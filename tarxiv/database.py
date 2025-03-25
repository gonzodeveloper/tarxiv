# Database utilities
import datetime

from .utils import read_config, get_logger
from couchbase.cluster import Cluster
from couchbase.options import ClusterOptions
from couchbase.auth import PasswordAuthenticator


class TarxivDB:

    def __init__(self, config_dir):
        # General parameters
        config = read_config(config_dir + "config.yml")
        self.logger = get_logger("tarxiv_db", config["log_level"], "tarxiv_db")

        # Connect to Couchbase
        self.logger.info("connecting to couchbase")
        connection_str = 'couchbase://' + config["database"]["host"]
        options = ClusterOptions(PasswordAuthenticator(config["database"]["user"], config["database"]["pass"]))
        self.cluster = Cluster(connection_str, options)
        self.conn = self.cluster.bucket('tarxiv')
        self.logger.info("connected")

    def upsert(self, doc_id, doc_payload, collection):
        coll = self.conn.collection(collection)
        doc_id = str(doc_id)
        coll.upsert(doc_id, doc_payload)
        self.logger.debug({"action": "upserted", "doc_id": doc_id})

    def close(self):
        self.cluster.close()

