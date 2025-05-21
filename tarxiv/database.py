# Database utilities
from .utils import TarxivModule
from couchbase.cluster import Cluster
from couchbase.options import ClusterOptions
from couchbase.auth import PasswordAuthenticator
import json


class TarxivDB(TarxivModule):
    """Base class for tarxiv data"""

    def __init__(self, config_dir, debug=False):
        super().__init__("tarxiv-couchbase", config_dir, debug)
        self.schema_file = config_dir + "/schema.json"
        # Connect to Couchbase
        self.logger.info("connecting to couchbase")
        connection_str = "couchbase://" + self.config["database"]["host"]
        options = ClusterOptions(
            PasswordAuthenticator(
                self.config["database"]["user"], self.config["database"]["pass"]
            )
        )
        self.cluster = Cluster(connection_str, options)
        self.conn = self.cluster.bucket("tarxiv")
        self.logger.info("connected")

    def get_object_schema(self):
        with open(self.schema_file) as f:
            return json.load(f)

    def upsert(self, object_name, payload, collection):
        coll = self.conn.collection(collection)
        coll.upsert(object_name, payload)
        self.logger.debug({"action": "upserted", "object_name": object_name, "collection": collection})

    def get(self, object_name, collection):
        coll = self.conn.collection(collection)
        result = coll.get(object_name)
        self.logger.debug({"action": "retrieved", "object_name": object_name, "collection": collection})
        return result

    def close(self):
        self.cluster.close()
