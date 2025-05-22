# Database utilities
from .utils import TarxivModule
from couchbase.cluster import Cluster
from couchbase.options import ClusterOptions
from couchbase.auth import PasswordAuthenticator
import json
import os

class TarxivDB(TarxivModule):
    """Interface for TarXiv couchbase data."""

    def __init__(self, *args, **kwargs):
        """
        Read in object schema and connect to couchbase.
        """
        super().__init__("tarxiv-couchbase", *args, **kwargs)
        self.schema_file = os.path.join(self.config_dir, "schema.json")
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
        """
        Read object schema from config directory and return it.
        :return: object metadata schema; dict
        """
        with open(self.schema_file) as f:
            return json.load(f)

    def upsert(self, object_name, payload, collection):
        """
        Insert document into couchbase collection. Update if already exists.
        :param object_name: name of the object to be used as a document id; str
        :param payload: document to upsert, either metadata or lightcurve; dict or list of dicts
        :param collection: couchbase collection; meta or lightcurve; str
        :return: void
        """
        coll = self.conn.collection(collection)
        coll.upsert(object_name, payload)
        self.logger.debug({"action": "upserted", "object_name": object_name, "collection": collection})

    def get(self, object_name, collection):
        """
        Retrieve a document from couchbase collection based on object_id
        :param object_name: name of the object to be used as a document id; str
        :param collection: couchbase collection; meta or lightcurve; str
        :return: object document, either metadata or lightcurve; dict or list of dicts
        """
        coll = self.conn.collection(collection)
        result = coll.get(object_name).value
        self.logger.debug({"action": "retrieved", "object_name": object_name, "collection": collection})
        return result

    def close(self):
        """
        Close connection to couchbase
        :return:
        """
        self.cluster.close()
