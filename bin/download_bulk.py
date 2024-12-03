from tarxiv.tns_alerts import TarxivTNS
import sys

# Get config dir from arguments
config_dir = sys.argv[1]

# Instance and run bulk
txv_tns = TarxivTNS(config_dir, "tns-bulk-download")
txv_tns.download_bulk_tns()