# Make sure to run 'pip install fink-tns' before executing

from fink_tns.utils import download_catalog

with open("tns_marker.txt") as f:
    tns_marker = f.read().replace("\n", "")

pdf_tns = download_catalog(os.environ["TNS_API_KEY"], tns_marker)

pdf_tns.to_csv("bulk.csv", index=False, sep='\t')
