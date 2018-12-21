import logging
import sys
from medusa.storage import Storage


def main(config, backup_name):
    storage = Storage(config=config.storage)
    backup = storage.get_cluster_backup(backup_name)
    if not backup:
        logging.error('No such backup')
        sys.exit(1)

    for hostname, ringitem in backup.tokenmap.items():
        print(hostname)
        print(ringitem['tokens'])
