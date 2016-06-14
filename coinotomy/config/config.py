from coinotomy.backend.csvbackend import CsvStorageBackend

import coinotomy.watchers.btce
import coinotomy.watchers.bitfinex
import coinotomy.watchers.btcc

import os


# the storage class to use.
STORAGE_CLASS = CsvStorageBackend

# the directory where files should be stored
STORAGE_DIRECTORY = "../coinotomy_data"
# create the dir on module load it doesn't exist yet
if not os.path.exists(STORAGE_DIRECTORY): os.mkdir(STORAGE_DIRECTORY)

# the watchers to use.
WATCHERS = (
    coinotomy.watchers.btce.watchers +
    coinotomy.watchers.bitfinex.watchers +
    coinotomy.watchers.btcc.watchers
)




