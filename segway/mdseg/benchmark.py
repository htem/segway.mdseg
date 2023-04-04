import sys
import argparse
import daisy
import importlib
import os
import functools
import sys
from time import time

sys.path.insert(0, '/n/groups/htem/cb2/repos/segway.mdseg')
import segway.mdseg.database

neuron_db_path = '/n/groups/htem/cb2/cb2_project_analysis/database/neurons.db'
db = segway.mdseg.database.NeuronDBServerSQLite(neuron_db_path)

names = ['grc_1000']
t0 = time()
for i in range(5):
    for name in names:
        neuron = neuron_db.get_neuron(name, with_children=with_children)
print(time() - t0)


