from datetime import date
import sys

import numpy as np
import pyspark.sql.functions as F
from pyspark.sql.session import SparkSession
from pyspark.sql.types import *

csv = sys.argv[1]

ss = SparkSession.builder.getOrCreate()
df = ss.read.csv(csv, 'iid INT, uid INT, r FLOAT, t INT')

data_frame_to_array = lambda df: np.array(df.rdd.flatMap(lambda x: x).collect())

days = lambda x: [x[0], (date(*map(int, x[1].split('-'))) - date(1, 1, 1)).days]
