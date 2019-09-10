from datetime import date
import sys

import numpy as np
import pyspark.sql.functions as F
from pyspark.sql.session import SparkSession
from pyspark.sql.types import *

csv = sys.argv[1]

ss = SparkSession.builder.getOrCreate()
df = ss.read.csv(csv, 'iid INT, uid INT, r FLOAT, date STRING')

data_frame_to_array = lambda df: np.array(df.rdd.flatMap(lambda x: x).collect())

days = lambda x: (date(*map(int, x.split('-'))) - date(1, 1, 1)).days
df = df.withColumn('t', F.udf(days, IntegerType())(df.date))
sorted_df = df.sort('uid', 't')

uid = data_frame_to_array(sorted_df.select('uid'))
iid = data_frame_to_array(sorted_df.select('iid'))
r = data_frame_to_array(sorted_df.select('r'))

np.save('uid', uid)
np.save('iid', iid)
np.save('r', r)
