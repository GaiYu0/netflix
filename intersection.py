import numpy as np
from datetime import date

data = sqlCtx.read.csv('data.csv', 'mid INT, uid INT, r INT, t STRING')
qualifying = sqlCtx.read.csv('qualifying.csv', 'mid INT, uid INT, t STRING')
days = lambda x: [x[0], (date(*map(int, x[1].split('-'))) - date(1, 1, 1)).days]
key_value = lambda x: [x['uid'], x['t']]
array = lambda x: np.array(x.collect())[:, 1]
earliest = array(qualifying.rdd.map(key_value).map(days).reduceByKey(min).sortByKey())
reduced_data = data.join(qualifying.select('uid').dropDuplicates(), on='uid', how='inner')
latest = array(reduced_data.rdd.map(key_value).map(days).reduceByKey(max).sortByKey())
