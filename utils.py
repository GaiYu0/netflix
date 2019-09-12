import numpy as np

def arange(a, b):
    d = b - a
    cs = np.cumsum(d)
    return np.repeat(a, d) + np.arange(cs[-1]) - np.repeat(cs - d, d)
