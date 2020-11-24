import copy
import datetime
import pandas as pd

def detect_type(_dbstream, name, example):
    print('Define type of %s...' % name)
    try:
        query = "SELECT CAST('%s' as TIMESTAMP)" % example
        _dbstream.execute_query(query)
        return "TIMESTAMP"
    except:
        pass
    if isinstance(example, datetime.date):
        return "TIMESTAMP"
    elif isinstance(example, bool):
        return "BOOL"
    elif isinstance(example, int):
        return "INT64"
    elif isinstance(example, float):
        return "FLOAT64"
    else:
        return "STRING"

def convert_to_bool(x):
    if x.lower() == "true" or x == 1 or x.lower() == "t":
        return True
    if x.lower() == "false" or x == 0 or x.lower() == "f":
        return False
    else:
        raise Exception

def convert_to_int(x):
    if x[-2:] == ".0":
        return int(x.replace(".0",""))
    else:
        return int(x)

def len_or_max(s):
    if isinstance(s, str):
        return len(s)
    return s

def find_sample_value(df, name, i):
    df1 = df[name].dropna()
    try:
        df1 = df1.apply(lambda x: str(x))
    except:
        pass
    try:
        df1 = df1.apply(lambda x: convert_to_bool(x))
    except:
        try:
            df1 = df1.apply(lambda x: convert_to_int(x))
        except:
            try:
                df1 = df1.apply(lambda x: float(x))
            except:
                pass
    df1_copy = copy.deepcopy(df1)
    if df1.dtype == 'object':
        df1 = df1.apply(lambda x: (str(x.encode()) if isinstance(x, str) else x) if x is not None else '')
        if df1.empty:
            return None, None
        else:
            return df1_copy[df1.map(len_or_max) == df1.map(len_or_max).max()].iloc[0], df1_copy[df1.map(len_or_max) == df1.map(len_or_max).min()].iloc[0]
    elif df1.dtype == 'int64':
        max = int(df1.max())
        min = int(df1.min())
        return max, min
    elif df1.dtype == 'float64':
        max = float(df1.max())
        min = float(df1.min())
        return max, min
    else:
        rows = df.values.tolist()
        for row in rows:
            if row[i] is not None:
                return row[i], row[i]
        return None, None