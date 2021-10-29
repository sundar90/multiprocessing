import pandas as pd
import re
import time
import os
from multiprocessing import Pool
import numpy as np


def util_function(row):
    languages = ['French', 'German', 'English', 'Danish', 'Thai', 'Greek', 'Japanese', 'Swedish', 'Italian', 'Korean']
    for language in languages:
        if re.search(language,row):
            return 'Utility'
    return ''


def utility_calc(df):
    df['utility'] = df.apply(lambda x: util_function(str(x['language'])),axis=1)
    return df

def parallelize_dataframe(df,func,n_cores=8):
    df_split = np.array_split(df,n_cores)
    pool = Pool(n_cores)
    df = pd.concat(pool.map(func,df_split))
    pool.close()
    pool.join()
    return df

if  __name__ == "__main__":
    file = 'C:/Users/Sundar/Downloads/IMDb movies.csv/IMDb movies.csv'
    df = pd.read_csv(file)
    start_time = time.time()
    results = parallelize_dataframe(df,utility_calc)
    print('runtime with multiprocessing:',time.time() - start_time)