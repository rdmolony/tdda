# accounts_verify_10k_against_1k.py

from __future__ import print_function
import pandas as pd

from tdda.constraints.pd.constraints import verify_df

df = pd.read_csv('testdata/accounts25k.csv')
print(verify_df(df, 'accounts1k.tdda'))
