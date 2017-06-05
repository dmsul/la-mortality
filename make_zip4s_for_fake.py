"""
`fake_data.dta` has zip4's that are really zip's with "0000" on the end, so
merging on aermod fails. This creates a dummy dataset for testing that
collapses aermod to zip, then appends the extra zeros.
"""
import pandas as pd

if __name__ == '__main__':
    df = pd.read_stata("../data/zips_aermod_pre.dta")
    df['zip'] = df['zip4'].astype(str).str[:5]
    df2 = df.groupby('zip')['aermod_pre'].mean().reset_index()
    df2['zip4'] = df2['zip'] + "0000"
    df2 = df2.drop('zip', axis=1).set_index('zip4')
    df2.to_stata('../data/zip5s_fake_aermod_pre.dta')
