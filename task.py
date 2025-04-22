import pandas as pd

df = pd.read_json('users.json')
# print(df)

# Normalize the nested 'usage' column
usage_df = pd.json_normalize(df['usage'])
df = pd.concat([df.drop(columns='usage'), usage_df], axis=1)
# print(df)

df['signup_date'] = pd.to_datetime(df['signup_date'])
df['last_login'] = pd.to_datetime(df['last_login'])
# print(df.dtypes)

from datetime import datetime

today = datetime.today()
df['account_age_days'] = (today - df['signup_date']).dt.days
print(df[['name', 'signup_date', 'account_age_days']])

filtered_df = df[(df['is_active'] == True) & (df['hours_used'] > 50)]
print(filtered_df)

filtered_df.to_parquet("active_users.parquet", index=False)
