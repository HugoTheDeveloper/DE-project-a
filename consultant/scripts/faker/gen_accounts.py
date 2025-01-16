import pandas as pd
import faker
import random

fake = faker.Faker()
account = pd.DataFrame(columns=['id', 'open_date', 'close_date'])
for i in range(100):
    r = random.randint(0, 1)
    if r == 0:
        account.loc[i+1] = pd.Series(
            data={'id':fake.uuid4(),
                  'open_date': fake.date_between(start_date='-10y', end_date='-5y').strftime('%Y-%m-%d'),
                  'close_date': fake.date_between(start_date='-5y', end_date='-1d').strftime('%Y-%m-%d')})
    else:
        account.loc[i+1] = pd.Series(
            data={'id':fake.uuid4(),
                  'open_date': fake.date_between(start_date='-5y', end_date='-1d').strftime('%Y-%m-%d'),
                  'close_date': ''})
    account.to_csv('account.csv', sep=';', encoding='utf_8')