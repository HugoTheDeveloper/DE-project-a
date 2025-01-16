import pandas as pd
import numpy as np
import pprint
from faker import Faker
import random

fake = Faker('ru_RU')

client = pd.DataFrame(data={
    'id': fake.uuid4(),
    'first_name': fake.first_name_male(),
    'last_name': fake.last_name_male(),
    'patronymic': fake.middle_name_male(),
    'birth_date': fake.date_of_birth(minimum_age=18, maximum_age=100),
}, index=[1])

for i in range(1, 3):
    client.loc[random.randint(1, 3)] = [fake.uuid4(), fake.first_name_male(), fake.last_name_male(),
                                         fake.middle_name_male(),
                                         fake.date_of_birth(minimum_age=18, maximum_age=100)]
    client.loc[random.randint(1, 3)] = [fake.uuid4(), fake.first_name_female(), fake.last_name_female(),
                                         fake.middle_name_female(),
                                         fake.date_of_birth(minimum_age=18, maximum_age=100)]

client.sort_index(axis=1)

client.to_csv('client.csv', sep=';', encoding='utf_8', index=False)
print(pd.read_csv("client.csv", sep=';'))