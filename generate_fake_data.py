import pandas as pd
from random import shuffle, random
from faker import Faker
from datetime import datetime, timedelta

patients = []
N = 100
perc_discharged = 0.9
fakerObj = Faker('nl_NL')

for i in range(N):

    patients.append({
        'patient_id': i,
        'first_name': fakerObj.first_name(),
        'last_name': fakerObj.last_name(),
        'action': 'admission',
        'timestamp': datetime.now()
    })

    # FOr X patient of patients add discharge
    if random() < perc_discharged:

        patients.append({
            'patient_id': i,
            'first_name': fakerObj.first_name(),
            'last_name': fakerObj.last_name(),
            'action': 'discharge',
            'timestamp': datetime.now() + timedelta(hours=12)
        })

shuffle(patients)
pd.DataFrame.from_records(patients).to_csv('./data-lake/hospital_a_2020_05_16.csv', index=False)
