from pyspark.sql.functions  import *
import phonenumbers as pn
from pyspark.sql.types import StringType


DEGREES = [
    'DDS',  # Doctor of Dental Surgery
    'PhD',  # Doctor of Philosophy
    'DVM',  # Doctor of Veterinary Medicine
    'MD',   # Doctor of Medicine
    'EdD',  # Doctor of Education
    'DPT',  # Doctor of Physical Therapy
    'JD',   # Juris Doctor
    'MS',   # Master of Science
    'MA',   # Master of Arts
    'MBA',  # Master of Business Administration
    'MFA',  # Master of Fine Arts
    'MEng',  # Master of Engineering
    'MPhil', # Master of Philosophy
    'DSc',  # Doctor of Science
    'DSW',  # Doctor of Social Work
    'ScD',  # Doctor of Science
    'DNP',  # Doctor of Nursing Practice
    'PharmD', # Doctor of Pharmacy
    'CRNA', # Certified Registered Nurse Anesthetist
    'MHA',  # Master of Health Administration
    'MEd',  # Master of Education
    'OTD',  # Doctor of Occupational Therapy
]

SHORT_TITLES = [
    'Mr',    # Mister
    'Mrs',   # Mistress (married woman)
    'Ms',    # Miss (woman, marital status unknown)
    'Dr',    # Doctor
    'Prof',   # Professor
    'Jr',    # Junior
    'Sr',    # Senior
    'Mx',    # Gender-neutral title
    'Rev',   # Reverend
    'Hon',   # Honorable
    'Capt',  # Captain
    'Lt',    # Lieutenant
    'Col',   # Colonel
    'Maj',   # Major
    'Sgt',   # Sergeant
    'Cpl',   # Corporal
    'Eng',   # Engineer
    'Dame',  # Title for women equivalent to knighthood
    'Sir',   # Title for men equivalent to knighthood
    
]

TITLES = [
    'Miss'
]

TITLES_REGEX = "|".join([f'{i}\.' for i in SHORT_TITLES])

TITLES_REGEX += f'|{"|".join([i for i in TITLES])}' # Add titles from TITLES with another logic


DEGREES_REGEX = "|".join(DEGREES)


def valid_phone_num(num):
    parsed = pn.parse(num, None)
    if pn.is_valid_number(parsed):
        standarted = pn.format_number(parsed, pn.PhoneNumberFormat.E164) 
        return f'{standarted}{'x' + parsed.extension if parsed.extension else ''}'
    return 'Incorrect phone number'


valid_phon_udf = udf(valid_phone_num, StringType())


def clean_clients(df):
    df \
    .withColumn('names_len', size(split(col('client_name'), r"\s+"))) \
    .filter(col('names_len') >= 3) \
    .withColumn(
        'client_name',
        regexp_replace(
            col('client_name'),
            rf"(^({TITLES_REGEX})\s*)|(\s+({DEGREES_REGEX})$)",
            "")
    ) \
    .withColumn('client_phone', valid_phon_udf(col('client_phone'))) \
    