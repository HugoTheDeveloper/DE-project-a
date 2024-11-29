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