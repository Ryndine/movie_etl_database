import pandas as pd
import re

df = pd.read_json('Resources/wikipedia_movies.json')

def fix_numbers(v):
    if isinstance(v, list) or not isinstance(v, str):
        return 'skipperoo'

    else:
        try:
            res = re.match(r'.*\$(.*\d)(?=\s([mb]illion)|$)', v)
            val, multi = float(res.groups()[0]), res.groups()[1]
            if multi is None:
                return val
            elif multi == 'million':
                return val * 10 ** 6
            elif multi == 'billion':
                return val * 10 ** 9
        except Exception as e:
            return 'skipperoo'

df['numberfied'] = df['Box office'].apply(fix_numbers)

print(df)