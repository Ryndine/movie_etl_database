import re
import pandas as pd
import numpy as np

import re

from sqlalchemy import create_engine
import psycopg2

from config import db_password
import time

def clean_movie(movie_dict, lang_list, column_dict):
    movie = dict(movie_dict) # non-destructive copy
    alt_titles = {}
    # alternate titles
    for key in lang_list:
        if key in movie:
            alt_titles[key] = movie[key]
            movie.pop(key)
    if len(alt_titles) > 0:
        movie['alt_titles'] = alt_titles

    for old_name, new_name in column_dict.items():
        if old_name in movie:
            movie[new_name] = movie.pop(old_name)
   
    return movie

def format_money(df):
    def parse_dollar(row_str):
        if type(row_str) != str:
            return np.nan
        # Look into capture groups...
        if re.match(r'\$\s*\d+\.?\d*\s*milli?on', row_str, flags=re.IGNORECASE):
            row_str = re.sub('\$|\s|[a-zA-Z]','', row_str)

            value = float(row_str) * 10**6
            return value

        elif re.match(r'\$\s*\d+\.?\d*\s*billi?on', row_str, flags=re.IGNORECASE):
            row_str = re.sub('\$|\s|[a-zA-Z]','', row_str)

            value = float(row_str) * 10**9
            return value

        elif re.match(r'\$\s*\d{1,3}(?:[,\.]\d{3})+(?!\s[mb]illion)', row_str, flags=re.IGNORECASE):
            row_str = re.sub('\$|,','', row_str)

            value = float(row_str)
            return value

        else:
            return np.nan

    form_one = r'\$\d+\.?\d*\s*[mb]illion'
    form_two = r'\$\d{1,3}(?:,\d{3})+'

    box_office = df['Box office'].dropna()
    box_office = box_office.apply(lambda x: ' '.join(x) if type(x) is list else x)

    df['box_office'] = box_office.str.extract(f'({form_one}|{form_two})', flags=re.IGNORECASE)[0].apply(parse_dollar)
    df.drop('Box office', axis=1, inplace=True)

    budget = df['Budget'].dropna()
    budget = budget.map(lambda x: ' '.join(x) if type(x) is list else x)
    budget = budget.str.replace(r'\$.*[-—–](?![a-z])', '$', regex=True)
    df['budget'] = budget.str.extract(f'({form_one}|{form_two})', flags=re.IGNORECASE)[0].apply(parse_dollar)

def format_date(df):
    release_date = df['Release date'].dropna().apply(lambda x: ' '.join(x) if type(x) is list else x)
    date_form_one = r'(?:January|February|March|April|May|June|July|August|September|October|November|December)\s[123]\d,\s\d{4}'
    date_form_two = r'\d{4}.[01]\d.[123]\d'
    date_form_three = r'(?:January|February|March|April|May|June|July|August|September|October|November|December)\s\d{4}'
    date_form_four = r'\d{4}'
    df['release_date'] = pd.to_datetime(release_date.str.extract(f'({date_form_one}|{date_form_two}|{date_form_three}|{date_form_four})')[0], infer_datetime_format=True)

def format_runtime(df):
    run_time = df['Running time'].dropna().apply(lambda x: ' '.join(x) if type(x) is list else x)
    run_time_extract = run_time.str.extract(r'(\d+)\s*ho?u?r?s?\s*(\d*)|(\d+)\s*m')
    run_time_extract = run_time_extract.apply(lambda col: pd.to_numeric(col, errors='coerce')).fillna(0)
    df['run_time'] = run_time_extract.apply(lambda row: row[0]*60 + row[1] if row[2] == 0 else row[2], axis=1)
    df.drop('Running time', axis=1, inplace=True)

def cleanup_kaggle(df):
    df = df[df['adult'] == 'False'].drop('adult',axis='columns')
    df['video'] = df['video'] == 'True'
    df['budget'] = df['budget'].astype(int)
    df['id'] = pd.to_numeric(df['id'], errors='raise')
    df['popularity'] = pd.to_numeric(df['popularity'], errors='raise')
    df['release_date'] = pd.to_datetime(df['release_date'])
    
def fill_missing_kaggle_data(df, kag_wiki_dict):
    for kaggle_col, wiki_col in kag_wiki_dict.items():
        df[kaggle_col] = df.apply(
            lambda row: row[wiki_col] if row[kaggle_col] == 0 else row[kaggle_col]
            , axis=1)
        df.drop(columns=wiki_col, inplace=True)

def merge_movie_rating(movie_df, ratings_df):
    movie_df = movie_df[['imdb_id','id','title_kaggle','original_title','tagline','belongs_to_collection','url','imdb_link',
                'runtime','budget_kaggle','revenue','release_date_kaggle','popularity','vote_average','vote_count',
                'genres','original_language','overview','spoken_languages','Country',
                'production_companies','production_countries','Distributor',
                'Producer(s)','Director','Starring','Cinematography','Editor(s)','Writer(s)','Composer(s)','Based on'
                ]]

    movie_df.rename({'id':'kaggle_id','title_kaggle':'title','url':'wikipedia_url',
                    'budget_kaggle':'budget','release_date_kaggle':'release_date',
                    'Country':'country','Distributor':'distributor','Producer(s)':'producers',
                    'Director':'director','Starring':'starring','Cinematography':'cinematography',
                    'Editor(s)':'editors','Writer(s)':'writers','Composer(s)':'composers',
                    'Based on':'based_on'}, axis='columns', inplace=True)
 
    movie_df['kaggle_id'] = pd.to_numeric(movie_df['kaggle_id'], errors='raise')

    rating_counts = ratings_df.groupby(['movieId','rating'], as_index=False).count().rename({'userId':'count'}, axis=1).pivot(index='movieId',columns='rating', values='count')
    rating_counts.columns = ['rating_' + str(col) for col in rating_counts.columns]
    movies_with_ratings_df = pd.merge(movie_df, rating_counts, left_on='kaggle_id', right_index=True, how='left')
    movies_with_ratings_df[rating_counts.columns] = movies_with_ratings_df[rating_counts.columns].fillna(0)

def load_to_postgres(movie_df, ratings_raw):
    db_string = f"postgresql://postgres:{db_password}@127.0.0.1:5433/movie_data"
    engine = create_engine(db_string)
    movie_df.to_sql(name='movies', con=engine, if_exists='replace')

    rows_imported = 0
    start_time = time.time()

    for data in pd.read_csv(ratings_raw, chunksize=1000000):
        print(f'importing rows {rows_imported} to {rows_imported + len(data)}...', end='')
        data.to_sql(name='ratings', con=engine, if_exists='append')
        rows_imported += len(data)

        print(f'Done. {time.time() - start_time} total seconds elapsed')
