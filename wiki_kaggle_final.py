import json
import pandas as pd

from etl_functions import *

def extract_transform_load(wiki_movies_raw, kaggle_metadata, ratings, ratings_raw):
    # Filter out movies we don't want
    wiki_movies = [movie for movie in wiki_movies_raw if ('Director' in movie or 'Directed by' in movie) and 'imdb_link' in movie and 'No. of episodes' not in movie]

    # Cleanup movie dict & make dataframe
    remove_languages = ['Also known as','Arabic','Cantonese','Chinese','French',
                        'Hangul','Hebrew','Hepburn','Japanese','Literally',
                        'Mandarin','McCune-Reischauer','Original title','Polish',
                        'Revised Romanization','Romanized','Russian','Simplified',
                        'Traditional','Yiddish']

    new_column_names = {'Adaptation by':'Writer(s)','Country of origin':'Country','Directed by':'Director',\
                    'Distributed by':'Distributor','Edited by':'Editor(s)','Length':'Running time',\
                    'Original release':'Release date','Music by':'Composer(s)','Produced by':'Producer(s)',\
                    'Producer':'Producer(s)','Productioncompanies ':'Production company(s)',\
                    'Productioncompany ':'Production company(s)','Released':'Release Date',\
                    'Screen story by':'Writer(s)','Screenplay by':'Writer(s)','Screenplay by':'Writer(s)',\
                    'Story by':'Writer(s)','Theme music composer':'Composer(s)','Written by':'Writer(s)'}

    clean_movie_list = [clean_movie(movie, remove_languages, new_column_names) for movie in wiki_movies]
    wiki_movies_df = pd.DataFrame(clean_movie_list)

    # Regex cleanup
    try:
        wiki_movies_df['imdb_id'] = wiki_movies_df['imdb_link'].str.extract(r'(tt\d{7})')
        wiki_movies_df.drop_duplicates(subset='imdb_id', inplace=True)
    except Exception as e:
        print(e)

    wiki_columns_to_keep = [column for column in wiki_movies_df.columns if wiki_movies_df[column].isnull().sum() < len(wiki_movies_df) * 0.9]
    wiki_movies_df = wiki_movies_df[wiki_columns_to_keep]

    # Reformat money
    format_money(wiki_movies_df)

    format_date(wiki_movies_df)

    format_runtime(wiki_movies_df)

    cleanup_kaggle(kaggle_metadata)

    movies_df = pd.merge(wiki_movies_df, kaggle_metadata, on='imdb_id', suffixes=['_wiki','_kaggle'])

    columns_to_drop = ['title_wiki','release_date_wiki','Language','Production company(s)']
    movies_df.drop(columns=columns_to_drop, inplace=True)

    kaggle_wiki_columns = {'runtime':'run_time','budget_kaggle':'budget_wiki','revenue':'box_office'}
    fill_missing_kaggle_data(movies_df, kaggle_wiki_columns)

    merge_movie_rating(movies_df, ratings)

    load_to_postgres(movies_df, ratings_raw)

file_dir = 'Resources'
wiki_file = f'{file_dir}/wikipedia_movies.json'
kaggle_file = f'{file_dir}/movies_metadata.csv'
ratings_file = f'{file_dir}/ratings.csv'

with open(wiki_file, mode='r') as file:
    wiki_movies_raw = json.load(file)

kaggle_metadata = pd.read_csv(kaggle_file)
ratings = pd.read_csv(ratings_file)

extract_transform_load(wiki_movies_raw, kaggle_metadata, ratings, ratings_file)