from os import rename
from glob import glob
from re import compile
from utils import print_time
from datetime import datetime
from konlpy.tag import Komoran
from pandas import read_csv, to_datetime, Series


@print_time
def disaster_message_preprocessor():
    is_append = False
    input_path = './data/raw\\disaster_messages_'
    userdic_path = './preprocessors\\userdic_'
    stopwords_path = './preprocessors\\stopwords_'
    output_path = './data/preprocessed\\disaster_messages_preprocessed_'
    input_file_list = glob(f'{input_path}*')
    output_file_list = glob(f'{output_path}*')
    stopwords_file_list = glob(f'{stopwords_path}*')
    userdic_file_list = glob(f'{userdic_path}*')

    if not input_file_list:
        return

    p1 = compile(r'\d{14}')
    p2 = compile(r'stopwords_ver\d+')
    p3 = compile(r'userdic_ver\d+')
    t1 = p1.search(input_file_list[0]).group()
    stopwords_ver = p2.search(stopwords_file_list[0]).group()
    userdic_ver = p3.search(userdic_file_list[0]).group()
    new_output_path = f'{output_path}{t1}_{stopwords_ver}_{userdic_ver}.csv'

    if output_file_list:
        t2 = p1.search(output_file_list[0]).group()
        if p2.search(output_file_list[0]).group() != stopwords_ver:
            rename(output_file_list[0], new_output_path)
        elif p3.search(output_file_list[0]).group() != userdic_ver:
            rename(output_file_list[0], new_output_path)
        elif t1 != t2:
            is_append = True
            rename(output_file_list[0], new_output_path)
        else:
            return

    raw = read_csv(input_file_list[0])
    stopwords = read_csv(stopwords_file_list[0], squeeze=True)
    komoran = Komoran(userdic=userdic_file_list[0])

    def get_sender(df):
        p = compile(r'[\[(〔［\^【]+(.+?)[\])〕］\^】]+(.+)')
        match = p.match(df['body'])

        if match is None:
            return df['time'], None, df['body']
        elif '모의훈련' in match.group(1):
            return None
        else:
            return df['time'], match.group(1), match.group(2)

    def tokenizer(df):
        tokens = komoran.morphs(df['message'])
        filtered = filter(lambda x: not stopwords.isin([x]).any(), tokens)
        return '┃'.join(filtered)

    if is_append:
        start = datetime.strptime(t2, '%Y%m%d%H%M%S')
        raw = raw[to_datetime(raw['time']) > start]

    df = raw.apply(get_sender, axis=1).dropna()
    df = df.apply(lambda x: Series(x))
    df.columns = ['time', 'sender', 'message']
    df['tokens'] = df.apply(tokenizer, axis=1)

    if is_append:
        df[['time', 'sender', 'tokens']].to_csv(
            new_output_path, mode='a', header=False, index=False
            )
    else:
        df[['time', 'sender', 'tokens']].to_csv(new_output_path, index=False)
