from glob import glob
from re import compile
from konlpy.tag import Okt
from utils import print_time
from datetime import datetime
from os import getcwd, chdir, rename
from pandas import read_csv, to_datetime


@print_time
def disaster_message_preprocessor():
    is_append = False
    input_path = './data/raw\\disaster_messages_'
    stopwords_path = './preprocessors\\stopwords_'
    output_path = './data/preprocessed\\disaster_messages_preprocessed_'

    if getcwd()[-16:] != 'COVID-19 Project':
        chdir('..')

    input_file_list = glob(f'{input_path}*')
    output_file_list = glob(f'{output_path}*')
    stopwords_file_list = glob(f'{stopwords_path}*')

    if not input_file_list:
        return

    p1 = compile(r'\d{14}')
    p2 = compile(r'stopwords_ver\d+')
    t1 = p1.search(input_file_list[0]).group()
    stopwords_ver = p2.search(stopwords_file_list[0]).group()
    new_output_path = f'{output_path}{t1}_{stopwords_ver}.csv'

    if output_file_list:
        t2 = p1.search(output_file_list[0]).group()
        if p2.search(output_file_list[0]).group() != stopwords_ver:
            rename(output_file_list[0], new_output_path)
        elif t1 != t2:
            is_append = True
            rename(output_file_list[0], new_output_path)
        else:
            return

    raw = read_csv(input_file_list[0])
    stopwords = read_csv(stopwords_file_list[0], squeeze=True)
    okt = Okt()

    def tokenizer(df):
        tokens = okt.morphs(df['body'])
        filtered = filter(lambda x: not stopwords.isin([x]).any(), tokens)
        return ' '.join(filtered)

    if is_append:
        start = datetime.strptime(t2, '%Y%m%d%H%M%S')
        raw = raw[to_datetime(raw['time']) > start]

    raw['tokens'] = raw.apply(tokenizer, axis=1)

    if is_append:
        raw[['time', 'tokens']].to_csv(
            new_output_path, mode='a', header=False, index=False
            )
    else:
        raw[['time', 'tokens']].to_csv(new_output_path, index=False)


if __name__ == '__main__':
    disaster_message_preprocessor()
