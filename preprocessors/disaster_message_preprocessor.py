import parmap
from re import compile
from datetime import datetime
from numpy import array_split
from konlpy.tag import Komoran
from multiprocessing import Pool
from utils import print_time, File_manager
from pandas import read_csv, to_datetime, Series, concat

komoran = None


def initializer():
    global komoran
    userdic = File_manager('ref', 'userdic', format='txt')
    komoran = Komoran(userdic=userdic.path)


def get_sender(df):
    p = compile(r'[\[(〔［\^【]+(.+?)[\])〕］\^】]+(.+)')
    match = p.match(df['body'])

    if '모의훈련' in df['body']:
        return None
    elif match is None:
        return df['time'], None, df['body']
    else:
        return df['time'], match.group(1), match.group(2)


def tokenizer(df):
    stopwords = File_manager('ref', 'stopwords')
    stopwords_data = read_csv(stopwords.path, squeeze=True)
    tokens = komoran.morphs(df['message'])
    filtered = filter(lambda x: not stopwords_data.isin([x]).any(), tokens)
    return '┃'.join(filtered)


def tsk(df):
    working_df = df.apply(get_sender, axis=1).dropna()
    working_df = working_df.apply(lambda x: Series(x))
    working_df.columns = ['time', 'sender', 'message']
    working_df['tokens'] = working_df.apply(tokenizer, axis=1)
    return working_df[['time', 'sender', 'tokens']]


@print_time
def disaster_message_preprocessor(max_workers):
    mode = 'w'
    input = File_manager('raw', 'disasterMessage')
    userdic = File_manager('ref', 'userdic', format='txt')
    stopwords = File_manager('ref', 'stopwords')
    output = File_manager('preprocessed', 'disasterMessage')
    new_ver = input.ver.copy()

    if new_ver['disasterMessage'] == '0':
        return

    raw = read_csv(input.path)
    t = output.ver['disasterMessage']
    new_ver.update(userdic.ver)
    new_ver.update(stopwords.ver)
    compare = output.compare_version(new_ver)
    header = True
    n = len(compare)

    if n:
        output.update_version(new_ver)
        if n == 1 and compare[0] == 'disasterMessage' and t != '0':
            mode = 'a'
            header = False
            raw = raw.iloc[t:]
    else:
        return

    df_split = array_split(raw, max_workers)
    df_list = parmap.map(
        tsk, df_split, pm_pbar=True,
        pm_pool=Pool(max_workers, initializer=initializer)
        )
    concat(df_list).to_csv(output.path, mode=mode, index=False, header=header)
