from re import compile
from datetime import datetime
from konlpy.tag import Komoran
from utils import print_time, File_manager
from pandas import read_csv, to_datetime, Series


@print_time
def disaster_message_preprocessor():
    mode = 'w'
    input = File_manager('raw', 'disasterMessage')
    userdic = File_manager('ref', 'userdic', format='txt')
    stopwords = File_manager('ref', 'stopwords')
    output = File_manager('preprocessed', 'disasterMessage')
    new_ver = input.ver.copy()

    if new_ver['disasterMessage'] == '0':
        return

    t = output.ver['disasterMessage']
    new_ver.update(userdic.ver)
    new_ver.update(stopwords.ver)
    compare = output.compare_version(new_ver)
    n = len(compare)

    if n:
        output.update_version(new_ver)
        if n == 1 and compare[0] == 'disasterMessage' and t != '0':
            mode = 'a'
    else:
        return

    raw = read_csv(input.path)
    stopwords_data = read_csv(stopwords.path, squeeze=True)
    komoran = Komoran(userdic=userdic.path)

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
        filtered = filter(lambda x: not stopwords_data.isin([x]).any(), tokens)
        return '┃'.join(filtered)

    if mode == 'a':
        start = datetime.strptime(t, '%Y%m%d%H%M%S')
        raw = raw[to_datetime(raw['time']) > start]

    df = raw.apply(get_sender, axis=1).dropna()
    df = df.apply(lambda x: Series(x))
    df.columns = ['time', 'sender', 'message']
    df['tokens'] = df.apply(tokenizer, axis=1)
    df[['time', 'sender', 'tokens']].to_csv(
        output.path, mode=mode, index=False,
        header=False if mode == 'a' else True
        )
