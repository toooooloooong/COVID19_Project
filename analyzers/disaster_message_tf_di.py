from os import rename
from glob import glob
from re import compile
from utils import print_time
from pandas import read_csv, DataFrame, to_datetime


@print_time
def disaster_message_tf_di():
    input_path = './data/preprocessed\\disaster_messages_preprocessed_'
    output_path = './data/analyzed\\disaster_messages_tf_di_'
    input_file_list = glob(f'{input_path}*')
    output_file_list = glob(f'{output_path}*')

    if not input_file_list:
        return

    p = compile(r'(\d{14})_(stopwords_ver\d+)_(userdic_ver\d+)')
    search1 = p.search(input_file_list[0])
    t = search1.group(1)
    stopwords_ver = search1.group(2)
    userdic_ver = search1.group(3)
    new_output_path = f'{output_path}{t}_{stopwords_ver}_{userdic_ver}.csv'

    if output_file_list:
        search2 = p.search(output_file_list[0])
        if t != search2.group(1):
            rename(output_file_list[0], new_output_path)
        elif stopwords_ver != search2.group(2):
            rename(output_file_list[0], new_output_path)
        elif userdic_ver == search2.group(3):
            rename(output_file_list[0], new_output_path)
        else:
            return

    preprocessed = read_csv(input_file_list[0])
    preprocessed['time'] = to_datetime(preprocessed['time'])
    tokens = set(w for doc in preprocessed['tokens'] for w in doc.split('┃'))
    tf_di = DataFrame(index=tokens)

    for year, data in preprocessed.groupby(preprocessed.time.dt.year):
        total = []
        for token in tokens:
            count = data['tokens'].apply(lambda x: x.split('┃').count(token))
            total.append(count.sum())
        tf_di[year] = total/sum(total)

    n = len(tf_di.columns)
    tf_di['tf_di'] = 0

    for i, (name, col) in enumerate(tf_di.iteritems()):
        if name == 'tf_di':
            break

        tf_di['tf_di'] += col*(i+1)/n

    tf_di.sort_values(by='tf_di', ascending=False).to_csv(new_output_path)
