from glob import glob
from re import compile
from utils import print_time
from os import getcwd, chdir, rename
from pandas import read_csv, DataFrame, Series
from sklearn.feature_extraction.text import TfidfVectorizer


@print_time
def disaster_message_tf_idf():
    input_path = './data/preprocessed\\disaster_messages_preprocessed_'
    output_path = './data/analyzed\\disaster_messages_tf_idf_'

    if getcwd()[-16:] != 'COVID-19 Project':
        chdir('..')

    input_file_list = glob(f'{input_path}*')
    output_file_list = glob(f'{output_path}*')

    if not input_file_list:
        return

    p = compile(r'(\d{14})_(stopwords_ver\d+)')
    search1 = p.search(input_file_list[0])
    t = search1.group(1)
    stopwords_ver = search1.group(2)
    new_output_path = f'{output_path}{t}_{stopwords_ver}.csv'

    if output_file_list:
        search2 = p.search(output_file_list[0])
        if t == search2.group(1) and stopwords_ver == search2.group(2):
            return
        else:
            rename(output_file_list[0], new_output_path)

    preprocessed = read_csv(input_file_list[0])
    docs = preprocessed['tokens']
    tfidfv = TfidfVectorizer().fit(docs)
    vocabs = sorted(tfidfv.vocabulary_, key=tfidfv.vocabulary_.get)
    tfidf = DataFrame(tfidfv.transform(docs).toarray(), columns=vocabs)
    idf = Series(tfidfv.idf_, index=vocabs).sort_values()

    tfidf.to_csv(new_output_path, index=False)
    idf.to_csv('./data/analyzed\\disaster_messages_idf.csv', header=False)


if __name__ == '__main__':
    disaster_message_tf_idf()
