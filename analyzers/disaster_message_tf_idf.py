from utils import print_time, File_manager
from pandas import read_csv, DataFrame, Series
from sklearn.feature_extraction.text import TfidfVectorizer


@print_time
def disaster_message_tf_idf(max_workers):
    input = File_manager('preprocessed', 'disasterMessage')
    tf_idf = File_manager('analyzed', 'disasterMessageTFIDF')
    idf = File_manager('analyzed', 'disasterMessageIDF')
    cmpr = idf.compare_version(input.ver)
    new_ver = input.ver.copy()

    if new_ver['disasterMessage'] == '0':
        return
    if idf.ver[cmpr[0]] == new_ver['disasterMessage'] and len(cmpr) == 1:
        return

    new_ver['disasterMessageTFIDF'] = new_ver['disasterMessage']
    del new_ver['disasterMessage']
    tf_idf.update_version(new_ver)
    new_ver['disasterMessageIDF'] = new_ver['disasterMessageTFIDF']
    del new_ver['disasterMessageTFIDF']
    idf.update_version(new_ver)

    preprocessed_data = read_csv(input.path)
    docs = preprocessed_data['tokens']
    tfidfv = TfidfVectorizer(
        lowercase=False, token_pattern=r'(?u)[^┃]+?(?=┃|$)'
        ).fit(docs)
    vocabs = sorted(tfidfv.vocabulary_, key=tfidfv.vocabulary_.get)
    tf_idf_data = DataFrame(tfidfv.transform(docs).toarray(), columns=vocabs)
    idf_data = Series(tfidfv.idf_, index=vocabs).sort_values()

    tf_idf_data.to_csv(tf_idf.path, index=False)
    idf_data.to_csv(idf.path, header=False)
