from utils import print_time, File_manager
from pandas import read_csv, DataFrame, to_datetime


@print_time
def disaster_message_tf_di():
    input = File_manager('preprocessed', 'disasterMessage')
    output = File_manager('analyzed', 'disasterMessageTFDI')
    cmpr = output.compare_version(input.ver)
    new_ver = input.ver.copy()

    if new_ver['disasterMessage'] == '0':
        return
    if output.ver[cmpr[0]] == new_ver['disasterMessage'] and len(cmpr) == 1:
        return

    new_ver['disasterMessageTFDI'] = new_ver['disasterMessage']
    del new_ver['disasterMessage']
    output.update_version(new_ver)

    preprocessed = read_csv(input.path)
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

    tf_di.sort_values(by='tf_di', ascending=False).to_csv(output.path)
