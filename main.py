from argparse import ArgumentParser
from utils import print_time, stabilize_path


@stabilize_path
@print_time
def main(crawl, preprocess, analyze, max_workers):
    m = int(max_workers)

    if crawl == 'True':
        import crawlers

        for i in dir(crawlers):
            item = getattr(crawlers, i)
            if callable(item):
                item(m)

    if preprocess == 'True':
        import preprocessors

        for i in dir(preprocessors):
            item = getattr(preprocessors, i)
            if callable(item):
                item(m)

    if analyze == 'True':
        import analyzers

        for i in dir(analyzers):
            item = getattr(analyzers, i)
            if callable(item):
                item(m)


if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument('-c', '--crawl', required=False, default='True')
    parser.add_argument('-p', '--preprocess', required=False, default='True')
    parser.add_argument('-a', '--analyze', required=False, default='True')
    parser.add_argument('-m', '--max_workers', required=False, default='6')
    args = parser.parse_args()

    main(args.crawl, args.preprocess, args.analyze, args.max_workers)
