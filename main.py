from argparse import ArgumentParser
from utils import print_time, stabilize_path


@stabilize_path
@print_time
def main(crawl, preprocess, analyze):
    if crawl == 'True':
        import crawlers

        for i in dir(crawlers):
            item = getattr(crawlers, i)
            if callable(item):
                item()

    if preprocess == 'True':
        import preprocessors

        for i in dir(preprocessors):
            item = getattr(preprocessors, i)
            if callable(item):
                item()

    if analyze == 'True':
        import analyzers

        for i in dir(analyzers):
            item = getattr(analyzers, i)
            if callable(item):
                item()


if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument('-c', '--crawl', required=False, default='True')
    parser.add_argument('-p', '--preprocess', required=False, default='True')
    parser.add_argument('-a', '--analyze', required=False, default='True')
    args = parser.parse_args()

    main(args.crawl, args.preprocess, args.analyze)
