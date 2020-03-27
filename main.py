import crawlers
import analyzers
import preprocessors
from utils import print_time, stabilize_path


@stabilize_path
@print_time
def main():
    for i in dir(crawlers):
        item = getattr(crawlers, i)
        if callable(item):
            item()

    for i in dir(preprocessors):
        item = getattr(preprocessors, i)
        if callable(item):
            item()

    for i in dir(analyzers):
        item = getattr(analyzers, i)
        if callable(item):
            item()


if __name__ == '__main__':
    main()
