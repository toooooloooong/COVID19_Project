from time import time


def print_time(f):
    def wrapper():
        print(f'Started {f.__name__}.')
        start = time()
        f()
        print(f'Finished {f.__name__} in {time() - start:.1f} seconds.')

    return wrapper
