from time import time


def print_time(f):
    def wrapper(*args):
        print(f'Started {f.__name__}.')
        start = time()
        f(*args)
        print(f'Finished {f.__name__} in {time() - start:.1f} seconds.')

    return wrapper
