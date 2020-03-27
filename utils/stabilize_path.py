from os import getcwd, chdir, path


def stabilize_path(f):
    if getcwd()[-16:] != 'COVID19_Project':
        dir = path.realpath(__file__)
        dir = dir[:dir.rfind('COVID19_Project') + 15]
        chdir(dir)

    return f
