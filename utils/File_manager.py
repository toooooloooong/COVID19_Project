from glob import glob
from os import rename
from re import compile


class File_manager:
    def __init__(self, loc, subject, format='csv'):
        self.path = self.retrieve_path(loc, subject, format)
        self.ver = self.parse_version()

    def retrieve_path(self, loc, subject, format):
        path_list = glob(f'./data/{loc}\\{subject}*.{format}')

        if not path_list:
            path = f'./data/{loc}\\{subject}0.{format}'
            open(path, 'w').close()
            return path
        else:
            return path_list[0]

    def parse_version(self):
        return {k: v for k, v in compile(r'(\w+?)(\d+)_*').findall(self.path)}

    def compare_version(self, ver):
        return [k for k, v in self.ver.items() if ver.get(k) != self.ver[k]]

    def update_version(self, ver):
        def updater(m):
            return f'{m.group(1)}{ver.get(m.group(1), m.group(2))}{m.group(3)}'

        new_path = compile(r'(\w+?)(\d+)(_*)').sub(updater, self.path)

        etc = '_'.join(f'{k}{v}' for k, v in ver.items() if k not in self.ver)
        etc = etc if etc == '' else f'_{etc}'

        new_path = compile(r'(?!^\.)\..+?$').sub(fr'{etc}\g<0>', new_path)

        rename(self.path, new_path)
        self.path = new_path
        self.ver = self.parse_version()
