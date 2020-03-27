from glob import glob
from os import rename
from re import compile


class File_manager:
    def __init__(self, loc, subject, format='csv'):
        self.path = self.retrieve_path(loc, subject, format)

    def retrieve_path(self, loc, subject, format):
        path_list = glob(f'./data/{loc}\\{subject}*.{format}')

        if not path_list:
            path = f'./data/{loc}\\{subject}0.{format}'
            open(path, 'w').close()
            return path
        else:
            return path_list[0]

    def retrieve_file(self, mode):
        return open(self.path, mode)

    def parse_version(self):
        return {k: v for k, v in compile(r'(\w+?)(\d+)_*').findall(self.path)}

    def update_version(self, ver):
        def updater(m):
            return f'{m.group(1)}{ver.get(m.group(1), m.group(2))}{m.group(3)}'

        new_path = compile(r'(\w+?)(\d+)(_*)').sub(updater, self.path)

        cur = self.parse_version()
        append = '_'.join(f'{k}{v}' for k, v in ver.items() if k not in cur)
        append = append if append == '' else f'_{append}'

        new_path = compile(r'(?!^\.)\..+?$').sub(fr'{append}\g<0>', new_path)

        rename(self.path, new_path)
        self.path = new_path
