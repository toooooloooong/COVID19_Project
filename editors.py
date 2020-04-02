from argparse import ArgumentParser
from pandas import read_csv, concat, Series
from utils import File_manager, stabilize_path


class Editor:
    is_parent = True

    def __init__(self, file_name, format):
        self.file = File_manager('ref', file_name, format=format)

    def version_up(self, file_name):
        ver = int(self.file.ver[file_name]) + 1
        self.file.update_version({file_name: str(ver)})

    def delete(self, data, header, file_name):
        s = read_csv(self.file.path, squeeze=True, header=header)
        s[~s.isin(data)].to_csv(
            self.file.path, index=False, header=bool(header)
            )
        if self.is_parent:
            self.version_up(file_name)
        else:
            self.version_up()

    def add(self, data, header, file_name):
        s1 = read_csv(self.file.path, squeeze=True, header=header)
        s2 = Series(data)
        concat([s1, s2[~s2.isin(s1)]]).sort_values().to_csv(
            self.file.path, index=False, header=bool(header)
            )
        if self.is_parent:
            self.version_up(file_name)
        else:
            self.version_up()

    def edit_with_file(self, file_name, header, mode):
        path = File_manager('ref', file_name).path
        s = read_csv(path, squeeze=True, header=None)

        if self.is_parent:
            if mode == 'a':
                self.add(s, header, file_name)
            elif mode == 'd':
                self.delete(s, header, file_name)
        else:
            if mode == 'a':
                self.add(s)
            elif mode == 'd':
                self.delete(s)

        open(path, 'w').close()


class Stopwords_editor(Editor):
    is_parent = False

    def __init__(self):
        super().__init__('stopwords', 'csv')

    def version_up(self):
        super().version_up('stopwords')

    def delete(self, data):
        super().delete(data, 'infer', 'stopwords')

    def add(self, data):
        super().add(data, 'infer', 'stopwords')

    def edit_with_file(self, mode):
        super().edit_with_file('stopwordsToEdit', 'infer', mode)


class Userdic_editor(Editor):
    is_parent = False

    def __init__(self):
        super().__init__('userdic', 'txt')

    def version_up(self):
        super().version_up('userdic')

    def delete(self, data):
        super().delete(data, None, 'userdic')

    def add(self, data):
        super().add(data, None, 'userdic')

    def edit_with_file(self, mode):
        super().edit_with_file('userdicToEdit', None, mode)


@stabilize_path
def main(mode, object):
    if object == 'stopwords':
        editor = Stopwords_editor()
    elif object == 'userdic':
        editor = Userdic_editor()
    else:
        return

    editor.edit_with_file(mode)


if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument('-m', '--mode', required=True)
    parser.add_argument('-o', '--object', required=True)
    args = parser.parse_args()

    main(args.mode, args.object)
