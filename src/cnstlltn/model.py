import pathlib
import textwrap

FILE_BAGS = 'up', 'down', 'common'


class Resource:
    def __init__(self, name):
        self.name = name
        self.imported = set()
        self.exported = set()
        self.files = dict((bag, {}) for bag in FILE_BAGS)
        self._dirs = dict((bag, set()) for bag in FILE_BAGS)

    def imports(self, **items):
        for import_name, (resource_name, export_name) in items.items():
            assert isinstance(resource_name, str), "resource name must be a string"
            assert isinstance(export_name, str), "export name must be a string"

            self.imported.add((import_name, resource_name, export_name))

        return self

    def exports(self, *items):
        for export_name in items:
            assert isinstance(export_name, str), "export name must be a string"

            self.exported.add(export_name)

        return self

    def file(self, bag, dest, src, *, dedent_str=True):
        assert bag in FILE_BAGS, "unknown bag: " + bag
        dest = pathlib.PurePosixPath(dest)
        assert not dest.is_absolute(), "path cannot be absolute"
        assert '..' not in dest.parts, "path cannot contain '..'"
        assert dest.parts, "path cannot be a directory"

        for check_bag in FILE_BAGS if bag == 'common' else (bag, 'common'):
            if dest.parts in self._dirs[check_bag]:
                raise RuntimeError("path is a directory: {}".format(dest))

            for prefix_len in range(1, len(dest.parts)):
                dest_prefix = dest.parts[:prefix_len]
                if dest_prefix in self.files[check_bag]:
                    raise RuntimeError("file already exists: " + "/".join(dest_prefix))

                self._dirs[check_bag].add(dest_prefix)

            self.files[check_bag].pop(dest.parts, None)

        if dedent_str and isinstance(src, str):
            src = textwrap.dedent(src).lstrip()

        if callable(src):
            wrapped_src = src
        else:
            wrapped_src = lambda imp: src  # noqa: E731

        self.files[bag][dest.parts] = wrapped_src


class Model:
    def __init__(self, base_path):
        self.base_path = base_path
        self.statestorage = None
        self.resources = {}

    def resource(self, name, aliases=[]):
        assert isinstance(name, str), "resource name must be a string"

        r = self.resources.get(name)

        if not r:
            r = Resource(name)

            self.resources[name] = r

            for alias in aliases:
                assert isinstance(alias, str), "resource alias must be a string"
                # TODO
                # self.resource[alias] = r

        return r
