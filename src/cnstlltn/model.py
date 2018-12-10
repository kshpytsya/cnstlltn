import pathlib
import textwrap

FILE_BAGS = 'up', 'down', 'common'


class Resource:
    def __init__(self, model, name):
        self.model = model
        self.name = name
        self.tags = set()
        self.always_refresh = False
        self.imported = {}
        self.constants = {}
        self.exported = set()

        def make_per_bag(what):
            return dict((bag, what()) for bag in FILE_BAGS)

        self.files = make_per_bag(dict)
        self._dirs = make_per_bag(set)
        self.script_chunks = make_per_bag(list)
        self._script_chunk_seq = 0

    def tag(self, *tags):
        for tag in tags:
            assert isinstance(tag, str), "tag must be a string"
            self.tags.add(tag)

        return self

    def imports(self, **items):
        for import_name, (resource_name, export_name) in items.items():
            assert isinstance(resource_name, str), "resource name must be a string"
            assert isinstance(export_name, str), "export name must be a string"

            self.imported[import_name] = resource_name, export_name
            self.constants.pop(import_name, None)

        return self

    def const(self, **items):
        for name, value in items.items():
            assert isinstance(value, str), "const value must be a string"

            self.imported.pop(name, None)
            self.constants[name] = value

        return self

    def exports(self, *items):
        for export_name in items:
            assert isinstance(export_name, str), "export name must be a string"
            assert export_name.isidentifier(), "export name must be a valid identifier"

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

        return self

    def script_chunk(self, bag, chunk, *, order=0, dedent_str=True):
        assert bag in FILE_BAGS, "unknown bag: " + bag
        assert isinstance(chunk, str), "chunk must be a string"
        assert isinstance(order, int), "order must be an integer"

        if dedent_str:
            chunk = textwrap.dedent(chunk).lstrip()

        self.script_chunks[bag].append(((order, self._script_chunk_seq), chunk))
        self._script_chunk_seq += 1

        return self


class Model:
    def __init__(self, base_path):
        self.base_path = base_path
        self.statestorage = None
        self.resources = {}
        self.aliases = {}

    def resource(self, name, aliases=[]):
        assert isinstance(name, str), "resource name must be a string"

        ra = self.aliases.get(name)
        if ra:
            raise RuntimeError("'{}' is an existing alias assigned to resource: '{}'".format(name, ra))

        r = self.resources.get(name)

        if not r:
            r = Resource(self, name)

            self.resources[name] = r

        for alias in aliases:
            assert isinstance(alias, str), "resource alias must be a string"

            if alias in self.resources:
                raise RuntimeError("alias name matches existing resource: '{}'".format(alias))

            if alias in self.aliases:
                raise RuntimeError("alias '{}' for resource '{}' is already assigned to resource '{}'".format(
                    alias,
                    name,
                    self.aliases[alias]
                ))

            self.aliases[alias] = name

        return r
