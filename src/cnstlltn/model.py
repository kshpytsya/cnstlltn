import textwrap


class Resource:
    def __init__(self, name):
        self.name = name
        self.imported = set()
        self.exported = set()
        self.up_files = {}
        self.down_files = {}

    def imports(self, **items):
        for import_name, (resource_name, export_name) in items.items():
            assert isinstance(resource_name, str)
            assert isinstance(export_name, str)

            self.imported.add((import_name, resource_name, export_name))

        return self

    def exports(self, *items):
        for export_name in items:
            assert isinstance(export_name, str)

            self.exported.add(export_name)

        return self

    def _file(self, bag, name, body, *, dedent_str=True):
        assert isinstance(name, str)
        assert '/' not in name

        if dedent_str and isinstance(body, str):
            body = textwrap.dedent(body).lstrip()

        if callable(body):
            wrapped_body = body
        else:
            wrapped_body = lambda imp: body  # noqa: E731

        bag[name] = wrapped_body

    def up_file(self, *args, **kw):
        self._file(self.up_files, *args, **kw)
        return self

    def down_file(self, *args, **kw):
        self._file(self.down_files, *args, **kw)
        return self


class Model:
    def __init__(self, base_path):
        self.base_path = base_path
        self.statestorage = None
        self.resources = {}

    def resource(self, name, aliases=[]):
        assert isinstance(name, str)

        r = self.resources.get(name)

        if not r:
            r = Resource(name)

            self.resources[name] = r

            for alias in aliases:
                assert isinstance(alias, str)
                # TODO
                # self.resource[alias] = r

        return r
