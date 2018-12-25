import formic
import jinja2
import json
import pathlib
import shlex


def add_files_to_resource(
    resource,
    bag,
    *formic_args,
    **formic_kw
):
    """
    https://formic.readthedocs.io/en/latest/api.html
    """

    dest = pathlib.Path(formic_kw.pop('dest', '.'))
    assert not dest.is_absolute(), "'dest' cannot be an absolute path"
    assert '..' not in dest.parts, "'dest' path cannot contain '..'"

    directory = pathlib.Path(formic_kw.pop('directory', '.'))
    if not directory.is_absolute():
        directory = resource.model.base_path.joinpath(directory).resolve()

    formic_kw['directory'] = directory

    for file_name in formic.FileSet(*formic_args, **formic_kw):
        file_path = pathlib.Path(file_name)
        dest_file_path = dest.joinpath(file_path.relative_to(directory))

        if not file_path.is_file():
            raise RuntimeError("do not know how to deal with '{}'".format(file_path))

        resource.file(bag, dest_file_path, lambda _, file_path=file_path: file_path.read_text())


def add_imports_as_json(
    resource,
    *,
    bag='common',
    file_name='imports.json'
):
    resource.file(bag, file_name, lambda imports: json.dumps(imports))


def format_shell_vars(v, name_prefix):
    return "\n".join("{}{}={}".format(name_prefix, k, shlex.quote(v)) for k, v in sorted(v.items()))


def add_imports_as_sh(
    resource,
    *,
    bag='common',
    file_name='imports.sh',
    name_prefix="IMP_"
):
    resource.file(bag, file_name, lambda imports: format_shell_vars(imports, name_prefix))


def add_import_as_file(
    resource,
    import_name,
    *,
    bag='common',
    file_name=None
):
    if file_name is None:
        file_name = 'imports/' + import_name

    resource.file(bag, file_name, lambda imports: imports[import_name])


def add_formatted_imports(
    resource,
    file_name,
    format_str,
    *,
    bag='common'
):
    resource.file(bag, file_name, lambda imports: format_str.format_map(imports))


def add_jinja(
    resource,
    file_name,
    template_str,
    *,
    bag='common',
    validator=None,
    jinja_opts=None
):
    if jinja_opts is None:
        jinja_opts = dict(
            undefined=jinja2.runtime.StrictUndefined
        )

    template = jinja2.Template(template_str, **jinja_opts)

    def render(imports):
        result = template.render(imports)

        if validator:
            validator(result)

        return result

    resource.file(bag, file_name, render)


def add_reexport(
    resource,
    import_name,
    export_name=None
):
    if export_name is None:
        export_name = import_name

    add_import_as_file(resource, import_name, file_name='exports/' + export_name, bag='up')
    resource.exports(export_name)
