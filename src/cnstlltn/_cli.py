import ansimarkup
import attrdict
import click
# import json
# import os
import pathlib
import runpy
import shutil
import subprocess
import sys
import tempfile
import toposort

from .model import Model
from .statestorage import StateStorage


def validate_and_finalize_model(model):
    if model.statestorage is None:
        raise click.ClickException("'statestorage' has not been set")

    model.state = StateStorage(backend=model.statestorage)

    model.dependencies = {}

    for res_name, res in model.resources.items():
        dependencies = model.dependencies[res_name] = set()

        for imp_name, dep_res_name, dep_export_name in res.imported:
            dependencies.add(dep_res_name)

            dep_res = model.resources.get(dep_res_name)

            if not dep_res:
                raise click.ClickException(
                    "resource '{}' depends on non-existent resource '{}'".format(
                        res_name,
                        dep_res_name
                    ))

            if dep_export_name not in dep_res.exported:
                raise click.ClickException(
                    "resource '{}' imports variable '{}' which is not exported by resource '{}'".format(
                        res_name,
                        dep_export_name,
                        dep_res_name
                    ))

    try:
        model.resource_order = toposort.toposort_flatten(model.dependencies)
    except toposort.CircularDependencyError as e:
        raise click.ClickException("circular resource dependencies: {}".format(e.data))


def load_py_model(path):
    py = runpy.run_path(path)

    configure_f = py.get('configure')
    if not callable(configure_f):
        raise click.ClickException("'configure' function is not defined or is not a callable")

    model = Model(path.parent)

    configure_f(model)

    return model


def load_model(path):
    if path.suffix == '.py':
        return load_py_model(path)

    raise click.ClickException("don't know how to interpret %s" % path)


def run_script(*, kind, res_dir, res_name, debug):
    cp = subprocess.run(
        [
            "/bin/bash",
            "-c",
            # note: no comma between the following strings
            "set -eu{}o pipefail;"
            "shopt -s nullglob;"
            "for i in s.*.sh; do source \"$i\"; done".format("x" if debug else "")
        ],
        cwd=res_dir
    )

    if cp.returncode != 0:
        raise click.ClickException("{} script for resource '{}' has failed with exit status {}".format(
            kind,
            res_name,
            cp.returncode
        ))


def up_resource(
    *,
    debug,
    full,
    messages,
    model,
    res_dir,
    resource,
    resources_vars,
    state,
):
    res_dir.mkdir()
    exports_dir = res_dir / "exports"
    exports_dir.mkdir()

    imports = dict(
        (import_name, resources_vars[resource_name][export_name])
        for import_name, resource_name, export_name in resource.imported
    )

    new_up_files, new_down_files = tuple(
        dict(
            (fname, render_f(imports))
            for fname, render_f in bag.items()
        )
        for bag in (resource.up_files, resource.down_files)
    )
    new_deps = sorted(model.dependencies[resource.name])

    resource_state = state['resources'].setdefault(resource.name, {})

    dirty = resource_state.get("dirty", True)
    old_up_files = resource_state.get("up_files")
    old_down_files = resource_state.get("down_files")
    old_deps = resource_state.get("deps")

    for fname, fbody in new_up_files.items():
        res_dir.joinpath(fname).write_text(fbody)

    resource_vars = resources_vars[resource.name] = {}

    resource_state['down_files'] = new_down_files
    resource_state['deps'] = new_deps

    if full or dirty or old_up_files != new_up_files:
        click.echo("Bringing up resource '{}'".format(resource.name))

        resource_state['dirty'] = True
        resource_state.pop('exports', None)
        resource_state['up_files'] = new_up_files
        state.write()

        run_script(kind='up', res_dir=res_dir, res_name=resource.name, debug=debug)

        for var_name in resource.exported:
            export_fname = exports_dir / var_name
            if export_fname.exists():
                resource_vars[var_name] = export_fname.read_text()
            else:
                raise click.ClickException("resource '{}' does not export '{}' variable".format(
                    resource.name,
                    var_name
                ))

        unexpected_exports = set(i.name for i in exports_dir.iterdir()) - resource.exported
        if unexpected_exports:
            raise click.ClickException("resource '{}' exports unexpected variables: {}".format(
                resource.name,
                ', '.join(sorted(unexpected_exports))
            ))

        message_fname = res_dir / 'message.txt'
        if message_fname.exists():
            message = message_fname.read_text()
        else:
            message = None

        resource_state['dirty'] = False
        resource_state['exports'] = resource_vars
        resource_state['message'] = message

        state.write()
    else:
        click.echo("Resource '{}' is up to date".format(resource.name))

        for var_name, var_value in resource_state['exports'].items():
            resource_vars[var_name] = var_value

        message = resource_state.get('message')

        if old_deps != new_deps or old_down_files != new_down_files:
            state.write()

    if message:
        messages.append(message)


def down_resource(
    *,
    debug,
    messages,
    res_dir,
    res_name,
    state,
):
    click.echo("Bringing down resource '{}'".format(res_name))

    res_dir.mkdir()

    resource_state = state['resources'][res_name]

    for fname, fbody in resource_state['down_files'].items():
        res_dir.joinpath(fname).write_text(fbody)

    resource_state['dirty'] = True
    state.write()

    run_script(kind='down', res_dir=res_dir, res_name=res_name, debug=debug)

    state['resources'].pop(res_name)
    state.write()

    message_fname = res_dir / 'message.txt'
    if message_fname.exists():
        messages.append(message_fname.read_text())


class PathType(click.Path):
    def coerce_path_result(self, rv):
        return pathlib.Path(super().coerce_path_result(rv))


@click.command()
@click.version_option()
# TODO: only
# TODO: tags
@click.option('--pretend', '-p', is_flag=True)
@click.option('--down', '-d', is_flag=True)
@click.option('--full', is_flag=True)
@click.option('--yes', '-y', is_flag=True)
@click.option('--debug', is_flag=True)
@click.option('--keep-work', is_flag=True)
@click.option(
    '--file', '-f',
    type=PathType(
        dir_okay=False,
        exists=True
    ),
    default='Cnstlltnfile.py'
)
def main(**kwargs):
    opts = attrdict.AttrMap(kwargs)

    try:
        model = load_model(opts.file)
        validate_and_finalize_model(model)

        with model.state as state:
            existing_resources = state.setdefault('resources', {})

            resources_to_down = set(existing_resources)
            if opts.down:
                resources_to_up = []
            else:
                resources_to_up = model.resource_order

                def keep_resources(resources):
                    nonlocal resources_to_down
                    resources_to_down -= set(resources)
                    for res_name in resources:
                        keep_resources(model.dependencies[res_name])

                keep_resources(set(model.resources))

            resources_to_down = [
                j for j in
                reversed(toposort.toposort_flatten(dict(
                    (i, set(existing_resources[i]['deps']))
                    for i in resources_to_down
                )))
                if j in resources_to_down
            ]

            for what, which in [
                ("Will bring down: {}", resources_to_down),
                ("Will bring up: {}", resources_to_up),
            ]:
                if which:
                    def describe_res(name):
                        existing_resource = existing_resources.get(name)
                        if existing_resource is not None:
                            if existing_resource.get('dirty', True):
                                return "dirty"
                            else:
                                return "clean"
                        else:
                            return "new"

                    click.echo(what.format(
                        ", ".join(
                            "{}({})".format(i, describe_res(i))
                            for i in which
                        )
                    ))

            if not opts.pretend:
                if not opts.yes and (resources_to_down or resources_to_up):
                    click.confirm("Proceed?", abort=True)

                success = False
                work_dir = pathlib.Path(tempfile.mkdtemp(prefix="cnstlltn."))
                messages = []

                try:
                    for res_i, res_name in enumerate(resources_to_down):
                        res_dir = work_dir / "down-{:04}-{}".format(res_i, res_name)
                        down_resource(
                            debug=opts.debug,
                            messages=messages,
                            res_dir=res_dir,
                            res_name=res_name,
                            state=state,
                        )

                    resources_vars = {}
                    for res_i, res_name in enumerate(resources_to_up):
                        resource = model.resources[res_name]
                        res_dir = work_dir / "up-{:04}-{}".format(res_i, res_name)
                        up_resource(
                            debug=opts.debug,
                            full=opts.full,
                            messages=messages,
                            model=model,
                            res_dir=res_dir,
                            resource=resource,
                            resources_vars=resources_vars,
                            state=state,
                        )
                finally:
                    if opts.debug and not success or opts.keep_work:
                        click.echo("keeping working directory: {}".format(work_dir))
                    else:
                        shutil.rmtree(work_dir)

                    for message in messages:
                        click.echo(ansimarkup.parse(message.rstrip()))

    except Exception as e:
        if not opts.debug and not isinstance(e, (click.exceptions.ClickException, click.exceptions.Abort)):
            click.secho("error: {}".format(e), err=True, fg='red')
            sys.exit(1)
        else:
            raise
