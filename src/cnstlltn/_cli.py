# import ansimarkup
import attrdict
import click
import contextlib
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
from .statestorage_intf import IStateStorage


def validate_and_finalize_model(model):
    if model.statestorage is None:
        raise click.ClickException("'statestorage' has not been set")

    model.statestorage = IStateStorage(model.statestorage)

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


RESOURCE_KEY_PREFIX = "resource:"


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


def up_resource(*, model, res_dir, resource, resources_vars, full, debug):
    res_dir.mkdir()
    exports_dir = res_dir / "exports"
    exports_dir.mkdir()

    imports = dict(
        (import_name, resources_vars[resource_name][export_name])
        for import_name, resource_name, export_name in resource.imported
    )

    desired_up_files, desired_down_files = tuple(
        dict(
            (fname, render_f(imports))
            for fname, render_f in bag.items()
        )
        for bag in (resource.up_files, resource.down_files)
    )

    state_key = RESOURCE_KEY_PREFIX + resource.name
    resource_state = model.statestorage.state.get(state_key, {})
    prev_resource_state = dict(resource_state)

    def save_state():
        nonlocal prev_resource_state
        if resource_state != prev_resource_state:
            model.statestorage.set(state_key, resource_state)
            prev_resource_state = dict(resource_state)

    dirty = resource_state.get("dirty", True)
    existing_up_files = resource_state.get("up_files", None)

    for fname, fbody in desired_up_files.items():
        res_dir.joinpath(fname).write_text(fbody)

    resource_vars = resources_vars[resource.name] = {}

    resource_state["down_files"] = desired_down_files
    resource_state["deps"] = sorted(model.dependencies[resource.name])

    if full or dirty or existing_up_files != desired_up_files:
        click.echo("bringing up resource '{}'".format(resource.name))

        resource_state["dirty"] = True
        resource_state.pop("exports", None)
        resource_state["up_files"] = desired_up_files
        save_state()

        run_script(kind="up", res_dir=res_dir, res_name=resource.name, debug=debug)

        resource_state["dirty"] = False
        resource_state["exports"] = resource_vars

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
    else:
        click.echo("resource '{}' is up to date".format(resource.name))

        for var_name, var_value in resource_state["exports"].items():
            resource_vars[var_name] = var_value

    save_state()


def down_resource(*, res_name, statestorage, res_dir, debug):
    click.echo("bringing down resource '{}'".format(res_name))

    res_dir.mkdir()

    state_key = RESOURCE_KEY_PREFIX + res_name
    resource_state = statestorage.state[state_key]

    for fname, fbody in resource_state["down_files"].items():
        res_dir.joinpath(fname).write_text(fbody)

    resource_state["dirty"] = True
    statestorage.set(state_key, resource_state)

    run_script(kind="down", res_dir=res_dir, res_name=res_name, debug=debug)

    statestorage.set(state_key)


class PathType(click.Path):
    def coerce_path_result(self, rv):
        return pathlib.Path(super().coerce_path_result(rv))


@click.command()
@click.version_option()
# TODO: list
# TODO: only
# TODO: tags
@click.option('--state-timeout', type=int, default=10)
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

        success = False
        work_dir = pathlib.Path(tempfile.mkdtemp(prefix="cnstlltn."))

        try:
            with contextlib.closing(model.statestorage.open(timeout=opts.state_timeout)):

                existing_resources = dict(
                    (i[len(RESOURCE_KEY_PREFIX):], j)
                    for i, j in model.statestorage.state.items() if i.startswith(RESOURCE_KEY_PREFIX)
                )

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
                        (i, set(existing_resources[i]["deps"]))
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
                                if existing_resource.get("dirty", True):
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

                if not opts.yes and (resources_to_down or resources_to_up):
                    click.confirm("Proceed?", abort=True)

                for res_i, res_name in enumerate(resources_to_down):
                    res_dir = work_dir / "down-{:04}-{}".format(res_i, res_name)
                    down_resource(
                        statestorage=model.statestorage,
                        res_dir=res_dir,
                        res_name=res_name,
                        debug=opts.debug
                    )

                resources_vars = {}
                for res_i, res_name in enumerate(resources_to_up):
                    resource = model.resources[res_name]
                    res_dir = work_dir / "up-{:04}-{}".format(res_i, res_name)
                    up_resource(
                        model=model,
                        res_dir=res_dir,
                        resource=resource,
                        resources_vars=resources_vars,
                        full=opts.full,
                        debug=opts.debug
                    )
        finally:
            if opts.debug and not success or opts.keep_work:
                click.echo("keeping working directory: {}".format(work_dir))
            else:
                shutil.rmtree(work_dir)

    except Exception as e:
        if not opts.debug and not isinstance(e, (click.exceptions.ClickException, click.exceptions.Abort)):
            click.secho("error: {}".format(e), err=True, fg='red')
            sys.exit(1)
        else:
            raise

#     text = ansimarkup.parse(text)
# click.echo(text)
