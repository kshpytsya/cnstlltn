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

        if not res.tags:
            res.tag('untagged')

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


def write_files(bag, dest_dir):
    for fname, body in bag.items():
        dest_fname = dest_dir / fname
        dest_fname.parent.mkdir(parents=True, exist_ok=True)
        dest_fname.write_text(body)


def add_dicts(a, b):
    c = dict(a)
    c.update(b)
    return c


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

    new_files = dict(
        (
            bag_name,
            dict(
                ('/'.join(fname), render_f(imports))
                for fname, render_f in bag.items()
            )
        )
        for bag_name, bag in resource.files.items()
    )
    new_up_and_common = add_dicts(new_files['common'], new_files['up'])
    write_files(new_up_and_common, res_dir)
    new_deps = sorted(model.dependencies[resource.name])
    new_tags = sorted(resource.tags)

    resource_state = state['resources'].setdefault(resource.name, {})

    dirty = resource_state.get("dirty", True)
    old_files = resource_state.get("files", {})
    old_up_and_common = add_dicts(old_files.get('common', {}), old_files.get('up', {}))
    old_deps = resource_state.get("deps")
    old_tags = resource_state.get("tags", [])

    resource_vars = resources_vars[resource.name] = {}

    resource_state['files'] = new_files
    resource_state['deps'] = new_deps

    if full or dirty or new_up_and_common != old_up_and_common:
        click.echo("Bringing up resource '{}'".format(resource.name))

        resource_state['dirty'] = True
        resource_state.pop('exports', None)
        resource_state['files'] = new_files
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

        if (old_deps, old_files, old_tags) != (new_deps, new_files, new_tags):
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

    for bag in ('common', 'down'):
        write_files(resource_state.get('files', {}).get(bag, {}), res_dir)

    resource_state['dirty'] = True
    state.write()

    run_script(kind='down', res_dir=res_dir, res_name=res_name, debug=debug)

    del state['resources'][res_name]
    state.write()

    message_fname = res_dir / 'message.txt'
    if message_fname.exists():
        messages.append(message_fname.read_text())


class PathType(click.Path):
    def coerce_path_result(self, rv):
        return pathlib.Path(super().coerce_path_result(rv))


def join_split(seq, sep=","):
    r = sep.join(seq).split(sep)
    if r == ['']:
        return []
    else:
        return r


def inverse_setdict(setdict):
    result = {}

    for k, v in setdict.items():
        for i in v:
            result.setdefault(i, set()).add(k)

    return result


def with_all_dependencies(of, deps):
    result = of.copy()

    while True:
        new = result.copy()
        for i in result:
            new.update(deps.get(i, set()))

        if new == result:
            break

        result = new

    return result


def with_all_dependents(of, deps):
    return with_all_dependencies(of, inverse_setdict(deps))


def toposort_dependencies(of, deps):
    return list(filter(lambda i: i in of, toposort.toposort_flatten(deps)))


@click.command()
@click.version_option()
@click.option(
    '--only',
    multiple=True,
    metavar='NAMES',
    help="Resource names to process."
)
@click.option(
    '--skip',
    multiple=True,
    metavar='NAMES',
    help="Resource names to skip."
)
@click.option(
    '--tags', '-t',
    multiple=True,
    metavar='TAGS',
    help="Tags to match resources for processing."
)
@click.option(
    '--skip-tags', '-T',
    multiple=True,
    metavar='TAGS',
    help="Tags of resources to skip from processing."
)
@click.option(
    '--pretend',
    '-p',
    is_flag=True,
    help="Stop after reporting which resources are planned for processing."
)
@click.option(
    '--down', '-d',
    is_flag=True,
    help="Bring down all selected resources instead of bringing them up."
)
@click.option(
    '--full',
    is_flag=True,
    help="Run 'up' scripts even for existing 'clean' resources."
)
@click.option(
    '--yes', '-y',
    is_flag=True,
    help="Do not ask for confirmation to proceed with processing."
)
@click.option(
    '--debug',
    is_flag=True,
    help="Do not suppress exception stack traces, keep working directory, "
    "pass -x to bash to print commands and their arguments as they are executed."
)
@click.option(
    '--keep-work',
    is_flag=True,
    help="Keep working directory."
)
@click.option(
    '--file', '-f',
    type=PathType(
        dir_okay=False,
        exists=True
    ),
    default='Cnstlltnfile.py',
    help="Name of a model file to use",
    show_default=True
)
# TODO .dot dependency output
# TODO option to confirm each resource individually
def main(**kwargs):
    """
    Options --only, --tags, --skip, --skip-tags take comma separated lists and can be supplied
    multiple times with lists being accumulated.

    Resource tags match tags in --tags/--skip-tags if tag intersection set is not empty.
    Tags currently defined in the model are used for existing resources, as opposed to
    those which where defined when those resources where previously brought up.

    Both direct and indirect dependencies and dependents are considered in --only, --tags, --skip, --skip-tags.

    Options --only and --tags also select dependencies of these resources for bringing up,
    and dependents of these resources for bringing down.

    Options --skip and --skip-tags guarantee that resources selected by them are not going
    to be processed and will also skip bringing up of dependent resources and bringing
    down of dependencies of these resources.
    """
    opts = attrdict.AttrMap(kwargs)

    opts.only = set(join_split(opts.only))
    opts.skip = set(join_split(opts.skip))
    opts.tags = set(join_split(opts.tags))
    opts.skip_tags = set(join_split(opts.skip_tags))

    try:
        model = load_model(opts.file)
        validate_and_finalize_model(model)

        with model.state as state:
            report_sets = []

            existing_resources = state.setdefault('resources', {})

            current_tags = {}
            for k, v in existing_resources.items():
                current_tags[k] = set(v.get('tags', []))
            for k, v in model.resources.items():
                current_tags[k] = v.tags

            report_sets.append((
                "Existing resources (clean)",
                sorted(k for k, v in existing_resources.items() if not v['dirty'])
            ))
            report_sets.append((
                "Existing resources (dirty)",
                sorted(k for k, v in existing_resources.items() if v['dirty'])
            ))
            report_sets.append((
                "Resources defined in the model",
                sorted(model.resources)
            ))
            report_sets.append((
                "New resources",
                sorted(set(model.resources) - set(existing_resources))
            ))

            for k, v in sorted(inverse_setdict(current_tags).items()):
                report_sets.append((
                    "Resources tagged as '{}'".format(k),
                    sorted(v)
                ))

            existing_dependencies = dict((k, set(v['deps'])) for k, v in existing_resources.items())

            def is_included(res_name):
                return not(
                    opts.only and res_name not in opts.only
                    or opts.tags and opts.tags.isdisjoint(current_tags[res_name])
                )

            def is_excluded(res_name):
                return (
                    opts.skip and res_name in opts.skip
                    or opts.skip_tags and not opts.skip_tags.isdisjoint(current_tags[res_name])
                )

            def is_included_and_not_excluded(res_name):
                return is_included(res_name) and not is_excluded(res_name)

            resources_to_down = set(existing_resources)
            if opts.down:
                resources_to_up = []
            else:
                resources_to_up = model.resource_order
                resources_to_down -= set(model.resources)

            report_sets.append(("All resources to bring down", sorted(resources_to_down)))
            report_sets.append(("All resources to bring up", sorted(resources_to_up)))
            report_sets.append((
                "Explicitly selected resources to bring down",
                sorted(filter(is_included_and_not_excluded, resources_to_down))
            ))
            report_sets.append((
                "Explicitly selected resources to bring up",
                sorted(filter(is_included_and_not_excluded, resources_to_up))
            ))

            resources_to_down = set(filter(is_included, resources_to_down))
            resources_to_down = with_all_dependents(resources_to_down, existing_dependencies)
            resources_to_down -= with_all_dependencies(
                set(filter(is_excluded, resources_to_down)),
                existing_dependencies
            )
            resources_to_down = list(reversed(toposort_dependencies(resources_to_down, existing_dependencies)))

            resources_to_up = set(filter(is_included, resources_to_up))
            resources_to_up = with_all_dependencies(resources_to_up, model.dependencies)
            resources_to_up -= with_all_dependents(
                set(filter(is_excluded, resources_to_up)),
                model.dependencies
            )
            resources_to_up = toposort_dependencies(resources_to_up, model.dependencies)

            report_sets.append(("Will bring down (in this order)", resources_to_down))
            report_sets.append(("Will bring up (in this order)", resources_to_up))

            what_padding = max(len(what) for what, which in report_sets if which)

            for what, which in report_sets:
                if which:
                    click.echo("{:<{}} : {}".format(what, what_padding, ", ".join(which)))

            if not resources_to_down and not resources_to_up:
                click.echo("Nothing to do!")

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
