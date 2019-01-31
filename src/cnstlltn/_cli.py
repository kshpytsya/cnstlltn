import ansimarkup
import attrdict
import braceexpand
import click
import fnmatch
import graphviz
import json
# import os
import pathlib
import re
import runpy
import shutil
import subprocess
import sys
import tempfile
import toposort

from . import diffformatter
from .model import Model
from .statestorage import StateStorage

# TODO
# shellcheck
# dot tmpdir


def validate_and_finalize_model(model):
    if model.statestorage is None:
        raise click.ClickException("'statestorage' has not been set")

    model.state = StateStorage(backend=model.statestorage)

    model.dependencies = {}

    for res_name, res in model.resources.items():
        for bag in ('up', 'down'):
            res.file(bag, "script.sh", "\n".join([
                i[1] for i in sorted(
                    res.data.script_chunks[bag] + res.data.script_chunks['common'],
                    key=lambda i: i[0]
                )
            ]))

        dependencies = model.dependencies[res_name] = set()

        for imp_name, (dep_res_name, dep_export_name) in res.data.imports.items():
            dependencies.add(dep_res_name)

            dep_res = model.resources.get(dep_res_name)

            if not dep_res:
                raise click.ClickException(
                    "resource '{}' depends on non-existent resource '{}'".format(
                        res_name,
                        dep_res_name
                    ))

            if dep_export_name not in dep_res.data.exports:
                raise click.ClickException(
                    "resource '{}' imports variable '{}' which is not exported by resource '{}'".format(
                        res_name,
                        dep_export_name,
                        dep_res_name
                    ))

        if not res.data.tags:
            res.tags('untagged')

    try:
        model.resource_order = toposort.toposort_flatten(model.dependencies)
    except toposort.CircularDependencyError as e:
        raise click.ClickException("circular resource dependencies: {}".format(e.data))


def load_py_model(path, workspace):
    py = runpy.run_path(path)

    configure_f = py.get('configure')
    if not callable(configure_f):
        raise click.ClickException("'configure' function is not defined or is not a callable")

    model = Model(path.parent, workspace)

    configure_f(model)

    notify_f = py.get('notify', lambda *a: None)
    if not callable(notify_f):
        raise click.ClickException("'notify' is not a callable")

    return model, lambda *a: notify_f(model, *a)


def load_model(path, workspace):
    if path.suffix == '.py':
        return load_py_model(path, workspace)

    raise click.ClickException("don't know how to interpret %s" % path)


def process_aliases(
    existing_resources,
    aliases
):
    renames = []
    processed = []

    for name, res in existing_resources.items():
        new_name = aliases.get(name)
        if new_name:
            renames.append((name, new_name))
            processed.append((new_name, res))
        else:
            processed.append((name, res))

    return dict(processed), renames


def make_graph(
    model,
    current_tags,
    existing_resources,
    existing_dependencies,
    resources_to_up,
    resources_to_down
):
    # TODO show renames
    graph = graphviz.Digraph()

    def res_color(res_name):
        if res_name in resources_to_up:
            return 'green'

        if res_name in resources_to_down:
            return 'red'

        return 'black'

    for res_name, res in sorted(model.resources.items()):
        is_existing = res_name in existing_resources
        is_dirty = is_existing and existing_resources[res_name]['dirty']
        label_suffix = ''
        if is_dirty:
            label_suffix += '*'
        graph.node(
            'res-' + res_name,
            label=res_name + label_suffix,
            color=res_color(res_name),
            style=['solid', 'bold'][is_existing],
            group=['new', 'existing'][is_existing]
        )

        dependencies = {}

        for imp_name, (dep_res_name, dep_export_name) in res.data.imports.items():
            dependencies.setdefault(dep_res_name, []).append(dep_export_name)

        for dep_res_name, imports in sorted(dependencies.items()):
            graph.edge(
                'res-' + dep_res_name,
                'res-' + res_name,
                label=",\n".join(sorted(imports))
            )

    for res_name, _ in sorted(existing_resources.items()):
        if res_name in model.resources:
            continue

        graph.node(
            'res-' + res_name,
            label=res_name,
            color=res_color(res_name),
            style='dashed',
            group='old'
        )

        for dep_res_name in sorted(existing_dependencies[res_name]):
            graph.edge(
                'res-' + dep_res_name,
                'res-' + res_name
            )

    all_tags = set()
    for res_name, tags in current_tags.items():
        for tag in tags:
            all_tags.add(tag)
            graph.edge(
                'tag-' + tag,
                'res-' + res_name,
                style='dashed',
                arrowhead='none'
            )

    with graph.subgraph(name='cluster_tags', graph_attr=dict(style='invis')) as subgraph:
        for tag in all_tags:
            subgraph.node(
                'tag-' + tag,
                label=tag,
                shape='rectangle',
                fillcolor='yellow',
                style='filled'
            )

    for seq, seq_style in (
        (resources_to_down, dict(color='red')),
        (resources_to_up, dict(color='green'))
    ):
        for i, j in zip(seq[:-1], seq[1:]):
            graph.edge(
                'res-' + i,
                'res-' + j,
                constraint='false',
                **seq_style
            )

    return graph


def run_script(*, kind, res_dir, res_name, debug, confirm_bail=False):
    # TODO signal handling per
    # https://stefan.sofa-rockers.org/2013/08/15/handling-sub-process-hierarchies-python-linux-os-x/
    cp = subprocess.run(
        [
            "/bin/bash",
            "-c",
            "set -eu{}o pipefail; source script.sh".format("x" if debug else "")
        ],
        cwd=res_dir
    )

    if cp.returncode != 0:
        error_message = "{} script for resource '{}' has failed with exit status {}".format(
            kind,
            res_name,
            cp.returncode
        )

        if confirm_bail:
            if click.confirm(
                "{}. Ignore and continue? Note: the resource will be permanently forgotten "
                "and probably left in an inconsistent state requiring manual intervention".format(error_message)
            ):
                return

        raise click.ClickException(error_message)


def write_files(bag, dest_dir):
    for fname, body in bag.items():
        dest_fname = dest_dir / fname
        dest_fname.parent.mkdir(parents=True, exist_ok=True)
        dest_fname.write_text(body)


def read_files(path, *, cb=lambda _: None, dest=None):
    if dest is None:
        dest = {}

    for i in path.iterdir():
        if i.is_file():
            dest[i.name] = i.read_text()
            cb(i)
        else:
            raise click.ClickException("don't know how to deal with '{}'".format(i.absolute()))

    return dest


def write_mementos(dest, state):
    if dest.exists():
        wipe_dir(dest)
    else:
        dest.mkdir()

    for res_name, res in state['resources'].items():
        res_dir_name = res_name
        if res.get('dirty', True):
            res_dir_name += ".dirty"

        res_dir = dest / res_dir_name
        res_dir.mkdir()

        for memento_name, memento_data in res.get('mementos', {}).items():
            (res_dir / memento_name).write_text(memento_data)

        for mode_str, mementos_names in res.get('mementos_modes', {}).items():
            mode = int(mode_str, base=0)
            for memento_name in mementos_names:
                (res_dir / memento_name).chmod(mode)


def add_dicts(a, b):
    c = dict(a)
    c.update(b)
    return c


def wipe_dir(d):
    for i in d.iterdir():
        if i.is_dir():
            shutil.rmtree(i)
        else:
            i.unlink()


def show_dict_diff(old, new):
    diffs = []

    for name in sorted(set(old) | set(new)):
        diffs.extend(diffformatter.format_diff(
            old.get(name, ''),
            new.get(name, ''),
            header=["modified: {}".format(name)]
        ))

    def cutline():
        click.echo("." * 80)

    if diffs:
        cutline()
        click.echo("".join(diffs), nl=False)
        cutline()


def names_to_re(names):
    if names:
        return re.compile(
            '|'.join(
                fnmatch.translate(j)
                for i in names
                for j in braceexpand.braceexpand(i)
            )
        )
    else:
        return None


def up_resource(
    *,
    debug,
    step,
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
    mementos_dir = res_dir / "mementos"
    mementos_dir.mkdir()
    istate_dir = res_dir / "state"

    imports = dict(
        (import_name, resources_vars[resource_name][export_name])
        for import_name, (resource_name, export_name) in resource.data.imports.items()
    )
    imports.update(resource.data.const)

    new_files = dict(
        (
            bag_name,
            dict(
                ('/'.join(fname), render_f(imports))
                for fname, render_f in bag.items()
            )
        )
        for bag_name, bag in resource.data.files.items()
    )
    new_up_and_common = add_dicts(new_files['common'], new_files['up'])
    write_files(new_up_and_common, res_dir)
    new_deps = sorted(model.dependencies[resource.name])
    new_tags = sorted(resource.data.tags)

    is_new_resource = resource.name not in state['resources']
    resource_state = state['resources'].setdefault(resource.name, {})

    istate = resource_state.get('state')
    if istate is not None:
        istate_dir.mkdir()
        write_files(istate, istate_dir)

    dirty = resource_state.get('dirty', True)
    old_files = resource_state.get('files', {})
    old_up_and_common = add_dicts(old_files.get('common', {}), old_files.get('up', {}))
    old_deps = resource_state.get('deps')
    old_tags = resource_state.get('tags', [])

    resource_vars = resources_vars[resource.name] = {}
    resource_mementos_modes = {}

    resource_state['files'] = new_files
    resource_state['deps'] = new_deps
    resource_state['tags'] = new_tags

    def check_products():
        for x_kind, x_set, x_var in [
            ("variable", resource.data.exports, resource_vars),
            ("memento", resource.data.mementos, resource_mementos)
        ]:
            for x_name in x_set:
                if x_name not in x_var:
                    raise click.ClickException("resource '{}' did not export '{}' {}".format(
                        resource.name,
                        x_name,
                        x_kind
                    ))

            unexpected = set(x_var) - x_set
            if unexpected:
                raise click.ClickException("resource '{}' exported unexpected {}(s): {}".format(
                    resource.name,
                    x_kind,
                    ', '.join(sorted(unexpected))
                ))

    if full or dirty or resource.data.always_refresh or new_up_and_common != old_up_and_common:
        click.echo("Bringing up resource '{}'".format(resource.name))

        if debug and not is_new_resource:
            show_dict_diff(old_up_and_common, new_up_and_common)

        resource_state['dirty'] = True
        resource_state.pop('exports', None)
        resource_state['files'] = new_files
        state.write()

        if step:
            click.confirm("Proceed?", abort=True, default=True)

        run_script(kind='up', res_dir=res_dir, res_name=resource.name, debug=debug)

        read_files(exports_dir, dest=resource_vars)

        if istate_dir.is_dir():
            istate = read_files(istate_dir)
        else:
            istate = None

        def memento_cb(path):
            resource_mementos_modes.setdefault(
                oct(path.stat().st_mode & 0o777),
                []
            ).append(path.name)

        resource_mementos = read_files(mementos_dir, cb=memento_cb)

        check_products()

        message_fname = res_dir / 'message.txt'
        if message_fname.exists():
            message = message_fname.read_text()
        else:
            message = None

        resource_state['dirty'] = False
        resource_state['exports'] = resource_vars
        resource_state['mementos'] = resource_mementos
        resource_state['mementos_modes'] = resource_mementos_modes
        resource_state['message'] = message
        if istate is not None:
            resource_state['state'] = istate
        else:
            resource_state.pop('state', None)

        state.write()
    else:
        click.echo("Resource '{}' is up to date".format(resource.name))

        resource_vars.update(resource_state.get('exports', {}))
        resource_mementos = resource_state.get('mementos', {})

        check_products()

        message = resource_state.get('message')

        if (old_deps, old_files, old_tags) != (new_deps, new_files, new_tags):
            state.write()

    if message:
        messages.append(message)


def down_resource(
    *,
    debug,
    step,
    messages,
    res_dir,
    res_name,
    state,
):
    if step:
        click.confirm("Bringing down resource '{}'. Proceed?".format(res_name), abort=True, default=True)
    else:
        click.echo("Bringing down resource '{}'".format(res_name))

    res_dir.mkdir()

    resource_state = state['resources'][res_name]

    for bag in ('common', 'down'):
        write_files(resource_state.get('files', {}).get(bag, {}), res_dir)

    resource_state['dirty'] = True
    state.write()

    run_script(kind='down', res_dir=res_dir, res_name=res_name, debug=debug, confirm_bail=True)

    del state['resources'][res_name]
    state.write()

    message_fname = res_dir / 'message.txt'
    if message_fname.exists():
        messages.append(message_fname.read_text())


class PathType(click.Path):
    def coerce_path_result(self, rv):
        return pathlib.Path(super().coerce_path_result(rv))


def join_split(seq, sep=None):
    r = (sep or " ").join(seq).split(sep)
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
    '--workspace', '-w',
    metavar='NAME',
    default="",
    help="name of a workspace to use",
    show_default=True
)
@click.option(
    '--only',
    multiple=True,
    metavar='NAMES',
    help="resource names to process"
)
@click.option(
    '--skip',
    multiple=True,
    metavar='NAMES',
    help="resource names to skip"
)
@click.option(
    '--tags', '-t',
    multiple=True,
    metavar='TAGS',
    help="tags to match resources for processing"
)
@click.option(
    '--skip-tags', '-T',
    multiple=True,
    metavar='TAGS',
    help="tags of resources to skip from processing"
)
@click.option(
    '--pretend',
    '-p',
    is_flag=True,
    help="stop after reporting which resources are planned for processing"
)
@click.option(
    '--down', '-d',
    is_flag=True,
    help="bring down all selected resources instead of bringing them up"
)
@click.option(
    '--full',
    is_flag=True,
    help="run 'up' scripts even for existing non-dirty up-to-date resources"
)
@click.option(
    '--yes', '-y',
    is_flag=True,
    help="do not ask for confirmation to proceed with processing"
)
@click.option(
    '--step',
    is_flag=True,
    help="confirm execution of each resource 'up'/'down' script"
)
@click.option(
    '--graph',
    is_flag=True,
    help="display a visual graph of resources (uses Graphviz)"
)
@click.option(
    '--debug',
    is_flag=True,
    help="do not suppress exception stack traces, keep working directory, "
    "pass -x to bash to print commands and their arguments as they are executed"
)
@click.option(
    '--keep-work',
    is_flag=True,
    help="keep working directory"
)
@click.option(
    '--file', '-f',
    type=PathType(
        dir_okay=False,
        exists=True
    ),
    default='Cnstlltnfile.py',
    help="name of a model file to use",
    show_default=True
)
@click.option(
    '--mementos', '-m',
    type=PathType(
        file_okay=False
    ),
    help="directory into which to store model mementos. Warning: will completely wipe the directory! "
    "Note that mementos will only be stored in case of a successful execution"
)
@click.option(
    '--edit',
    is_flag=True,
    help="edit the state json after initial load. Tread carefully and make backups!"
)
def main(**kwargs):
    """
    Options --only, --tags, --skip, --skip-tags take space separated lists and can be supplied
    multiple times with lists being accumulated. Note that it is necessary to use quoting or
    backslash to include space as a part of an option value.

    Names specified in --only and --skip are expanded using Bash-style brace expansion [1]
    and are treated as Unix shell-style wildcards [2]. Note that if any of these features
    are used, it is necessary to use quoting.

    [1] https://github.com/trendels/braceexpand
    [2] https://docs.python.org/3.7/library/fnmatch.html

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

    opts.only = names_to_re(join_split(opts.only))
    opts.skip = names_to_re(join_split(opts.skip))
    opts.tags = set(join_split(opts.tags))
    opts.skip_tags = set(join_split(opts.skip_tags))

    model = None

    try:
        model, notification_cb = load_model(opts.file, opts.workspace)
        validate_and_finalize_model(model)

        with model.state as state:
            notification_cb('lock')
            if opts.edit:
                edited_state_str = json.dumps(state, indent=4, sort_keys=True)

                while True:
                    edited_state_str = click.edit(text=edited_state_str, extension='.json')

                    if edited_state_str is None:
                        click.confirm("File was not saved. Continue execution with unedited state?", abort=True)
                    else:
                        try:
                            edited_state = json.loads(edited_state_str)
                        except json.JSONDecodeError as e:
                            click.confirm("Error parsing json: {}. Continue editing?".format(e), abort=True)
                            continue

                        state.clear()
                        state.update(edited_state)
                        state.write()

                    break

            if state:
                state_workspace = state.get("workspace", "")
                if opts.workspace != state_workspace:
                    raise click.ClickException(
                        "Workspace stored in state (\"{}\") "
                        "does not match workspace selected via --workspace (\"{}\")".format(
                            state_workspace,
                            opts.workspace
                        )
                    )
            else:
                state["workspace"] = opts.workspace

            report_sets = []

            existing_resources = state.setdefault('resources', {})

            existing_resources, alias_renames = process_aliases(existing_resources, model.aliases)

            current_tags = {}
            for k, v in existing_resources.items():
                current_tags[k] = set(v.get('tags', []))
            for k, v in model.resources.items():
                current_tags[k] = v.data.tags

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
                    opts.only and not opts.only.match(res_name)
                    or opts.tags and opts.tags.isdisjoint(current_tags[res_name])
                )

            def is_excluded(res_name):
                return (
                    opts.skip and opts.skip.match(res_name)
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

            what_padding = max((len(what) for what, which in report_sets if which), default=0)

            for what, which in report_sets:
                if which:
                    click.echo("{}{:<{}} : {}".format(
                        click.style(what, underline=True),
                        '',
                        what_padding - len(what),
                        ", ".join(which)
                    ))

            if alias_renames:
                click.echo("The following resources are renamed: {}".format(", ".join(
                    "{}->{}".format(i, j) for i, j in alias_renames
                )))

            if opts.graph:
                make_graph(
                    model,
                    current_tags,
                    existing_resources,
                    existing_dependencies,
                    resources_to_up,
                    resources_to_down
                ).view()

            if not resources_to_down and not resources_to_up:
                click.echo("Nothing to do!")

            if not opts.pretend:
                if not opts.yes and (resources_to_down or resources_to_up):
                    click.confirm("Proceed?", abort=True)

                if existing_resources is not state['resources']:
                    state['resources'] = existing_resources
                    state.write()

                success = False
                work_dir = pathlib.Path(tempfile.mkdtemp(prefix="cnstlltn."))
                messages = []

                notification_cb('start')

                try:
                    for res_i, res_name in enumerate(resources_to_down):
                        res_dir = work_dir / "down-{:04}-{}".format(res_i, res_name)
                        notification_cb('resource-down-start', res_name)
                        down_resource(
                            debug=opts.debug,
                            step=opts.step,
                            messages=messages,
                            res_dir=res_dir,
                            res_name=res_name,
                            state=state,
                        )
                        notification_cb('resource-down-done', res_name)

                    resources_vars = {}
                    for res_i, res_name in enumerate(resources_to_up):
                        resource = model.resources[res_name]
                        res_dir = work_dir / "up-{:04}-{}".format(res_i, res_name)
                        notification_cb('resource-up-start', res_name)
                        up_resource(
                            debug=opts.debug,
                            step=opts.step,
                            full=opts.full,
                            messages=messages,
                            model=model,
                            res_dir=res_dir,
                            resource=resource,
                            resources_vars=resources_vars,
                            state=state,
                        )
                        notification_cb('resource-up-done', res_name)
                finally:
                    if opts.debug and not success or opts.keep_work:
                        click.echo("keeping working directory: {}".format(work_dir))
                    else:
                        shutil.rmtree(work_dir)

                    for message in messages:
                        click.echo(ansimarkup.parse(message.rstrip()))

            if opts.mementos:
                write_mementos(opts.mementos, state)

        notification_cb('success')
    except Exception as e:
        if notification_cb:
            if isinstance(e, click.exceptions.Abort):
                notification_cb('abort')
            else:
                notification_cb('fail')

        if not opts.debug and not isinstance(e, (click.exceptions.ClickException, click.exceptions.Abort)):
            click.secho("error: {}".format(e), err=True, fg='red')
            sys.exit(1)
        else:
            raise
