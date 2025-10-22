# This code is adapted from CWL User Guide, licensed under
# the CC BY 4.0 license, quoting their license:
#
# Attribution---You must give appropriate credit (mentioning
# that your work is derived from work that is Copyright ©
# the Common Workflow Language project, and, where practical,
# linking to https://www.commonwl.org/ ),...
# Ref: https://github.com/common-workflow-language/user_guide/blob/8abf537144d7b63c3561c1ff2b660543effd0eb0/LICENSE.md

from pathlib import Path
from shutil import copy, move
from typing import cast, Optional
from sphinx.util import logging

from docutils import nodes
from docutils.nodes import Element, Node
from docutils.parsers.rst import directives
from sphinx.util.docutils import SphinxDirective

from docs.source.ext.runcmd import RunCmdDirective

"""Functions focusing in running commands, designed to be run with autosubmit, 
with that images will be generated and displayed in order to automate the documentation."""

# TODO: We could use Autosubmit's version here... if someday we make it dynamic and available
#       to Python (it's in a file now? ``VERSION``, updated manually, I believe...).
__version__ = "0.2.0"

logger = logging.getLogger(__name__)


class AutosubmitFigureDirective(SphinxDirective):
    has_content = True
    required_arguments = 0
    optional_arguments = 99
    final_argument_whitespace = False

    option_spec = {
        "linenos": directives.flag,
        "dedent": int,
        "lineno-start": int,
        "command": directives.unchanged_required,
        "expid": directives.unchanged_required,
        "type": directives.unchanged,
        "args": directives.unchanged,
        "exp": directives.unchanged_required,
        "output": directives.unchanged_required,
        "figure": directives.path,
        "name": directives.unchanged_required,
        "width": directives.unchanged,
        "align": directives.unchanged,
        "alt": directives.unchanged,
        "caption": directives.unchanged,
    }

    def run(self) -> list[Node]:
        caption = self.options.get('caption')

        configuration_name: Optional[str] = self.options.get('name', None)
        if configuration_name is None:
            logger.warning('No configuration name specified in the figure directive, skipping copying YAML files.')
            raise ValueError('No configuration name specified in the figure directive.')
        source_path = Path(self.env.srcdir, self.env.docname).parent
        _copy_rst_yaml_files_to_as_experiment(source_path, self.env.app.outdir.parent, configuration_name)

        command: str = ' '.join([
            'autosubmit',
            '-lc',
            'DEBUG',
            cast(str, self.options.get('command')),
            cast(str, self.options.get('expid')),
            '--hide',
            '-o',
            self.options.get('type', 'png'),
            self.options.get('args', ''),
            '-v'
        ])
        self._run_command(command)

        # The ``autosubmit`` directory used below is created in the ``Makefile``. Make sure you
        # have it (and subdirectories) and the ``AUTOSUBMIT_CONFIGURATION`` is pointing to the
        # ``autosubmitrc`` file in this directory, or you might end up using real experiments.
        experiment_plot_path = Path(self.env.app.outdir.parent, 'autosubmit/autosubmit/a000/plot/')
        target_path = Path(self.env.app.outdir, '_static/images/', self.env.docname).parent
        figure_name: str = cast(str, self.options.get('figure'))
        _move_latest_image(experiment_plot_path, target_path, figure_name)

        # We extract the part after the build directory (e.g. after ``build/html``, like
        # ``_static/images/.../file.png``), to use this as the URI for the figure.
        figure_uri = '/' + str(Path(target_path, figure_name).relative_to(self.env.app.outdir))

        img_rel_path, _ = self.env.relfn2path(figure_uri)
        builder = self.env.app.builder
        # NOTE: builder has a ``.link_suffix``! So we need to drop the extension... (*shrugs*)
        # TODO: Maybe there is a better way of doing this? And does it work with PDF and other builders too?
        figure_uri_relative = builder.get_relative_uri(self.env.docname, img_rel_path).split('.html')[0]

        figure_node = nodes.figure()
        image_node = nodes.image(src=figure_uri_relative)
        image_node['width'] = self.options.get('width', '100%')
        image_node['align'] = self.options.get('align', 'center')
        image_node['alt'] = self.options.get('alt', '')

        if caption:
            image_node += nodes.caption(text=caption)

        figure_node += image_node

        # NOTE: If we return the ``figure`` node, it will fail as the image does not exist.
        #       If we modify the state machine (``self.state_machine.insert_input(bla, source=rst_file)`` or so)
        #       it will render it incorrectly (or treat it as title?).
        #       So we re-use what ``figure`` did, but add it as raw HTML! It just works TM.

        raw_node = nodes.raw('', str(figure_node), format='html')

        logger.debug('The raw HTML we will use:')
        logger.debug(str(raw_node))

        return [raw_node]

    def _run_command(self, command: str) -> None:
        """Uses the existing ``runcmd`` directive to execute a command that produces images.

        :param command: Command to execute.
        """
        run_cmd_node = RunCmdDirective(
            name='runCMD',
            arguments=[command],
            options={
                'cache': False
            },
            content=self.content,
            lineno=self.lineno,
            content_offset=self.content_offset,
            block_text='\n'.join(self.content),
            state=self.state,
            state_machine=self.state_machine
        ).run()

        logger.debug(f'Below the output of the command: {command}')
        logger.debug(str(cast(Element, run_cmd_node[0])))


def _copy_rst_yaml_files_to_as_experiment(
        source_path: Path,
        target_path: Path,
        configuration_name: str
) -> None:
    """Copy YAML files used by this reStructuredText documentation into the Autosubmit experiment.

    This experiment is the one being currently used by this page to create the images dynamically.

    We copy any files that exist in the ``code`` subdirectory, of the directory of the reStructuredText
    file, and that match the names ``jobs.yml`` and ``expdef.yml``. We can add more names or change
    this logic later, if needed.

    The ``configuration_name`` is expected to be used in the code file name. For instance,
    if you have a name like ``monarch-da``, then you might have ``<FOLDER-WITH-RST>/code/jobs_monarch-da.yml``,
    with the ``JOBS`` to be added to the experiment used to produce documentation images.

    Not all the reStructuredText pages will have YAML files.

    Missing and invalid files do not result in errors, but are logged at ``DEBUG`` level.

    :param source_path: Where we search for the YAML files (should be the parent of ``code`` folder).
    :param target_path: Path where we will find the experiment where the YAML files must be copied to.
    :param configuration_name: Configuration name.
    """
    # FIXME: move the source YAML files?
    if not configuration_name:
        logger.debug('No configuration name specified in the fixture, skipping copying YAML files.')
    expid = 'a000'
    logger.debug(f'Searching for YAML configuration for experiment {expid} in {source_path}')
    for key in ['jobs', 'expdef']:
        path_from = source_path / f"code/{key}_{configuration_name}.yml"
        if not path_from.is_file():
            logger.debug(f'Skipping invalid YAML file {path_from}')
            continue
        path_to = Path(target_path, f'autosubmit/autosubmit/{expid}/conf/{key}_{expid}.yml')
        logger.debug(f'Copying {path_from} to {path_to}')
        copy(path_from, path_to)


def _move_latest_image(experiment_plot_path: Path, target_path: Path, figure_name: str) -> None:
    """Moves latest image produced with the ``runcmd`` directive to the Sphinx build directory.

    In case an Autosubmit experiment ends up with many image files, this function will look for
    the latest and move it to the Sphinx build directory.

    :param experiment_plot_path: Path where the Autosubmit experiment produced plot images (PNG).
    :param target_path: Path where we will move the latest image produced with the ``runcmd`` directive to.
    :param figure_name: The name of the image used in our documentation.
    """
    images = [png for png in experiment_plot_path.glob('*.png')]
    if not images:
        logger.warning(f'Could not find latest image in {experiment_plot_path}')
        return
    latest_image = max([png for png in experiment_plot_path.glob('*.png')], key=lambda f: f.stat().st_ctime)
    if figure_name.startswith('/'):
        figure_name = figure_name[1:]
    path_to = target_path / figure_name
    path_to.parent.mkdir(parents=True, exist_ok=True)

    logger.debug(f'Moving {latest_image} to {path_to}')
    move(latest_image, path_to)


def setup(app):
    app.add_directive("autosubmitfigure", AutosubmitFigureDirective)

    return {
        "version": __version__,
        "parallel_read_safe": True,
        "parallel_write_safe": True
    }
