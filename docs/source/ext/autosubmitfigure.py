import os
import re
import shutil

from docutils import nodes
from docutils.parsers.rst import directives
from pathlib import Path
from sphinx.directives import code
from sphinx.util.fileutil import copy_asset

from docs.source.ext.runcmd import RunCmdDirective

# This code is adapted from CWL User Guide, licensed under
# the CC BY 4.0 license, quoting their license:
#
# Attribution---You must give appropriate credit (mentioning
# that your work is derived from work that is Copyright ©
# the Common Workflow Language project, and, where practical,
# linking to https://www.commonwl.org/ ),...
# Ref: https://github.com/common-workflow-language/user_guide/blob/8abf537144d7b63c3561c1ff2b660543effd0eb0/LICENSE.md

""""
Patched version of https://github.com/sphinx-contrib/sphinxcontrib-runcmd
with default values to avoid having to re-type in every page. Also
prepends commands with a value (``$``), see https://github.com/invenia/sphinxcontrib-runcmd/issues/1.
Finally, it also checks if the command is ``cwltool``, and if then
tries to remove any paths from the command-line (not the logs).
"""

__version__ = "0.2.0"

# CONSTANTS
RE_SPLIT = re.compile(r"(?P<pattern>.*)(?<!\\)/(?P<replacement>.*)")


# These classes were in the .util module of the original directive.
class _Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(_Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class Singleton(_Singleton("SingletonMeta", (object,), {})):
    pass


class Python:
    pass


class AutosubmitFigureDirective(code.CodeBlock):
    has_content = False
    final_argument_whitespace = False
    required_arguments = 0
    optional_arguments = 99

    option_spec = {
        "linenos": directives.flag,
        "dedent": int,
        "lineno-start": int,
        "command": directives.unchanged_required,
        "expid": directives.unchanged_required,
        "type": directives.unchanged,
        "exp": directives.unchanged_required,
        "figure": directives.path,
        "name": directives.unchanged_required,
        "path": directives.unchanged_required,
        "width": directives.unchanged,
        "align": directives.unchanged,
        "alt": directives.unchanged,
        "caption": directives.unchanged,
    }

    def run(self):
        caption = self.options.get('caption')

        if self.options.get('name'):
            path_from = f"{self.env.srcdir}/{self.options.get('path')}/code/job_{self.options.get('name')}.yml"
            if Path(path_from).is_file():
                path_to = f"/home/docs/autosubmit/a000/conf/jobs_a000.yml"
                shutil.move(path_from, path_to)

            path_from = f"{self.env.srcdir}/{self.options.get('path')}/code/exp_{self.options.get('name')}.yml"
            if Path(path_from).is_file():
                path_to = f"/home/docs/autosubmit/a000/conf/expdef_a000.yml"
                shutil.move(path_from, path_to)

        command: str = f"autosubmit {self.options.get('command')} {self.options.get('expid')} --hide -o {self.options.get('type', 'png')}"

        RunCmdDirective(
            name='runCMD',
            arguments=[command],
            options={},
            content=self.content,
            lineno=self.lineno,
            content_offset=self.content_offset,
            block_text='\n'.join(self.content),
            state=self.state,
            state_machine=self.state_machine
        ).run()

        for f in Path("/home/docs/autosubmit/a000/plot/").glob('*'):
            if f.is_file():
                path_from = f"/home/docs/autosubmit/a000/plot/{f.name}"
                path_to = f"{self.env.srcdir}/{self.options.get('path')}/{self.options.get('figure')}"
                shutil.move(path_from, path_to)

        figure_node = nodes.figure()
        image_node = nodes.image(uri=f"./{self.options.get('figure')}")
        image_node['width'] = self.options.get('width', '100%')
        image_node['align'] = self.options.get('align', 'center')
        image_node['alt'] = self.options.get('alt', '')

        if caption:
            image_node += nodes.caption(text=caption)

        figure_node += image_node

        return [figure_node]


def setup(app):
    app.add_directive("autosubmitfigure", AutosubmitFigureDirective)

    return {"version": __version__}
