# This code is adapted from CWL User Guide, licensed under
# the CC BY 4.0 license, quoting their license:
#
# Attribution---You must give appropriate credit (mentioning
# that your work is derived from work that is Copyright ©
# the Common Workflow Language project, and, where practical,
# linking to https://www.commonwl.org/ ),...
# Ref: https://github.com/common-workflow-language/user_guide/blob/8abf537144d7b63c3561c1ff2b660543effd0eb0/LICENSE.md

import shutil
from pathlib import Path
from typing import cast

from docutils import nodes
from docutils.nodes import Element, Node
from docutils.parsers.rst import directives
from sphinx.directives import code

from docs.source.ext.runcmd import RunCmdDirective

"""Functions focusing in running commands, designed to be run with autosubmit, 
with that images will be generated and displayed in order to automate the documentation."""

__version__ = "0.2.0"


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
        "args": directives.unchanged,
        "exp": directives.unchanged_required,
        "output": directives.unchanged_required,
        "figure": directives.path,
        "name": directives.unchanged_required,
        "path": directives.unchanged_required,
        "width": directives.unchanged,
        "align": directives.unchanged,
        "alt": directives.unchanged,
        "caption": directives.unchanged,
    }

    def run(self) -> list[Node]:
        caption = self.options.get('caption')

        if self.options.get('name'):
            for key in ['jobs','expdef']:
                path_from = f"{self.env.srcdir}/{self.options.get('path')}/code/{key}_{self.options.get('name')}.yml"
                path_to = f"{self.env.app.outdir.parent}/autosubmit/autosubmit/a000/conf/{key}_a000.yml"
                if Path(path_from).is_file() and Path(path_to).is_file():
                    shutil.copy(path_from, path_to)

        command: str = f"autosubmit {self.options.get('command')} {self.options.get('expid')} --hide -o {self.options.get('type', 'png')} {self.options.get('args', '')}"

        run_cmd_node = RunCmdDirective(
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

        if self.options.get('output'):
            print("######### FIGURE OUTPUT #########")
            print(str(cast(Element, run_cmd_node[0])))

        for f in Path(f"{self.env.app.outdir.parent}/autosubmit/autosubmit/a000/plot/").glob('*'):
            path_from = f"{self.env.app.outdir.parent}/autosubmit/autosubmit/a000/plot/{f.name}"
            path_to = f"{self.env.srcdir}/{self.options.get('path')}/{self.options.get('figure')}"
            if f.is_file():
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
