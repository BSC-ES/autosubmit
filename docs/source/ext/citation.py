# Copyright 2015-2025 Earth Sciences Department, BSC-CNS
#
# This file is part of Autosubmit.
#
# Autosubmit is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# Autosubmit is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with Autosubmit.  If not, see <http://www.gnu.org/licenses/>.

from pathlib import Path

from docutils.nodes import Node
from docutils.statemachine import StringList
from ruamel.yaml import YAML
from sphinx import addnodes
from sphinx.util import logging
from sphinx.util.docutils import SphinxDirective

# TODO: We could use Autosubmit's version here... if someday we make it dynamic and available
#       to Python (it's in a file now? ``VERSION``, updated manually, I believe...).
__version__ = "0.1.0"

logger = logging.getLogger(__name__)


class AutosubmitCitationDirective(SphinxDirective):
    has_content = True
    required_arguments = 0
    optional_arguments = 99
    final_argument_whitespace = False

    option_spec = {}

    def run(self) -> list[Node]:
        citation_file = Path(__file__).parents[3].joinpath('CITATION.cff')
        citation_data = YAML().load(stream=citation_file)

        preferred_citation = citation_data['preferred-citation']
        authors = ', '.join(
            [
                f'{author["given-names"][0]}. {author["family-names"]}'
                for author in preferred_citation['authors']
            ]
        )
        doi = preferred_citation['doi']

        rst = [
            citation_data['message'],
            '',
            '.. grid:: 1',
            '   :gutter: 0',
            '   :margin: 0',
            '   :padding: 0',
            '',
            '   .. grid-item-card::',
            '',
            f'      | **{preferred_citation["title"]}**',
            f'      | {authors} ({preferred_citation["year"]})',
            f'      | *{preferred_citation["journal"]}, {preferred_citation["publisher"]["alias"]} {doi}*',
            '       ',
            f'      `https://doi.org/{doi} <https://doi.org/{doi}>`_'
        ]

        node = addnodes.desc()
        self.state.nested_parse(
            StringList(rst),
            self.content_offset,
            node
        )
        return [node]


def setup(app):
    app.add_directive("citation", AutosubmitCitationDirective)

    return {
        "version": __version__,
        "parallel_read_safe": True,
        "parallel_write_safe": True
    }
