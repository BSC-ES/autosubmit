:orphan:

Commands
========

Autosubmit provides a command-line interface for managing experiments,
workflows, and the Autosubmit installation.

The general syntax is::

    autosubmit [global options] COMMAND [command options]

Global options such as ``--version`` and logging options apply to all
commands. Each command provides its own options and arguments.

For help on the command line, run::

    autosubmit --help

For help on a specific command, run::

    autosubmit COMMAND --help


Command groups
--------------

.. autosubmit-commands::

Command-line completion
-----------------------

Autosubmit provides optional Bash tab completion for commands and command-line
options.

To enable completion for the current shell session, source the completion
script included with the Autosubmit installation::

    source /path/to/autosubmit-completion.bash

Once enabled, pressing ``TAB`` after ``autosubmit`` completes available
sub-command names::

    $ autosubmit <TAB>
    archive       clean         create        ...
    configure     delete        describe      ...
    refresh       report        run           ...

After selecting a sub-command, pressing ``TAB`` again completes its command
options. For example::

    $ autosubmit run <TAB>
    --help
    --profile
    --run_only_members
    --start_after
    --update_version
    ...

Partial options are also completed::

    $ autosubmit run --prof<TAB>
    $ autosubmit run --profile

Autosubmit intentionally does not complete experiment IDs or other
dynamically generated positional values. An Autosubmit installation may
contain thousands or tens of thousands of experiments, and querying and
listing those values during every TAB completion would add unnecessary
overhead to the command line.

Completion therefore only uses information that is inexpensive to obtain
locally, such as registered commands, ``argparse`` options, and statically
defined option choices.

The completion script only affects the current shell when it is sourced.
To enable it automatically for future Bash sessions, add the ``source``
command to your shell startup configuration, such as ``~/.bashrc``.

.. note::

   The completion script communicates with Autosubmit through an internal
   ``__complete`` command. This command is intended for shell completion and
   is not part of the normal Autosubmit command-line interface.

