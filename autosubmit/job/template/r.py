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

"""Autosubmit template scripts written in R."""

from textwrap import dedent

_DEFAULT_EXECUTABLE = "/usr/bin/env Rscript"
"""The default executable used when none provided."""

_AS_R_HEADER = dedent("""\
        ###################
        # Autosubmit header
        ###################
        oldw <- getOption("warn")
        options( warn = -1 )
        leave = F
        langs <- c("C.utf8","C.UTF-8","C","en_GB","es_ES")
        i = 1
        e=""
        while (nchar(e) == 0 || leave)
        {
            e=Sys.setlocale("LC_ALL",langs[i])
            e
            i=i+1
            if (i > NROW(langs)) 
            {
                leave=T
            }
        } 
        options( warn = oldw )
        job_name_ptrn = '%CURRENT_LOGDIR%/%JOBNAME%'
        fileConn<-file(paste(job_name_ptrn,"_STAT_%FAIL_COUNT%", sep = ''),"w")
        writeLines(toString(trunc(as.numeric(Sys.time()))), fileConn)
        close(fileConn)
        ###################
        # Autosubmit Checkpoint
        ###################
        # Creates a new checkpoint file upton call based on the current numbers of calls to the function
        AS_CHECKPOINT_CALLS = 0
        as_checkpoint <- function() {
            AS_CHECKPOINT_CALLS <<- AS_CHECKPOINT_CALLS + 1
            fileConn<-file(paste(job_name_ptrn,"_CHECKPOINT_",AS_CHECKPOINT_CALLS, sep = ''),"w")
            close(fileConn)
        }
        %EXTENDED_HEADER% 

""")
"""Autosubmit R header."""

_AS_R_TAILER = dedent("""\
        %EXTENDED_TAILER%
        ###################
        # Autosubmit tailer
        ###################

        fileConn<-file(paste(job_name_ptrn,'_COMPLETED', sep = ''), 'a')
        close(fileConn)
        quit(save = 'no', status = 0)

        """)
"""Autosubmit R tailer."""


def as_header(platform_header: str, executable: str) -> str:
    executable = executable or _DEFAULT_EXECUTABLE
    shebang = f'#!{executable}'

    return '\n'.join(
        [
            shebang,
            platform_header,
            _AS_R_HEADER]
    )


def as_body(body: str) -> str:
    return dedent(
        f"""
        ###################
        # Autosubmit job
        ###################

        tryCatch(
            expr = {{
                {body}
            }}
        ), finally {{
            # Write the finish time in the job _STAT_
            fileConn<-file(paste(job_name_ptrn,"_STAT_%FAIL_COUNT%", sep = ''),"a")
            writeLines(toString(trunc(as.numeric(Sys.time()))), fileConn)
            close(fileConn)
        }}

        # Now, we let the execution of the tailer happen, where the _COMPLETED
        # file will be created.
        """)


def as_tailer() -> str:
    return _AS_R_TAILER
