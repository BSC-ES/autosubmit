"""Run the configuration precedence example with an isolated Autosubmit database."""

import os
from pathlib import Path
from tempfile import TemporaryDirectory

from autosubmit.config.basicconfig import BasicConfig
from autosubmit.config.configcommon import AutosubmitConfig
from autosubmit.database.db_common import create_db
from autosubmit.log.log import Log


def main() -> None:
    with TemporaryDirectory() as directory:
        root = Path(directory)
        rc = root / "autosubmitrc"
        rc.write_text(f"[database]\npath = {root}\n[local]\npath = {root}\n", encoding="utf-8")
        os.environ["AUTOSUBMIT_CONFIGURATION"] = str(rc)
        BasicConfig.read()
        create_db()
        Log.set_console_level("ERROR")

        expid = "a000"
        conf = root / expid / "conf"
        conf.mkdir(parents=True)
        shared = root / "shared.yml"
        local = root / "local.yml"
        shared.write_text("EXPERIMENT:\n  NUMCHUNKS: 2\n", encoding="utf-8")
        local.write_text("EXPERIMENT:\n  NUMCHUNKS: 4\n", encoding="utf-8")
        defaults = (
            f"DEFAULT:\n  EXPID: {expid}\n  HPCARCH: local\n"
            f"  CUSTOM_CONFIG:\n    PRE: '{shared}'\n"
        )
        override = "EXPERIMENT:\n  NUMCHUNKS: 3\n"
        layers = (
            ("PRE only", defaults, 2),
            ("PRE + experiment", defaults + override, 3),
            ("PRE + experiment + POST", defaults + f"    POST: '{local}'\n" + override, 4),
        )

        for label, contents, expected in layers:
            (conf / "experiment.yml").write_text(contents, encoding="utf-8")
            config = AutosubmitConfig(expid)
            config.reload(force_load=True)
            chunks = config.get_num_chunks()
            if chunks != expected:
                raise RuntimeError(f"{label}: expected NUMCHUNKS {expected}, got {chunks}")
            print(f"{label}: NUMCHUNKS = {chunks}")


if __name__ == "__main__":
    main()
