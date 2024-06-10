import pytest
from pathlib import Path
from autosubmit.migrate.migrate import Migrate
from autosubmitconfigparser.config.configcommon import AutosubmitConfig
from autosubmitconfigparser.config.yamlparser import YAMLParserFactory
from autosubmitconfigparser.config.basicconfig import BasicConfig
import os

import pwd
from log.log import AutosubmitCritical

from test.unit.utils.common import create_database, init_expid


@pytest.mark.skip('This test requires a running SSH server, with password-less authentication')
class TestMigrate:

    @pytest.fixture(scope='class')
    def migrate_tmpdir(self, tmpdir_factory):
        folder = tmpdir_factory.mktemp(f'migrate_tests')
        os.mkdir(folder.join('scratch'))
        os.mkdir(folder.join('migrate_tmp_dir'))
        file_stat = os.stat(f"{folder.strpath}")
        file_owner_id = file_stat.st_uid
        file_owner = pwd.getpwuid(file_owner_id).pw_name
        folder.owner = file_owner

        # Write an autosubmitrc file in the temporary directory
        autosubmitrc = folder.join('autosubmitrc')
        autosubmitrc.write(f'''
[database]
path = {folder}
filename = tests.db

[local]
path = {folder}

[globallogs]
path = {folder}

[structures]
path = {folder}

[historicdb]
path = {folder}

[historiclog]
path = {folder}

[defaultstats]
path = {folder}

''')
        os.environ['AUTOSUBMIT_CONFIGURATION'] = str(folder.join('autosubmitrc'))
        create_database(str(folder.join('autosubmitrc')))
        assert "tests.db" in [Path(f).name for f in folder.listdir()]
        init_expid(str(folder.join('autosubmitrc')), platform='pytest-local',create=False)
        assert "t000" in [Path(f).name for f in folder.listdir()]
        return folder

    @pytest.fixture(scope='class')
    def prepare_migrate(self, migrate_tmpdir):
        # touch as_misc
        as_misc_path = Path(f"{migrate_tmpdir.strpath}/t000/conf/as_misc.yml")
        platforms_path = Path(f"{migrate_tmpdir.strpath}/t000/conf/platforms_t000.yml")
        # In as_misc we put the pickup (NEW_USER)
        with as_misc_path.open('w') as f:
            f.write(f"""
AS_MISC: True
ASMISC:
    COMMAND: migrate

PLATFORMS:
    pytest-local:
        type: ps
        host: 127.0.0.1
        user: {migrate_tmpdir.owner}
        project: whatever_new
        scratch_dir: {migrate_tmpdir}/scratch
        temp_dir: {migrate_tmpdir}/migrate_tmp_dir
        same_user: True

""")

        with platforms_path.open('w') as f:
            f.write(f"""
PLATFORMS:
    pytest-local:
        type: ps
        host: 127.0.0.1
        user: {migrate_tmpdir.owner}
        project: whatever
        scratch_dir: {migrate_tmpdir}/scratch        

        """)
        expid_dir = Path(f"{migrate_tmpdir.strpath}/scratch/whatever/{migrate_tmpdir.owner}/t000")
        dummy_dir = Path(f"{migrate_tmpdir.strpath}/scratch/whatever/{migrate_tmpdir.owner}/t000/dummy_dir")
        real_data = Path(f"{migrate_tmpdir.strpath}/scratch/whatever/{migrate_tmpdir.owner}/t000/real_data")
        # write some dummy data inside scratch dir
        os.makedirs(expid_dir, exist_ok=True)
        os.makedirs(dummy_dir, exist_ok=True)
        os.makedirs(real_data, exist_ok=True)

        with open(dummy_dir.joinpath('dummy_file'), 'w') as f:
            f.write('dummy data')
        # create some dummy absolute symlinks in expid_dir to test migrate function
        os.symlink(dummy_dir.joinpath('dummy_file'), real_data.joinpath('dummy_symlink'))
        return migrate_tmpdir

    @pytest.fixture
    def migrate_remote_only(self, prepare_migrate):
        migrate = Migrate('t000', True)
        return migrate

    @pytest.fixture
    def migrate_prepare_test_conf(self, prepare_migrate, migrate_remote_only):
        basic_config = BasicConfig()
        basic_config.read()
        as_conf = AutosubmitConfig("t000", basic_config, YAMLParserFactory())
        as_conf.reload()
        original = as_conf.misc_data["PLATFORMS"]
        platforms = migrate_remote_only.load_platforms_in_use(as_conf)
        return as_conf, original, platforms, migrate_remote_only

    def test_migrate_conf_good_config(self, migrate_prepare_test_conf):
        # Test OK
        as_conf, original, platforms, migrate_remote_only = migrate_prepare_test_conf
        migrate_remote_only.check_migrate_config(as_conf, platforms, as_conf.misc_data["PLATFORMS"])
        as_conf.misc_data["PLATFORMS"]["PYTEST-LOCAL"]["TEMP_DIR"] = ""
        migrate_remote_only.check_migrate_config(as_conf, platforms, as_conf.misc_data["PLATFORMS"])

    def test_migrate_no_platforms(self, migrate_prepare_test_conf):
        as_conf, original, platforms, migrate_remote_only = migrate_prepare_test_conf
        as_conf.misc_data["PLATFORMS"] = {}
        with pytest.raises(AutosubmitCritical):
            migrate_remote_only.check_migrate_config(as_conf, platforms, as_conf.misc_data["PLATFORMS"])

    def test_migrate_no_scratch_dir(self, migrate_prepare_test_conf):
        as_conf, original, platforms, migrate_remote_only = migrate_prepare_test_conf
        as_conf.misc_data["PLATFORMS"]["PYTEST-LOCAL"]["SCRATCH_DIR"] = ""
        with pytest.raises(AutosubmitCritical):
            migrate_remote_only.check_migrate_config(as_conf, platforms, as_conf.misc_data["PLATFORMS"])

    def test_migrate_no_project(self, migrate_prepare_test_conf):
        as_conf, original, platforms, migrate_remote_only = migrate_prepare_test_conf
        as_conf.misc_data["PLATFORMS"]["PYTEST-LOCAL"]["PROJECT"] = ""
        with pytest.raises(AutosubmitCritical):
            migrate_remote_only.check_migrate_config(as_conf, platforms, as_conf.misc_data["PLATFORMS"])

    def test_migrate_no_same_user(self, migrate_prepare_test_conf):
        as_conf, original, platforms, migrate_remote_only = migrate_prepare_test_conf
        as_conf.misc_data["PLATFORMS"]["PYTEST-LOCAL"]["SAME_USER"] = False
        with pytest.raises(AutosubmitCritical):
            migrate_remote_only.check_migrate_config(as_conf, platforms, as_conf.misc_data["PLATFORMS"])

    def test_migrate_no_user(self, migrate_prepare_test_conf):
        as_conf, original, platforms, migrate_remote_only = migrate_prepare_test_conf
        as_conf.misc_data["PLATFORMS"]["PYTEST-LOCAL"]["USER"] = ""
        with pytest.raises(AutosubmitCritical):
            migrate_remote_only.check_migrate_config(as_conf, platforms, as_conf.misc_data["PLATFORMS"])

    def test_migrate_no_host(self, migrate_prepare_test_conf):
        as_conf, original, platforms, migrate_remote_only = migrate_prepare_test_conf
        as_conf.misc_data["PLATFORMS"]["PYTEST-LOCAL"]["HOST"] = ""
        with pytest.raises(AutosubmitCritical):
            migrate_remote_only.check_migrate_config(as_conf, platforms, as_conf.misc_data["PLATFORMS"])

    def test_migrate_remote(self, migrate_remote_only, migrate_tmpdir):
        # Expected behavior: migrate everything from scratch/whatever to scratch/whatever_new
        assert migrate_tmpdir.join(f'scratch/whatever/{migrate_tmpdir.owner}/t000').check(dir=True)
        assert migrate_tmpdir.join(f'scratch/whatever_new/{migrate_tmpdir.owner}/t000').check(dir=False)
        assert "dummy data" == migrate_tmpdir.join(
            f'scratch/whatever/{migrate_tmpdir.owner}/t000/real_data/dummy_symlink').read()

        migrate_remote_only.migrate_offer_remote()
        assert migrate_tmpdir.join(f'migrate_tmp_dir/t000').check(dir=True)
        migrate_remote_only.migrate_pickup()
        assert migrate_tmpdir.join(f'scratch/whatever/{migrate_tmpdir.owner}/t000').check(dir=False)
        assert migrate_tmpdir.join(f'scratch/whatever_new/{migrate_tmpdir.owner}/t000').check(dir=True)
        assert "dummy data" == migrate_tmpdir.join(
            f'scratch/whatever_new/{migrate_tmpdir.owner}/t000/real_data/dummy_symlink').read()
