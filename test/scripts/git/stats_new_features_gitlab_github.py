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

# In December 2024 Autosubmit code base was migrated from BSC's GitLab,
# https://earth.bsc.es/gitlab/es/autosubmit, to under the BSC-ES GitHub
# organisation, at https://github.com/BSC-ES/autosubmit.
#
# 2024: This script uses the GitLab REST API and Python to fetch the issues
# from BSC archived GitLab project, and report the number of features added
# (for project proposals).
#
# 2025+: This script uses the GitHub REST API and Python to fetch the issues
# from the BSC-ES/autosubmit repository from GitHub, and report the number of
# features added (for project proposals).
#
# The results of this script are not perfect, as in GitLab some issues had
# labels like 'bug', but before the issue was closed the label was removed;
# or a new feature issue did not use the 'new feature' label; etc.
#
# Requirements:
# - python-gitlab==6.2.*
# - PyGitHub==2.6.* (https://github.com/Xiaoven/PyGithub/tree/main@main for issue.type, https://github.com/PyGithub/PyGithub/pull/3322)
# - pytz


def new_features_2024():
    import os

    import gitlab

    AUTOSUBMIT_PROJECT_ID = 58
    gl = gitlab.Gitlab(url='https://earth.bsc.es/gitlab', private_token=os.environ['GITLAB_TOKEN'])
    # gl.enable_debug()
    project = gl.projects.get(AUTOSUBMIT_PROJECT_ID)

    issues_in_2024 = project.issues.list(
        per_page=10,
        iterator=True,
        created_after='2024-01-01T00:00:00.00+00:00',
        created_before='2024-12-31T00:00:00.00+00:00',
        state='closed'
    )

    ignore_labels = ['bug', 'to do']
    filtered_labels = []

    # Run first without any iids, then add here the ones you want to ignore.
    ignore_iids = [
        1439,
        1419,
        1314,
        1310,
        1303,
        1300,
        1297,
        1287,
        1284,
        1224,
        1195,
        1194
    ]

    for issue in issues_in_2024:
        labels_not_ignored = not set(issue.labels).intersection(ignore_labels)
        not_ignored = labels_not_ignored and issue.iid not in ignore_iids
        in_milestone = issue.milestone is not None
        not_fixing = (
                not issue.title.lower().startswith('fix') and
                not issue.title.lower().startswith('failure') and
                not issue.title.lower().startswith('issue') and
                not issue.title.lower().startswith('publish') and
                not issue.title.lower().startswith('deprecate') and
                not issue.title.lower().startswith('delete') and
                not issue.title.lower().startswith('remove') and
                '[documentation]' not in issue.title.lower() and
                '[docs]' not in issue.title.lower() and
                '[tests]' not in issue.title.lower() and
                '[test]' not in issue.title.lower() and
                '[testing]' not in issue.title.lower() and
                '[regression test]' not in issue.title.lower() and
                '[cicd]' not in issue.title.lower() and
                'bug fix' not in issue.title.lower() and
                'nose' not in issue.title.lower() and
                'pytest' not in issue.title.lower() and
                'website' not in issue.title.lower()
        )
        if not_ignored and in_milestone and not_fixing:
            filtered_labels.append(issue)

    print(f'New features in 2024: {len(filtered_labels)}')
    # print(filtered_labels)

    for issue in filtered_labels:
        print(f'https://earth.bsc.es/gitlab/es/autosubmit/-/issues/{issue.iid}')


def new_features_2025():
    import datetime
    import os
    import pytz

    from github import Github
    from github import Auth

    utc = pytz.UTC

    auth = Auth.Token(os.environ['GITHUB_TOKEN'])

    g = Github(auth=auth)

    repo = g.get_repo("BSC-ES/autosubmit")

    issues_since_2025 = repo.get_issues(
        since=datetime.datetime(2025, 1, 1),
        state='closed',
    )
    last_day_2025 = datetime.datetime(2025, 12, 31, 23, 59, 59)
    last_day_2025 = utc.localize(last_day_2025)

    filtered_issues = []

    # Run first without any iids, then add here the ones you want to ignore.
    ignore_iids = [
        2456,
        2413,
        2054,
        2065,
        2031,
        2030,
        1480,
        1479,
        1472,
        1470,
        1456,
        1366,
        1055,
        345,
        626,
        813,
        2446,
        2403,
        2360,
        2300,
        2314,
        2324,
        2275,
        2164,
        2136,
        2198,
        2112,
        1390,
        1114,
        972,
        975,
        992,
        1008,
        1011,
        1091,
        2385,
        2374,
        2364,
        1469,
        2160,
        2332,
        2305,
        2286,
        2267,
        2261,
        2160,
        2077,
        1469,
        1972,
        1360,
        1261,
        961
    ]

    ignore_labels = [
        'bug',
        'to do'
    ]

    for issue in issues_since_2025:
        label_names = list(map(lambda l: l.name, issue.labels))
        if issue.created_at < last_day_2025:

            issue_type = issue.type
            if issue_type:
                if issue_type.name == 'Bug':
                    continue

            # Must have a milestone set.
            if not issue.milestone:
                continue

            title_lower = issue.title.lower()
            not_dependabot = not title_lower.startswith('bump') and not title_lower.startswith('updat')
            not_refactoring = not title_lower.startswith('[refactor')
            not_docs = not title_lower.startswith('[doc') and 'document' not in title_lower
            not_tests = not title_lower.startswith('[test')
            not_fixing = not title_lower.startswith('fix')
            not_moving = not title_lower.startswith('move')
            not_errors = (
                    not title_lower.startswith('critical') and
                    not title_lower.startswith('[critical') and
                    not title_lower.startswith('[error') and
                    not title_lower.startswith('error') and
                    not title_lower.startswith('issue') and
                    'critical error' not in title_lower and
                    not title_lower.startswith('unknown error') and
                    not title_lower.startswith('unexpected error') and
                    'not working' not in title_lower and
                    'missing' not in title_lower and
                    'possible bug' not in title_lower and
                    'missing in' not in title_lower and
                    not title_lower.startswith('bug') and
                    not title_lower.startswith('problem') and
                    not title_lower.startswith('cannot') and
                    not title_lower.startswith('can not') and
                    'test fail' not in title_lower and
                    'valueerror' not in title_lower and
                    'attributeerror' not in title_lower and
                    'keyerror' not in title_lower and
                    not title_lower.endswith('error') and
                    not title_lower.endswith('fails') and
                    'broken' not in title_lower and
                    'deprecate' not in title_lower and
                    'exception' not in title_lower
            )
            labels_not_ignored = not set(label_names).intersection(ignore_labels)
            not_about_citation = 'citation' not in title_lower
            not_build = (
                    'pip' not in title_lower and
                    'setuptools' not in title_lower and
                    'vulture' not in title_lower and
                    'lint' not in title_lower
            )
            not_ignored = (
                    issue.number not in ignore_iids and
                    not title_lower.startswith('delete')
            )
            not_cicd = 'cicd' not in title_lower and 'ci/cd' not in title_lower and 'ci cd' not in title_lower and 'gh actions' not in title_lower and 'github actions' not in title_lower
            not_pull_request = not issue.pull_request

            if (
                    not_dependabot and
                    not_refactoring and
                    not_docs and
                    not_tests and
                    not_fixing and
                    not_moving and
                    not_errors and
                    not_ignored and
                    not_cicd and
                    not_pull_request and
                    not_about_citation and
                    not_build and
                    labels_not_ignored
            ):
                filtered_issues.append(issue)

    print(f'New features in 2025: {len(filtered_issues)}')
    for issue in filtered_issues:
        print(issue)


def main():
    print('Tallying numbers...')
    # new_features_2024()
    new_features_2025()


if __name__ == '__main__':
    main()
