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
# from BSC archived GitLab project, and report the average time to close
# issues.
#
# 2025+: This script uses the GitHub REST API and Python to fetch the issues
# from the BSC-ES/autosubmit repository from GitHub, and report the average
# time to close issues.
#
# Requirements:
# - python-gitlab==6.2.*
# - PyGitHub==2.6.* (https://github.com/Xiaoven/PyGithub/tree/main@main for issue.type, https://github.com/PyGithub/PyGithub/pull/3322)
# - pytz


def avg_close_time_gitlab_2024():
    import datetime
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

    seconds_to_close = 0

    for issue in issues_in_2024:
        closed_at = datetime.datetime.strptime(issue.closed_at, '%Y-%m-%dT%H:%M:%S.%f%z')
        created_at = datetime.datetime.strptime(issue.created_at, '%Y-%m-%dT%H:%M:%S.%f%z')
        time_to_close = closed_at - created_at
        seconds_to_close += time_to_close.total_seconds()

    avg_seconds = seconds_to_close / len(issues_in_2024)
    print(f'Average time to close in seconds: {avg_seconds}')
    print(f'Average time to close in minutes: {avg_seconds / 60}')
    print(f'Average time to close in hours: {avg_seconds / 60 / 60}')
    print(f'Average time to close in days: {avg_seconds / 60 / 60 / 24}')


def avg_close_time_gitlab_2025():
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

    issues_in_2025 = []

    seconds_to_close = 0

    for issue in issues_since_2025:
        if issue.created_at < last_day_2025:
            issues_in_2025.append(issue)
            closed_at = issue.closed_at
            created_at = issue.created_at
            time_to_close = closed_at - created_at
            seconds_to_close += time_to_close.total_seconds()

    avg_seconds = seconds_to_close / len(issues_in_2025)
    print(f'Average time to close in seconds: {avg_seconds}')
    print(f'Average time to close in minutes: {avg_seconds / 60}')
    print(f'Average time to close in hours: {avg_seconds / 60 / 60}')
    print(f'Average time to close in days: {avg_seconds / 60 / 60 / 24}')


def main():
    print('Tallying numbers...')
    # avg_close_time_gitlab_2024()
    avg_close_time_gitlab_2025()


if __name__ == '__main__':
    main()
