#!/usr/bin/env python3

import argparse
import datetime
import time
from collections import defaultdict

import requests

FEATURE_LABELS = [
    "config",
    "wrappers",
    "performance",
    "statistics",
    "logging",
    "traceability",
    "documentation",
]

PRIORITY_ORDER = [
    "priority:low",
    "priority:medium",
    "priority:high",
    "priority:critical",
]


def parse_args():
    parser = argparse.ArgumentParser(
        description="Generate a Markdown report summarizing OPEN GitHub issues"
    )

    parser.add_argument("--repo", required=True, help="Repository owner/repo or URL")
    parser.add_argument("--token", required=True, help="GitHub API token")
    parser.add_argument("--output", default="issue_summary.md", help="Output file")
    parser.add_argument(
        "--sleep", type=float, default=1.0, help="Sleep time between API requests"
    )

    return parser.parse_args()


def normalize_repo(repo):
    if repo.startswith("http"):
        parts = repo.rstrip("/").split("/")
        return f"{parts[-2]}/{parts[-1]}"
    return repo


def fetch_open_issues(repo, headers, sleep_time):
    issues = []
    page = 1

    while True:
        url = f"https://api.github.com/repos/{repo}/issues"
        params = {"state": "open", "per_page": 100, "page": page}

        r = requests.get(url, headers=headers, params=params)

        if not r.ok:
            print("GitHub API error:", r.status_code, r.text)
            r.raise_for_status()

        data = r.json()

        if not data:
            break

        issues.extend([i for i in data if "pull_request" not in i])
        page += 1

        time.sleep(sleep_time)

    return issues


def parse_date(date_str):
    return datetime.datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%SZ")


def compute_open_aging(issues):
    now = datetime.datetime.now(tz=datetime.timezone.utc)

    buckets = {"<30d": 0, "30-90d": 0, "over 90d": 0}

    for i in issues:
        created = parse_date(i["created_at"])
        age = (now - created).days

        if age < 30:
            buckets["<30d"] += 1
        elif age < 90:
            buckets["30-90d"] += 1
        else:
            buckets["over 90d"] += 1

    return buckets


def generate_report(issues):
    total = len(issues)

    by_year = defaultdict(int)
    priority_counts = {p: 0 for p in PRIORITY_ORDER}
    project_counts = defaultdict(int)
    feature_counts = {f: 0 for f in FEATURE_LABELS}
    pending_validation = 0

    destine_issues = []

    for i in issues:
        year = parse_date(i["created_at"]).year
        by_year[year] += 1

        labels = [l["name"] for l in i["labels"]]

        for p in PRIORITY_ORDER:
            if p in labels:
                priority_counts[p] += 1

        for l in labels:
            if l.startswith("project:"):
                project_counts[l] += 1
                if l == "project:destine":
                    destine_issues.append(i)

        for f in FEATURE_LABELS:
            if f in labels:
                feature_counts[f] += 1

        if "pending-validation" in labels:
            pending_validation += 1

    open_aging = compute_open_aging(issues)

    report = [
        "# Open Issue Summary\n",
        f"Total open issues: {total}\n",
        "\n## Issues per Year\n",
    ]

    for y in sorted(by_year):
        report.append(f"- {y}: {by_year[y]}")

    report.append("\n## Open Issue Aging\n")
    for k, v in open_aging.items():
        report.append(f"- {k}: {v}")

    report.append("\n## By Priority\n")
    for p in PRIORITY_ORDER:
        report.append(f"- {p}: {priority_counts[p]}")

    report.append("\n## By Project\n")
    for p, c in sorted(project_counts.items()):
        report.append(f"- {p}: {c}")

    report.append("\n## Feature Labels\n")
    for f in FEATURE_LABELS:
        report.append(f"- {f}: {feature_counts[f]}")

    report.append("\n## Pending Validation\n")
    report.append(f"- pending-validation: {pending_validation}")

    # DESTINE PROJECT SUMMARY
    report.append("\n# Project: destine Summary\n")

    dest_priority = {p: 0 for p in PRIORITY_ORDER}
    dest_features = {f: 0 for f in FEATURE_LABELS}
    dest_years = defaultdict(int)

    for i in destine_issues:
        labels = [l["name"] for l in i["labels"]]
        year = parse_date(i["created_at"]).year
        dest_years[year] += 1

        for p in PRIORITY_ORDER:
            if p in labels:
                dest_priority[p] += 1

        for f in FEATURE_LABELS:
            if f in labels:
                dest_features[f] += 1

    report.append("\n## Priority Breakdown\n")
    for p in PRIORITY_ORDER:
        report.append(f"- {p}: {dest_priority[p]}")

    report.append("\n## Feature Breakdown\n")
    for f in FEATURE_LABELS:
        report.append(f"- {f}: {dest_features[f]}")

    report.append("\n## Issues per Year\n")
    for y in sorted(dest_years):
        report.append(f"- {y}: {dest_years[y]}")

    return "\n".join(report)


def main():
    args = parse_args()

    repo = normalize_repo(args.repo)

    headers = {
        "Authorization": f"Bearer {args.token}",
        "Accept": "application/vnd.github+json",
    }

    print(f"Fetching OPEN issues from {repo}...")
    issues = fetch_open_issues(repo, headers, args.sleep)

    print(f"Fetched {len(issues)} open issues")

    report = generate_report(issues)

    with open(args.output, "w") as f:
        f.write(report)

    print(f"Report written to {args.output}")


if __name__ == "__main__":
    main()
