# GitHub Actions Setup

This project includes an automated workflow at:

`/.github/workflows/resource-daily-activity.yml`

It runs the script:

`resource_daily_activity_and_task_report.py`

## Schedule

The workflow is scheduled for Bangladesh time (`Asia/Dhaka`, `GMT+6`) at:

- every hour, at minute `40`

GitHub Actions cron uses UTC, so the workflow file contains the UTC equivalents.

## Required GitHub Secrets

Add these in:

`Repository Settings -> Secrets and variables -> Actions`

Required:

- `CLICKUP_API_TOKEN`
- `GOOGLE_SERVICE_ACCOUNT_JSON`
- `AUDIT_SHEET_URL`
- `LOCAL_RESOURCE_POOL_URL`

Optional:

- `PROJECT_NAME_FILTER`
- `HIGHLIGHT_TRACKER_ERRORS`
- `RESOURCE_LOOKUP_TAB`

## Secret Values

### `GOOGLE_SERVICE_ACCOUNT_JSON`

Store the full JSON content of `service_account.json` as the secret value.

Do not store the file path. In GitHub Actions there is no local `service_account.json` file unless you create one manually, and this script already supports reading raw JSON from the env var.

### `PROJECT_NAME_FILTER`

Leave this secret empty or do not create it unless you want to restrict the run to one exact project name.

## What The Workflow Does

1. Checks out the repository
2. Sets up Python `3.11`
3. Installs the required Python packages from `requirements.txt`
4. Runs:

```bash
python resource_daily_activity_and_task_report.py
```

## Manual Run

You can also run it manually from:

`Actions -> Resource Daily Activity Report -> Run workflow`

## Notes

- The workflow uses in-memory caches only for the current run. This is safe for GitHub Actions.
- The script still depends on Google Sheets and ClickUp being reachable from GitHub-hosted runners.
- If a scheduled run is delayed slightly, that is normal for GitHub Actions cron.
