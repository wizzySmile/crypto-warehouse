# Daily Automation Troubleshooting

Use this checklist when the crypto pipeline does not appear to run, or when a
scheduled run fails.

## 1. Confirm the change is deployed

- Check that the workflow and Airflow changes are on the repository's default
  branch (`main` in this project).
- In GitHub **Actions**, confirm the latest **Fly Deploy** run completed
  successfully.
- A successful deploy only confirms deployment. It does not confirm that the
  next scheduled pipeline task will complete.

## 2. Check GitHub Actions

Open `.github/workflows/daily-update.yml` and confirm it has a `schedule` event.
GitHub cron expressions use UTC: `17 8 * * *` means 9:17 AM in Lagos.

In the workflow's **Actions** page, distinguish scheduled runs (`schedule`)
from runs started with **Run workflow** (`workflow_dispatch`). If no scheduled
run appears:

- Confirm Actions are enabled and the workflow is on the default branch.
- Check the cron time in UTC and avoid scheduling exactly at the start of an
  hour; GitHub may delay scheduled events during busy periods.
- Remember that a workflow being active does not guarantee GitHub will deliver
  every scheduled event.

The GitHub workflow runs independently of Airflow. It is not conditional on an
Airflow failure.

## 3. Check the live Airflow scheduler on Fly.io

Run these commands from the project directory:

```powershell
flyctl status -a crypto-warehouse
Invoke-RestMethod -Uri 'https://crypto-warehouse.fly.dev/health'
flyctl ssh console -a crypto-warehouse -C "airflow dags list"
flyctl ssh console -a crypto-warehouse -C "airflow jobs check --job-type SchedulerJob"
flyctl ssh console -a crypto-warehouse -C "airflow dags list-import-errors -o json"
flyctl ssh console -a crypto-warehouse -C "airflow dags list-runs -d crypto_pipeline"
```

Look for a started Fly machine, a healthy scheduler and metadatabase, no DAG
import errors, and `crypto_pipeline` with `is_paused` set to `False`. In the run
history, check whether the latest run is `success`, `failed`, or absent.

- **No run:** check that the DAG is unpaused and the scheduler is healthy; then
  check the schedule and GitHub default-branch/Actions settings.
- **Failed run:** open that run's task log in the Airflow UI and inspect
  `flyctl logs -a crypto-warehouse --no-tail` before redeploying. A scheduler
  can be healthy while a pipeline task fails.

## 4. Check configuration and dependencies

The pipeline needs `DATABASE_URL`, `R2_ACCOUNT_ID`, `R2_ACCESS_KEY`,
`R2_SECRET_KEY`, `R2_ENDPOINT`, and `R2_BUCKET` in the relevant platform's
secrets. `flyctl secrets list -a crypto-warehouse` shows configured secret
names without showing their values. Check GitHub repository Actions secrets
separately. Never paste secret values into logs, chat, or issue reports.

The Airflow image must include the pipeline's runtime dependencies, including
`pyarrow`, `python-dotenv`, and `psycopg2-binary`. Its Airflow version must also
be compatible with the existing Airflow metadata database. In this incident,
the database had a 2.10.3 migration revision while the Dockerfile used Airflow
2.8.1. The image was changed to Airflow 2.10.4; do not reset or delete the
metadata database to work around a version mismatch.

After changing `Dockerfile.airflow`, rebuild and recreate the local Airflow
service:

```powershell
docker compose build airflow
docker compose up -d --no-deps airflow
docker compose exec -T airflow airflow db check
docker compose exec -T airflow airflow dags list-import-errors -o json
docker compose exec -T airflow airflow jobs check --job-type SchedulerJob
```

Push the changes to the default branch to trigger the Fly deployment, then
confirm the **Fly Deploy** workflow and the live health endpoint are healthy.

## 5. Separate scheduling from successful data loading

Airflow and GitHub Actions provide two scheduled attempts, but neither can
guarantee completion if CoinGecko, R2, Neon, GitHub Actions, or Fly.io is
unavailable. A manual **Run workflow** is a real pipeline run that may write
data; use it as a test only when those side effects are acceptable.
