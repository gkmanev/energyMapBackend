# Production ingestion recovery — 13 November 2026

Production: backend VM 192.168.1.11, ~/enMapBackend.

## Cause

The image combined Celery 5.3.6 / Kombu 5.7.0 with redis-py 5.0.3.
Kombu's Redis transport rejected the client version. Beat and worker were
crashing repeatedly (857 and 3,775 restarts observed). Redis and PostgreSQL
remained healthy. Direct ENTSO-E and EPİAŞ checks returned missing source data.

## Deployed changes

- Pin Kombu's Redis extra at 5.7.0 and redis-py at 6.4.0.
- Use a rolling 48-hour generation window to recover late previous-day data.
- Apply bounded ENTSO-E HTTP requests and transient-response retries to prices.
- Treat only EPİAŞ MCP error SEF1124 (tomorrow prices not published before
  14:00 Turkish time) as an empty publication. Other API errors still fail.

## Recovery and verification

- Available November 12–13 generation and prices backfilled for BG, DE, FR,
  RO, GR; Turkish prices and generation backfilled through EPİAŞ.
- Border flows recovered across 124 directed EIC pairs involving the five
  affected countries; 136 recovery jobs finished without unexpected failures.
- Germany's recovery explicitly aggregates its four generation zones; an
  initial recovery helper omitted aggregation and was corrected and rerun.
- Standard all-country hourly generation and price tasks completed successfully.
- Beat automatically dispatched tasks at 12:30, 12:34, and 12:35 Sofia, and
  the worker received them. Scheduled Turkish generation completed successfully.
- Six regression checks passed locally and inside the final production image.
- Final rollout completed around 12:47 Sofia. Web, Beat, and worker running;
  Beat and worker have zero restarts. The normal Turkish price task completed
  successfully in 2.07 seconds, including the unpublished-tomorrow case.
- Final public API check returns November 13 generation, prices, and flows
  for all five countries, plus Turkish generation and prices.

## Remaining source limitation

At the final check, BG generation ends at 02:00 UTC (04:00 Sofia), consistent
with the delayed ENTSO-E source. The auxiliary ESO endpoint at port 17999
returns historical records with newest records dated June 3, so it currently
cannot provide current Bulgarian snapshots. Its scheduled task remains enabled.
The redundant manual ESO replay was cancelled; its scheduled replay completed.

## Rollback material

Original source files are in .work/production-before locally, with additional
remote backups and build/backfill logs under
/tmp/entsoe-ingestion-repair-20261113 on the backend VM. Original EPİAŞ helper
is backed up there as entsoe_api__epias.py.before. No credentials are stored in
these notes. Changes are deployed in production and present in this checkout;
no Git commit was created.
