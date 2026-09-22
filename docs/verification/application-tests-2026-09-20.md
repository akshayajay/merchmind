# Application-test evidence — September 20, 2026

This preserves the test/coverage summary from GitHub Actions so the figures remain
reviewable after the hosted logs expire. It is an observed result for the revision
below, not a new run or a live coverage badge. The excerpt was retrieved from the
Actions log on September 22, 2026; job prefixes and timestamps were removed.

- Tested commit: [`2f0c281ca0f7ec8c7802aa41f7ad0aa8f4d695f7`](https://github.com/akshayajay/merchmind/commit/2f0c281ca0f7ec8c7802aa41f7ad0aa8f4d695f7), [PR #3](https://github.com/akshayajay/merchmind/pull/3).
- Source: [Actions run 35542253563](https://github.com/akshayajay/merchmind/actions/runs/35542253563), `test` job, `Run pytest` step.
- Output timestamp: September 20, 2026, 22:38:33 UTC.
- Command: `pytest`, on Linux with Python 3.11.16.
- Result: **38 passed, 1 skipped; 88.61% statement coverage** (817 covered of 922 measured statements).

Coverage is scoped to the `merchmind` Python package through `--cov=merchmind`.
The [configuration at the tested revision](https://github.com/akshayajay/merchmind/blob/2f0c281ca0f7ec8c7802aa41f7ad0aa8f4d695f7/pyproject.toml)
omits `__main__.py`, `cli.py`, `dashboard.py`, and `warehouse.py`. This percentage
does not cover every repository file, the standalone Spark jobs, or the Airflow DAG.
The live broker/Spark test is skipped in this application job and runs separately
in the `streaming` job; Airflow also has its own job. The coverage threshold is 80%.
The table rounds total coverage to 89%; the final line reports 88.61%.

## Captured output

```text
================================ tests coverage ================================
_______________ coverage: platform linux, python 3.11.16-final-0 _______________

Name                                      Stmts   Miss  Cover   Missing
-----------------------------------------------------------------------
src/merchmind/__init__.py                     2      0   100%
src/merchmind/analytics.py                   65      0   100%
src/merchmind/api.py                         71     13    82%   18, 51, 70, 88, 93-102
src/merchmind/backtest.py                    59      0   100%
src/merchmind/close.py                      171     21    88%   71, 88, 149, 181, 183, 185, 257-270, 274
src/merchmind/config.py                      25      0   100%
src/merchmind/connectors/__init__.py          2      0   100%
src/merchmind/connectors/public_data.py      39      7    82%   36, 49, 51, 59, 68-69, 73
src/merchmind/inventory.py                  118     29    75%   32, 63, 68, 71, 115, 160, 165-194, 198
src/merchmind/live.py                       128     18    86%   69, 75, 179-193, 197
src/merchmind/pipeline.py                    61      1    98%   143
src/merchmind/quality.py                     36      0   100%
src/merchmind/streaming.py                   62     16    74%   28-29, 43, 72, 76, 87-108, 112
src/merchmind/synthetic.py                   83      0   100%
-----------------------------------------------------------------------
TOTAL                                       922    105    89%
Required test coverage of 80% reached. Total coverage: 88.61%
38 passed, 1 skipped, 2 warnings in 18.70s
```
