# FIMserve Viewer: offload the flood-map pipeline to Dask

## Goal

Stop sizing the fimserve **web** pods for the flood-map compute. Today the web
Deployment must be provisioned at roughly 12 Gi so the in-process HAND pipeline
does not OOM it, and every web replica carries that whether or not a generation
is running. After this change the web pod runs only Django plus job submit and
status polling (well under 1 Gi), and the memory heavy generation runs on a Dask
worker tier that scales to zero when idle.

This is a resource decoupling change, not a performance change. Per-job latency
is unchanged (the roughly 900 MB hydrofabric download dominates and is I/O
bound). The win is a small, always responsive web tier and a compute tier sized
and scaled independently.

## What already fits

- Job records live in a shared `jobs_db` persistent store (`job_store.py`,
  SQLAlchemy), so a worker on another pod can claim a job and report status.
- Results are already written to S3 as COGs (`results.store`, Django
  `default_storage`), so a worker returns results without shared local disk.
- The runner is a self contained module level function,
  `pipelines.run_nwm_pipeline(job, progress)` (and `run_custom_pipeline`), so it
  is importable and runnable on a worker with no web pod state.
- The sibling app fimeval already runs the same pattern in production
  (`tethys_sdk.jobs.DaskJob` plus a Dask scheduler), so the mechanism is proven.

The only web pinned piece is execution: `JobManager` runs the runner in a
`ThreadPoolExecutor` on the web process.

## Target architecture

```
browser --> web pod (Django)                jobs_db (RDS)        S3 (COGs)
             submit_job(): create row  --------^  ^------------------ |
             poll status  <---------------------/                     |
             dispatch to submitter                                    |
                 |                                                     |
                 v (prod)                                             |
             Dask scheduler  --adapt(0..N)-->  Dask worker pods ------/
                                               execute_job(): claim,
                                               heartbeat, run runner,
                                               finish
```

- **Web pod**: submits jobs and polls. Resources drop to about 256 Mi request,
  1 Gi limit.
- **Dask worker**: runs one generation, 8 to 12 Gi, with ephemeral disk for
  `FIMSERV_ROOT` (already LRU bounded by `manage_cache.py`). Scales 0 to N.
- **Dask scheduler**: small always on pod (or created by the operator).

Execution moves off the web pod but the coordination substrate (jobs_db, S3) and
the runner are unchanged.

## App code changes

The submission path is the only real code change. Keep one execution path shared
by every environment (DRY), and choose only *where* it runs.

### `execution.py` (new): run one job to completion, wherever it runs

Worker safe, no web pod state. Each function has one purpose.

- `execute_job(job_id, runner)`: claim the job, start its heartbeat, run the
  runner with a progress callback, finish success or error. This is the body of
  today's `JobManager.execute`, lifted to a module level function so a Dask
  worker can call it.
- `heartbeat(job_id, stop)`: refresh the running job's heartbeat until stopped.
  Replaces the replica wide loop; ownership now follows the worker that runs the
  job, and `mark_stale_interrupted` still recovers a dead worker's job.
- `worker_identity()`: moves here from `jobs.py` (it identifies the executor).

### `submitters.py` (new): choose where `execute_job` runs

- `ThreadSubmitter.submit(job_id, runner)`: run `execute_job` in a daemon
  thread. Used for local development and any deployment with no Dask scheduler.
- `DaskSubmitter.submit(job_id, runner)`: send `execute_job` to the Dask cluster
  and return immediately. Holds a `distributed.Client` from `scheduler.py`.
- `get_submitter()`: return a `DaskSubmitter` when a Dask scheduler is
  configured, otherwise a `ThreadSubmitter`. This is the single switch that
  keeps development working with no Dask.

### `scheduler.py` (new): Dask connection and adaptive scaling

- `dask_client()`: return a cached `distributed.Client` for the app's
  `dask_primary` scheduler, or `None` when none is configured. In production the
  scheduler runs adaptive worker scaling (see deploy), so the client only
  submits; it does not manage workers.
- `dask_available()`: whether a Dask scheduler is configured. Used by
  `get_submitter`.

### `jobs.py` (reduced): submission orchestration only

- `submit_job(kind, huc8, key, params, runner)`: dedupe and create the job row,
  then hand it to `get_submitter().submit(...)`. This replaces
  `JobManager.submit`.
- `active_for_huc(huc8)` and `get(job_id)`: thin reads, unchanged behavior.

`JobManager`, its `ThreadPoolExecutor`, the process wide singleton, and the
replica wide heartbeat are removed. `job_controllers.py` calls `submit_job(...)`
in place of `get_job_manager().submit(...)`; the arguments are identical.

### `app.py`: declare the scheduler

Add `scheduler_settings()` returning one `SchedulerSetting(name='dask_primary',
engine=SchedulerSetting.DASK, ...)`, mirroring fimeval. It is not `required`, so
the app still loads in development with no scheduler assigned (then
`get_submitter` picks the thread path).

## Deploy changes: adaptive Dask tier

Two ways to get workers that scale to zero. Both keep the web pod small; they
differ in how workers are created and torn down.

### Recommended: KEDA scaled worker Deployment on a static scheduler

Reuses the portal's existing KEDA and fimeval's static Dask scheduler pattern,
adds no cluster operator.

- A small Dask **scheduler** Deployment plus Service (the `dask_primary`
  scheduler the app connects to).
- A Dask **worker** Deployment (8 to 12 Gi, ephemeral `FIMSERV_ROOT` volume,
  the fimserve image with a worker command) fronted by a KEDA `ScaledObject`
  with `minReplicaCount: 0`. The trigger is the count of queued or running rows
  in `jobs_db` (KEDA postgresql scaler), so a worker appears when a job is
  queued and the tier returns to zero when the queue drains.
- Web Deployment resources drop to about 256 Mi request, 1 Gi limit.

### Alternative: Dask native adaptive via the dask-kubernetes operator

`KubeCluster(...).adapt(minimum=0, maximum=N)` lets the Dask scheduler create and
delete worker pods itself. This is Dask's own adaptive scaling and needs no
external queue metric, but it requires installing the dask-kubernetes operator
and granting the app service account RBAC to manage `DaskCluster` resources.
Prefer this only if adding the operator is acceptable.

Either way the fimserve image is reused for the worker (same code, worker
command), so teehr, gdal, and FIMserv are already present.

## Local development

- No Dask scheduler is assigned to `dask_primary`, so `get_submitter` returns
  the `ThreadSubmitter` and generation runs in a thread exactly as today.
- Optionally set an env flag to run against a `distributed.LocalCluster` for a
  realistic local Dask path without Kubernetes; default stays the thread path so
  `tethys manage start` needs nothing extra.
- The jobs_db sqlite dev service and the local `FIMSERV_ROOT` cache are
  unchanged.

## Rollout

1. Land the app code with the thread path as default; behavior is identical with
   no scheduler, so it is safe to merge before any Dask infra exists.
2. Deploy the scheduler plus KEDA scaled worker; assign the `dask_primary`
   scheduler to the app setting.
3. Drop the web Deployment resources.
4. Verify: submit a generation, watch a worker scale up, the COG land in S3, the
   job row finish, and the worker scale back to zero; confirm the web pod stays
   well under 1 Gi throughout.

## Out of scope

Parallelizing a single generation, changing the FIMserv pipeline, and sharing
the hydrofabric cache across workers. Each worker keeps its own bounded cache.
