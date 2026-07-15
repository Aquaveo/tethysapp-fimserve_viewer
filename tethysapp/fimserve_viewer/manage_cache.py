"""
manage_cache.py - Hydrofabric disk-cache management for the FIMserve Viewer.

A first-time HUC8 download is ~700-900 MB, almost all of it hydrofabric
internals (branches/, hydrotable.csv, ...) that are only needed while the
inundation step runs. The generated tif is ~2 MB. To keep a small server
from filling up, we cap the total hydrofabric footprint: before each new
download, the least-recently-used HUCs' heavy internals are deleted until
the total fits FIMSERVE_CACHE_MAX_GB. Small sidecar files that other
endpoints read afterwards (boundary/streams gpkg, branch_ids.csv) and the
inundation tifs are always kept. `aws s3 sync` in DownloadHUC8 re-fetches
only missing files, so an evicted HUC self-heals on its next request.
"""

import os
import shutil
from pathlib import Path
from typing import Optional

_HYDROFABRIC_KEEP_FILES = {
    "wbd.gpkg",                        # HUC boundary (preview masking)
    "nwm_subset_streams.gpkg",         # streams (Q-label endpoint)
    "nwm_catchments_proj_subset.gpkg", # boundary fallback (preview masking)
    "branch_ids.csv",
}


def _candidate_roots() -> list:
    # Imported lazily: fim_logic imports this module at load time, so a
    # module-level import here would be circular.
    from .fim_logic import _candidate_fimserv_roots

    return _candidate_fimserv_roots()


def _cache_max_bytes() -> int:
    """FIMSERVE_CACHE_MAX_GB as bytes. <= 0 (or unparsable) disables eviction."""
    raw = os.environ.get("FIMSERVE_CACHE_MAX_GB", "3")
    try:
        gb = float(raw)
    except ValueError:
        print(
            f"[fimserve_viewer] Ignoring unparsable FIMSERVE_CACHE_MAX_GB={raw!r}",
            flush=True,
        )
        return 0
    return int(gb * 1024**3) if gb > 0 else 0


def _tree_size_bytes(path: Path) -> int:
    if path.is_file():
        try:
            return path.stat().st_size
        except OSError:
            return 0
    total = 0
    for root, _dirs, files in os.walk(path):
        for name in files:
            try:
                total += os.path.getsize(os.path.join(root, name))
            except OSError:
                pass
    return total


def prune_huc_hydrofabric(huc8: str) -> int:
    """Delete one HUC8's heavy hydrofabric internals; keep the small sidecars.

    Sweeps every candidate root (pre-patch FIMserv runs may have written to
    the portal cwd). Returns bytes freed.
    """
    freed = 0
    for root in _candidate_roots():
        huc_dir = root / "output" / f"flood_{huc8}" / huc8
        if not huc_dir.is_dir():
            continue
        for child in huc_dir.iterdir():
            if child.name in _HYDROFABRIC_KEEP_FILES:
                continue
            size = _tree_size_bytes(child)
            try:
                if child.is_dir():
                    shutil.rmtree(child)
                else:
                    child.unlink()
            except OSError as exc:
                print(
                    f"[fimserve_viewer] Could not prune {child}: {exc}", flush=True
                )
                continue
            freed += size
    if freed:
        print(
            f"[fimserve_viewer] Pruned hydrofabric for HUC {huc8}: "
            f"freed {freed / 1024**2:.0f} MiB",
            flush=True,
        )
    return freed


def enforce_cache_budget(protect_huc: Optional[str] = None) -> None:
    """Evict least-recently-used HUC hydrofabric until under the cache cap.

    Call before starting a new HUC download so the ~1 GB it needs fits the
    budget. `protect_huc` (the HUC about to be used) is never evicted.
    """
    cap = _cache_max_bytes()
    if cap <= 0:
        return

    stats: dict = {}
    for root in _candidate_roots():
        out_dir = root / "output"
        if not out_dir.is_dir():
            continue
        for flood_dir in out_dir.glob("flood_*"):
            huc8 = flood_dir.name[len("flood_"):]
            huc_dir = flood_dir / huc8
            if not huc8.isdigit() or not huc_dir.is_dir():
                continue
            heavy = sum(
                _tree_size_bytes(c)
                for c in huc_dir.iterdir()
                if c.name not in _HYDROFABRIC_KEEP_FILES
            )
            recency = flood_dir.stat().st_mtime
            for sub in flood_dir.iterdir():
                try:
                    recency = max(recency, sub.stat().st_mtime)
                except OSError:
                    pass
            entry = stats.setdefault(huc8, {"bytes": 0, "recency": 0.0})
            entry["bytes"] += heavy
            entry["recency"] = max(entry["recency"], recency)

    total = sum(e["bytes"] for e in stats.values())
    if total <= cap:
        return

    print(
        f"[fimserve_viewer] Hydrofabric cache {total / 1024**3:.1f} GiB over "
        f"{cap / 1024**3:.1f} GiB cap; evicting least-recently-used HUCs",
        flush=True,
    )
    for huc8, entry in sorted(stats.items(), key=lambda kv: kv[1]["recency"]):
        if protect_huc is not None and huc8 == str(protect_huc):
            continue
        total -= prune_huc_hydrofabric(huc8)
        if total <= cap:
            break


__all__ = [
    "enforce_cache_budget",
    "prune_huc_hydrofabric",
]
