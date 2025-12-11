"""Command line entrypoint for solarpredict.

Implements two primary commands:

* ``run``: execute a daily simulation from a scenario config file.
* ``config``: interactive helper to build/edit scenario configs.

The CLI is intentionally lightweight and depends only on Typer (Click).
"""
from __future__ import annotations

import datetime as dt
import json
from dataclasses import asdict
from pathlib import Path
from typing import Callable, Dict, List, Optional, Tuple

import pandas as pd
import typer
import yaml

from solarpredict.core.config import ConfigError, load_scenario
from solarpredict.core import config as config_mod
from solarpredict.core.debug import JsonlDebugWriter, NullDebugCollector
from solarpredict.core.models import Location, PVArray, Scenario, Site, ValidationError
from solarpredict.engine.simulate import apply_actual_adjustment, simulate_day
from solarpredict.weather.open_meteo import OpenMeteoWeatherProvider
from solarpredict.weather.pvgis import PVGISWeatherProvider
from solarpredict.weather.composite import CompositeWeatherProvider
from solarpredict.integrations import ha_mqtt
from solarpredict.cli_config_tui import launch_config_tui
from solarpredict.cli_utils import write_scenario, load_existing, load_mqtt, load_run
from solarpredict.cli_config_tui import launch_config_tui

__version__ = "0.1.0"

app = typer.Typer(add_completion=False, help="Solar generation predictor CLI")


def default_weather_provider(debug) -> OpenMeteoWeatherProvider:
    """Factory separated for easy monkeypatching in tests."""

    return OpenMeteoWeatherProvider(debug=debug)


def _exit_with_error(msg: str) -> None:
    typer.echo(f"Error: {msg}", err=True)
    raise typer.Exit(code=1)


def _write_scenario(path: Path, scenario: Scenario) -> None:
    try:
        write_scenario(path, scenario)
    except ConfigError as exc:
        _exit_with_error(str(exc))


def _prompt_location(existing: Location | None = None) -> Location:
    lat = float(
        typer.prompt(
            "Latitude", default=str(existing.lat) if existing else "0.0"
        )
    )
    lon = float(
        typer.prompt(
            "Longitude", default=str(existing.lon) if existing else "0.0"
        )
    )
    tz = typer.prompt("Timezone", default=existing.tz if existing else "auto")
    elev_raw = typer.prompt(
        "Elevation meters (blank to skip)", default=str(existing.elevation_m) if existing and existing.elevation_m is not None else ""
    )
    elevation_m = float(elev_raw) if elev_raw.strip() else None
    loc_id = typer.prompt("Location id", default=existing.id if existing else "loc1")
    try:
        return Location(id=loc_id, lat=lat, lon=lon, tz=tz, elevation_m=elevation_m)
    except ValidationError as exc:  # pragma: no cover - validated via CLI tests
        _exit_with_error(str(exc))


def _prompt_array(existing: PVArray | None = None) -> PVArray:
    def prompt_float(label: str, default: float) -> float:
        return float(typer.prompt(label, default=str(default)))
    def prompt_optional_float(label: str, default: float | None) -> float | None:
        raw = typer.prompt(label, default="" if default is None else str(default))
        return float(raw) if str(raw).strip() else None

    arr_id = typer.prompt("Array id", default=existing.id if existing else "array1")
    tilt_deg = prompt_float("Tilt deg", existing.tilt_deg if existing else 30.0)
    azimuth_deg = prompt_float("Azimuth deg", existing.azimuth_deg if existing else 0.0)
    pdc0_w = prompt_float("pdc0_w", existing.pdc0_w if existing else 5000.0)
    gamma_pdc = prompt_float("gamma_pdc", existing.gamma_pdc if existing else -0.004)
    dc_ac_ratio = prompt_float("dc_ac_ratio", existing.dc_ac_ratio if existing else 1.1)
    eta_inv_nom = prompt_float("eta_inv_nom", existing.eta_inv_nom if existing else 0.96)
    losses_percent = prompt_float("losses_percent", existing.losses_percent if existing else 5.0)
    temp_model = typer.prompt("temp_model", default=existing.temp_model if existing else "close_mount_glass_glass")
    inverter_group_id = typer.prompt(
        "Inverter group id (blank = none)", default=existing.inverter_group_id if existing else ""
    ).strip() or None
    inverter_pdc0_w = prompt_optional_float(
        "Inverter pdc0_w (blank = derive)", existing.inverter_pdc0_w if existing else None
    )
    horizon_raw = typer.prompt(
        "Horizon profile (CSV of deg, blank = none)",
        default=",".join(str(v) for v in existing.horizon_deg) if existing and existing.horizon_deg else "",
    ).strip()
    horizon = [float(v) for v in horizon_raw.split(",") if v.strip()] if horizon_raw else None
    try:
        return PVArray(
            id=arr_id,
            tilt_deg=tilt_deg,
            azimuth_deg=azimuth_deg,
            pdc0_w=pdc0_w,
            gamma_pdc=gamma_pdc,
            dc_ac_ratio=dc_ac_ratio,
            eta_inv_nom=eta_inv_nom,
            losses_percent=losses_percent,
            temp_model=temp_model,
            inverter_group_id=inverter_group_id,
            inverter_pdc0_w=inverter_pdc0_w,
            horizon_deg=horizon,
        )
    except ValidationError as exc:  # pragma: no cover - validated via CLI tests
        _exit_with_error(str(exc))


def _prompt_site(existing: Site | None = None) -> Site:
    site_id = typer.prompt("Site id", default=existing.id if existing else "site1")
    location = _prompt_location(existing.location if existing else None)

    arrays: List[PVArray] = list(existing.arrays) if existing else []
    if not arrays:
        typer.echo("Add at least one array")
    while True:
        if not arrays or typer.confirm("Add array?", default=not arrays):
            arrays.append(_prompt_array())
            continue
        if typer.confirm("Edit an existing array?", default=False):
            ids = [arr.id for arr in arrays]
            choice = typer.prompt("Array id to edit", default=ids[0])
            for idx, arr in enumerate(arrays):
                if arr.id == choice:
                    arrays[idx] = _prompt_array(arr)
                    break
        if typer.confirm("Delete an array?", default=False):
            ids = [arr.id for arr in arrays]
            choice = typer.prompt("Array id to delete", default=ids[0])
            arrays = [arr for arr in arrays if arr.id != choice]
        # stop when user declines further changes
        if not typer.confirm("Modify arrays again?", default=False):
            break

    try:
        return Site(id=site_id, location=location, arrays=arrays)
    except ValidationError as exc:  # pragma: no cover - validated via CLI tests
        _exit_with_error(str(exc))


def _load_existing(path: Path) -> List[Site]:
    try:
        sites = load_existing(path)
        if sites:
            typer.echo(f"Loaded existing scenario with {len(sites)} site(s)")
        return sites
    except ConfigError as exc:
        typer.echo(f"Could not load existing config: {exc}", err=True)
        return []


@app.command()
def run(
    config: Path = typer.Option(Path("etc/config.yaml"), exists=True, readable=True, help="Scenario YAML/JSON file"),
    date: str = typer.Option(..., help="Target date (YYYY-MM-DD)"),
    timestep: Optional[str] = typer.Option(
        None,
        help="Forecast timestep, e.g. 1h or 15m. Defaults to run.timestep in config, else 1h.",
    ),
    weather_label: str = typer.Option(
        "end",
        help="Meaning of weather timestamps: 'end' (backward-averaged, default), 'start' (forward-averaged), or 'center'.",
    ),
    weather_source: str = typer.Option(
        "open-meteo",
        help="Weather provider: 'open-meteo' (default), 'pvgis-tmy' (typical meteorological year), or 'composite' (open-meteo primary with PVGIS fallback).",
    ),
    weather_mode: Optional[str] = typer.Option(
        None,
        help="Weather processing mode: 'standard' (use provider irradiance) or 'cloud-scaled' (clear-sky scaled by cloud cover). Defaults to run.weather_mode or 'standard'.",
    ),
    pvgis_cache_dir: Optional[Path] = typer.Option(
        None,
        help="Directory to cache PVGIS TMY responses (keyed by lat/lon). Only used when --weather-source=pvgis-tmy.",
    ),
    qc_pvgis: Optional[bool] = typer.Option(
        None,
        help="Compare forecast against PVGIS TMY baseline (sanity check). If omitted, falls back to config run.qc_pvgis (default false).",
    ),
    actual_kwh_today: Optional[float] = typer.Option(
        None,
        help="Observed energy for the day so far (kWh). Scales remaining intervals so forecast aligns with actuals.",
    ),
    actual_limit_suppress: Optional[bool] = typer.Option(
        None,
        help="Suppress output when applying actual adjustment fails validation (legacy limit=0 behaviour).",
    ),
    actual_as_of: Optional[str] = typer.Option(
        None,
        help="Timestamp (ISO) representing 'now' for actual scaling; defaults to current time clamped to simulation window.",
    ),
    base_load_w: Optional[float] = typer.Option(
        None,
        help="Base load (W) to detect windows where pac_net_w exceeds this threshold (per site, summed across arrays).",
    ),
    min_duration_min: Optional[float] = typer.Option(
        None,
        help="Minimum window duration in minutes for load window detection. Required when base_load_w is set.",
    ),
    required_wh: Optional[float] = typer.Option(
        None,
        help="Optional energy requirement (Wh) a window must satisfy to qualify.",
    ),
    debug: Optional[Path] = typer.Option(None, help="Write debug JSONL to this path"),
    format: str = typer.Option("json", "--format", "-f", help="Output format: json or csv"),
    output: Optional[Path] = typer.Option(None, help="Output file path; defaults to results.<format>"),
    intervals: Optional[Path] = typer.Option(
        None,
        help="Optional per-interval output (.json or .csv) with pac_net_w, poa_global, interval_h, wh_period, wh_cum",
    ),
    force: bool = typer.Option(False, "--force", help="Run even if output already generated today"),
):
    """Run a daily simulation for the provided scenario."""

    raw_cfg = None
    try:
        raw_cfg = config_mod._load_raw(config)  # type: ignore[attr-defined] - internal helper is fine here
        scenario = load_scenario(config)
    except ConfigError as exc:
        _exit_with_error(str(exc))

    try:
        date_obj = dt.date.fromisoformat(date)
    except ValueError:
        _exit_with_error("date must be YYYY-MM-DD")

    debug_collector = JsonlDebugWriter(debug) if debug else NullDebugCollector()
    output_path = output or Path(f"results.{format}")

    if not force and output_path.exists():
        existing = None
        try:
            existing = json.loads(output_path.read_text())
        except json.JSONDecodeError:
            existing = None  # unreadable; ignore guard

        generated_at = None
        payload_date = None
        if isinstance(existing, dict):
            if "meta" in existing:
                meta = existing.get("meta", {}) or {}
                generated_at = meta.get("generated_at")
                payload_date = meta.get("date") or existing.get("date")
            else:
                generated_at = existing.get("generated_at")
                payload_date = existing.get("date")
        # Flat list has no generated_at; skip guard.

        if generated_at and payload_date == date:
            gen_dt = dt.datetime.fromisoformat(generated_at)
            if gen_dt.date() == dt.date.today():
                typer.echo(
                    f"Output {output_path} for {payload_date} already generated today ({generated_at}); use --force to rerun."
                )
                raise typer.Exit(code=0)

    weather_source = weather_source.lower()
    run_section = raw_cfg.get("run", {}) if raw_cfg else {}

    # Resolve weather_mode with config fallback.
    effective_weather_mode = weather_mode or run_section.get("weather_mode") or "standard"
    weather_mode = effective_weather_mode.lower()
    if weather_source == "open-meteo":
        provider = default_weather_provider(debug=debug_collector)
    elif weather_source == "pvgis-tmy":
        provider = PVGISWeatherProvider(debug=debug_collector, cache_dir=pvgis_cache_dir)
    elif weather_source == "composite":
        primary = default_weather_provider(debug=debug_collector)
        secondary = PVGISWeatherProvider(debug=debug_collector, cache_dir=pvgis_cache_dir)
        provider = CompositeWeatherProvider(primary=primary, secondary=secondary, debug=debug_collector)
    else:
        _exit_with_error(f"Unsupported weather_source '{weather_source}'")

    if weather_mode not in {"standard", "cloud-scaled"}:
        _exit_with_error("weather_mode must be 'standard' or 'cloud-scaled'")

    effective_timestep = (
        timestep
        or run_section.get("timestep")
        or "1h"
    )

    # Optional load-window inputs (per-site aggregation of pac_net_w)
    cfg_base_load = run_section.get("base_load_w")
    cfg_min_duration = run_section.get("min_duration_min")
    cfg_required_wh = run_section.get("required_wh")

    effective_base_load = base_load_w if base_load_w is not None else cfg_base_load
    effective_min_duration = min_duration_min if min_duration_min is not None else cfg_min_duration
    effective_required_wh = required_wh if required_wh is not None else cfg_required_wh

    if effective_base_load is not None:
        try:
            effective_base_load = float(effective_base_load)
        except Exception:
            _exit_with_error("base_load_w must be numeric")
        if effective_base_load <= 0:
            _exit_with_error("base_load_w must be positive")
        if effective_min_duration is None:
            _exit_with_error("min_duration_min is required when base_load_w is provided")
        try:
            effective_min_duration = float(effective_min_duration)
        except Exception:
            _exit_with_error("min_duration_min must be numeric")
        if effective_min_duration <= 0:
            _exit_with_error("min_duration_min must be positive")
        if effective_required_wh is not None:
            try:
                effective_required_wh = float(effective_required_wh)
            except Exception:
                _exit_with_error("required_wh must be numeric")
            if effective_required_wh <= 0:
                _exit_with_error("required_wh must be positive when provided")

    result = simulate_day(
        scenario,
        date=date_obj,
        timestep=effective_timestep,
        weather_provider=provider,
        debug=debug_collector,
        weather_label=weather_label,
        weather_mode=weather_mode,
    )

    # Optional actual adjustment
    actual_cfg = run_section.get("actual_kwh_today") if run_section else None
    suppress_cfg = run_section.get("actual_limit_suppress") if run_section else None
    asof_cfg = run_section.get("actual_as_of") if run_section else None
    effective_actual = actual_kwh_today if actual_kwh_today is not None else actual_cfg
    suppress_flag = actual_limit_suppress if actual_limit_suppress is not None else suppress_cfg
    as_of_ts = actual_as_of if actual_as_of is not None else asof_cfg
    parsed_as_of = None
    if as_of_ts:
        try:
            parsed_as_of = pd.Timestamp(as_of_ts)
        except Exception as exc:
            debug_collector.emit("actual.adjust.error", {"error": f"invalid actual_as_of: {exc}"}, ts=date_obj)
            if suppress_flag:
                typer.echo("Invalid actual_as_of; suppressing output per limit flag", err=True)
                raise typer.Exit(code=1)

    if effective_actual is not None:
        try:
            result = apply_actual_adjustment(result, effective_actual, debug_collector, now_ts=parsed_as_of)
        except Exception as exc:
            debug_collector.emit("actual.adjust.error", {"error": str(exc)}, ts=date_obj)
            if suppress_flag:
                typer.echo("Actual adjustment failed; suppressing output per limit flag", err=True)
                raise typer.Exit(code=1)

    # Optional PVGIS QC sanity check: compare forecast energy/POA vs climatology.
    qc_enabled = qc_pvgis if qc_pvgis is not None else run_section.get("qc_pvgis", False)
    if qc_enabled:
        qc_provider = PVGISWeatherProvider(debug=debug_collector, cache_dir=pvgis_cache_dir)
        # Run a baseline simulation with PVGIS (hourly only) to get comparable POA energy per m².
        qc_result = simulate_day(
            scenario,
            date=date_obj,
            timestep="1h",
            weather_provider=qc_provider,
            debug=debug_collector,
            weather_label=weather_label,
        )

        # Map (site,array) -> PVGIS POA kWh/m² for easy join back onto the main forecast output.
        pvgis_poa_map = {}
        for _, row in qc_result.daily.iterrows():
            try:
                pvgis_poa_map[(row["site"], row["array"])] = float(row["poa_kwh_m2"])
            except Exception:
                continue

        # Emit ratios for visibility and keep prior warning logic, now comparing POA instead of GHI proxy.
        warnings = []
        for _, row in result.daily.iterrows():
            key = (row["site"], row["array"])
            forecast_poa = float(row.get("poa_kwh_m2", 0) or 0)
            baseline_poa = pvgis_poa_map.get(key)
            ratio = float(forecast_poa / baseline_poa) if baseline_poa else float("inf")
            debug_collector.emit(
                "qc.pvgis_compare",
                {"site": key[0], "array": key[1], "ratio": ratio, "baseline_poa_kwh_m2": baseline_poa},
                ts=date_obj,
            )
            # Heuristic band: allow wider when cloudy (low POA).
            cloudy = forecast_poa < 0.6  # kWh/m2 per day rough cloud marker
            low, high = (0.3, 2.0) if cloudy else (0.6, 1.6)
            if baseline_poa is not None and (ratio < low or ratio > high):
                warnings.append(
                    f"{key[0]}/{key[1]} PVGIS POA ratio {ratio:.2f} outside [{low},{high}] (cloudy={cloudy})"
                )

        # Attach PVGIS POA baseline onto the main daily output so MQTT can publish it.
        daily_with_pvgis = result.daily.copy()
        daily_with_pvgis["pvgis_poa_kwh_m2"] = daily_with_pvgis.apply(
            lambda row: pvgis_poa_map.get((row["site"], row["array"])), axis=1
        )
        # Clamp implausible POA/energy vs PVGIS (0.6x–1.6x) now that the baseline is attached.
        capped_rows = []
        for _, row in daily_with_pvgis.iterrows():
            pvgis = row.get("pvgis_poa_kwh_m2")
            poa = row.get("poa_kwh_m2")
            if pvgis is None or poa is None or pd.isna(pvgis) or pd.isna(poa) or pvgis <= 0:
                capped_rows.append(row)
                continue
            ratio = poa / pvgis
            if 0.6 <= ratio <= 1.6:
                capped_rows.append(row)
                continue
            cap = 1.6 if ratio > 1.6 else 0.6
            scale = cap / ratio if ratio != 0 else 1.0
            row = row.copy()
            row["poa_kwh_m2"] = poa * scale
            row["energy_kwh"] = row["energy_kwh"] * scale
            row["peak_kw"] = row["peak_kw"] * scale
            row["qc_clipped"] = True
            row["qc_ratio"] = ratio
            capped_rows.append(row)
        daily_with_pvgis = pd.DataFrame(capped_rows)

        result = type(result)(daily=daily_with_pvgis, timeseries=result.timeseries)

        for w in warnings:
            typer.echo(f"QC warning: {w}", err=True)

    daily = result.daily

    load_windows = None
    if effective_base_load is not None:
        from solarpredict.engine.load_window import compute_load_windows

        load_windows = compute_load_windows(
            result.timeseries,
            base_load_w=effective_base_load,
            min_duration_min=effective_min_duration,
            required_wh=effective_required_wh,
            debug=debug_collector,
        )

    fmt = format.lower()
    if fmt == "json":
        serializable = daily.copy()
        for col in serializable.columns:
            if str(serializable[col].dtype).startswith("datetime64"):
                serializable[col] = serializable[col].astype(str)
        payload = serializable.to_dict(orient="records")
        if load_windows is not None:
            payload = {"results": payload, "load_windows": load_windows}
        output_path.write_text(json.dumps(payload, indent=2))
    elif fmt == "csv":
        daily.to_csv(output_path, index=False)
    else:
        _exit_with_error("format must be json or csv")

    # Optional per-interval export
    if intervals:
        intervals_path = intervals
        intervals_fmt = intervals_path.suffix.lower().lstrip(".")
        intervals_df = _build_intervals_df(result.timeseries)
        if intervals_fmt == "json":
            serializable = intervals_df.copy()
            for col in serializable.columns:
                if str(serializable[col].dtype).startswith("datetime64"):
                    serializable[col] = serializable[col].astype(str)
            intervals_path.write_text(serializable.to_json(orient="records", indent=2))
        elif intervals_fmt == "csv":
            intervals_df.to_csv(intervals_path, index=False)
        else:
            _exit_with_error("intervals path must end with .json or .csv")
        typer.echo(f"Wrote interval data to {intervals_path} ({len(intervals_df)} rows)")

    typer.echo(daily.to_string(index=False))
    typer.echo(f"Wrote results to {output_path}")
    if debug:
        typer.echo(f"Debug events -> {debug}")


def _list_sites(sites: List[Site]) -> None:
    if not sites:
        typer.echo("No sites configured")
        return
    for site in sites:
        typer.echo(f"- {site.id}: {site.location.lat},{site.location.lon} tz={site.location.tz} arrays={len(site.arrays)}")


def _build_intervals_df(timeseries: Dict[Tuple[str, str], pd.DataFrame]) -> pd.DataFrame:
    """Flatten per-array timeseries into a single dataframe with Wh metrics."""
    frames: List[pd.DataFrame] = []
    for (site_id, array_id), df in timeseries.items():
        if df is None or df.empty:
            continue
        ts_df = df.sort_index().copy()
        ts_df["wh_period"] = (ts_df["pac_net_w"] * ts_df["interval_h"]).astype(float)
        ts_df["wh_cum"] = ts_df["wh_period"].cumsum()
        ts_df["site"] = site_id
        ts_df["array"] = array_id
        ts_df["ts"] = ts_df.index
        frames.append(
            ts_df[
                [
                    "site",
                    "array",
                    "ts",
                    "pac_net_w",
                    "poa_global",
                    "interval_h",
                    "wh_period",
                    "wh_cum",
                ]
            ]
        )
    if not frames:
        return pd.DataFrame(
            columns=["site", "array", "ts", "pac_net_w", "poa_global", "interval_h", "wh_period", "wh_cum"]
        )
    return pd.concat(frames)


@app.command()
def config(
    path: Path = typer.Argument(..., help="Path to save scenario YAML/JSON"),
    no_tui: bool = typer.Option(False, help="Use legacy prompt mode instead of TUI"),
    debug: Optional[Path] = typer.Option(None, help="Write TUI debug JSONL"),
):
    """Interactive scenario builder/editor.

    Default mode launches a prompt_toolkit TUI. Pass --no-tui to use the legacy
    prompt-driven flow (useful for non-interactive scripts/tests).
    """

    if not no_tui:
        launch_config_tui(path, debug_path=debug)
        return

    sites = _load_existing(path)

    while True:
        action = typer.prompt(
            "Choose action [a=add site, e=edit site, d=delete site, l=list, s=save]",
            default="s" if sites else "a",
        ).strip().lower()

        if action == "a":
            sites.append(_prompt_site())
            continue

        if action == "e":
            if not sites:
                typer.echo("No sites to edit")
                continue
            _list_sites(sites)
            target = typer.prompt("Site id to edit", default=sites[0].id)
            for idx, site in enumerate(sites):
                if site.id == target:
                    sites[idx] = _prompt_site(site)
                    break
            else:
                typer.echo(f"Site {target} not found")
            continue

        if action == "d":
            if not sites:
                typer.echo("No sites to delete")
                continue
            _list_sites(sites)
            target = typer.prompt("Site id to delete", default=sites[0].id)
            sites = [s for s in sites if s.id != target]
            continue

        if action == "l":
            _list_sites(sites)
            continue

        if action == "s":
            try:
                scenario = Scenario(sites=sites)
            except ValidationError as exc:
                typer.echo(f"Invalid scenario: {exc}")
                continue
            _write_scenario(path, scenario)
            typer.echo(f"Saved scenario to {path}")
            return

        typer.echo("Unknown action; choose a/e/d/l/s")


@app.callback()
def version_callback(
    version: bool = typer.Option(False, "--version", help="Show version and exit"),
):
    if version:
        typer.echo(__version__)
        raise typer.Exit()


@app.command("publish-mqtt")
def publish_mqtt(
    config: Path = typer.Option(Path("etc/config.yaml"), help="Combined scenario + mqtt config"),
    input: Path = typer.Option(Path("live_results.json"), "--input", "-i", help="Forecast JSON produced by cron/run"),
    mqtt_host: str = typer.Option(None, help="Override MQTT host"),
    mqtt_port: int = typer.Option(None, help="Override MQTT port"),
    mqtt_username: str = typer.Option(None, help="Override MQTT username"),
    mqtt_password: str = typer.Option(None, help="Override MQTT password"),
    base_topic: str = typer.Option(None, help="Override base topic"),
    discovery_prefix: str = typer.Option(None, help="Override discovery prefix"),
    connect_retries: int = typer.Option(None, help="MQTT connection retries"),
    retry_delay: float = typer.Option(None, help="Delay between retries in seconds"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Enable chatty logging"),
    force: bool = typer.Option(False, "--force", help="Publish even if unchanged or older"),
    no_state: bool = typer.Option(False, "--no-state", help="Skip retained state blob"),
    publish_topics: Optional[bool] = typer.Option(
        None,
        "--publish-topics/--no-publish-topics",
        help="Also publish scalar topics (retained). Defaults to config.",
        show_default=False,
    ),
    verify: bool = typer.Option(False, "--verify", help="Read back retained state and ensure it matches"),
    publish_retries: int = typer.Option(1, "--publish-retries", help="Retry publish+verify N times on failure"),
    no_discovery: bool = typer.Option(False, "--no-discovery", help="Skip HA discovery publish"),
    skip_if_fresh: bool = typer.Option(False, "--skip-if-fresh", help="If broker already has same/newer generated_at, do nothing (state path only)"),
):
    """Publish forecast JSON to MQTT using the same config format as the simulator."""

    # Reuse argparse-based merger by mimicking the Namespace shape.
    args = type(
        "Args",
        (),
        {
            "config": config,
            "input": input,
            "mqtt_host": mqtt_host,
            "mqtt_port": mqtt_port,
            "mqtt_username": mqtt_username,
            "mqtt_password": mqtt_password,
            "base_topic": base_topic,
            "discovery_prefix": discovery_prefix,
            "connect_retries": connect_retries,
            "retry_delay": retry_delay,
            "verbose": verbose,
            "force": force,
            "no_state": no_state,
            "publish_topics": publish_topics,
            "verify": verify,
            "publish_retries": publish_retries,
            "publish_discovery": not no_discovery,
            "skip_if_fresh": skip_if_fresh,
        },
    )()

    input_path, cfg = ha_mqtt._merge_config(args)
    debug_info = {}
    published = ha_mqtt.publish_forecast(
        input_path,
        cfg,
        force=force,
        verify=verify,
        publish_retries=publish_retries,
        retry_delay_sec=args.retry_delay or cfg.retry_delay_sec,
        skip_if_fresh=skip_if_fresh,
        debug=debug_info,
    )
    if published:
        typer.echo("Published new forecast to MQTT.")
    else:
        typer.echo("No publish needed (unchanged or not newer).")
    if verbose and debug_info:
        typer.echo(f"MQTT debug: {debug_info}")


def main() -> None:  # pragma: no cover - thin wrapper for console_script
    app()


__all__ = ["app", "main", "default_weather_provider"]


if __name__ == "__main__":  # pragma: no cover
    main()
