"""Command line entrypoint for solarpredict.

Implements two primary commands:

* ``run``: execute a daily simulation from a scenario config file.
* ``config``: interactive helper to build/edit scenario configs. (disabled)

The CLI is intentionally lightweight and depends only on Typer (Click).
"""
from __future__ import annotations

import datetime as dt
import json
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

import pandas as pd
import typer
import yaml

from solarpredict.core.config import ConfigError, load_scenario
from solarpredict.core import config as config_mod
from solarpredict.core.debug import JsonlDebugWriter, NullDebugCollector, build_debug_collector, JsonDebugWriter
from solarpredict.core.models import Location, PVArray, Scenario, Site, ValidationError
from solarpredict.calibration.ha_tune import auto_calibration_groups, build_prefetched_weather_from_debug_jsonl
from solarpredict.engine.simulate import apply_actual_adjustment, apply_array_scale_factors, apply_output_scale, simulate_day
from solarpredict.solar.clear_sky import clear_sky_irradiance
from solarpredict.solar.irradiance import poa_irradiance
from solarpredict.solar.position import solar_position
from solarpredict.weather.open_meteo import OpenMeteoWeatherProvider
from solarpredict.weather.pvgis import PVGISWeatherProvider
from solarpredict.weather.composite import CompositeWeatherProvider
from solarpredict.weather.prefetched import PrefetchedWeatherProvider
from solarpredict.integrations import ha_mqtt
from solarpredict.integrations.ha_export import HaDailyMaxExport

__version__ = "0.1.0"

app = typer.Typer(add_completion=False, help="Solar generation predictor CLI")


def default_weather_provider(debug) -> OpenMeteoWeatherProvider:
    """Factory separated for easy monkeypatching in tests."""

    return OpenMeteoWeatherProvider(debug=debug)


def _compute_clearsky_poa_kwh_m2(
    scenario: Scenario,
    timeseries: Dict[Tuple[str, str], pd.DataFrame],
    *,
    weather_label: str,
    debug,
    iam_model: str | None,
    iam_coefficient: float | None,
) -> Dict[Tuple[str, str], float]:
    """Compute per-array clear-sky POA daily energy (kWh/m²) for a physical ceiling check.

    Uses the already-simulated timeseries index + interval widths so time labeling (start/end)
    stays consistent with the main run.
    """

    if not timeseries:
        return {}

    site_map = {s.id: s for s in scenario.sites}
    out: Dict[Tuple[str, str], float] = {}

    for site_id, site in site_map.items():
        # Pick any existing array series for this site to get the canonical time grid.
        sample_key = next((k for k in timeseries.keys() if k[0] == site_id), None)
        if sample_key is None:
            continue
        sample_df = timeseries.get(sample_key)
        if sample_df is None or sample_df.empty:
            continue
        times = sample_df.index
        if getattr(times, "tz", None) is None:
            # Simulator expects tz-aware inputs; skip ceiling if index is naive.
            continue

        deltas = times.to_series().diff().dt.total_seconds().dropna()
        step_seconds = float(deltas.median()) if not deltas.empty else 0.0
        solar_times = times
        if step_seconds > 0:
            half = pd.to_timedelta(step_seconds / 2.0, unit="s")
            label = (weather_label or "end").lower()
            if label == "end":
                solar_times = times - half
            elif label == "start":
                solar_times = times + half
            elif label == "center":
                solar_times = times

        # Compute solar position and clear-sky irradiance at interval midpoints,
        # then re-index to original provider timestamps to stay aligned.
        sp = solar_position(site.location, solar_times, debug=NullDebugCollector(), site_id=site.id)
        sp.index = times
        cs = clear_sky_irradiance(
            lat=site.location.lat,
            lon=site.location.lon,
            times=solar_times,
            tz=str(solar_times.tz),
            elevation_m=site.location.elevation_m,
            debug=NullDebugCollector(),
        )
        cs.index = times

        for array in site.arrays:
            key = (site.id, array.id)
            df = timeseries.get(key)
            if df is None or df.empty or "interval_h" not in df.columns:
                continue

            interval_h = df["interval_h"].astype(float)
            arr_iam_model = iam_model if iam_model is not None else array.iam_model
            arr_iam_coeff = iam_coefficient if iam_coefficient is not None else array.iam_coefficient
            poa = poa_irradiance(
                surface_tilt=array.tilt_deg,
                surface_azimuth=array.azimuth_deg,
                dni=cs["dni_wm2"],
                ghi=cs["ghi_wm2"],
                dhi=cs["dhi_wm2"],
                solar_zenith=sp["zenith"],
                solar_azimuth=sp["azimuth"],
                albedo=array.albedo,
                horizon_deg=array.horizon_deg,
                iam_model=arr_iam_model,
                iam_coefficient=arr_iam_coeff,
                debug=NullDebugCollector(),
            )
            out[key] = float(((poa["poa_global"] / 1000.0) * interval_h).sum())

    return out


def _unwrap_typer_default(value):
    """When calling Typer commands as plain functions, defaults are OptionInfo.

    Tests (and some scripts) call these command functions directly; unwrap the
    declared default so regular Python invocation behaves sanely.
    """

    if isinstance(value, typer.models.OptionInfo):
        return value.default
    return value


def _exit_with_error(msg: str) -> None:
    typer.echo(f"Error: {msg}", err=True)
    raise typer.Exit(code=1)


def _default_config_candidates() -> List[Path]:
    return [Path("etc/config.tuned.yaml"), Path("etc/config.yaml")]


def _resolve_config_path(config: Path | None) -> Path:
    if config is not None:
        path = Path(config)
    else:
        path = None
        for cand in _default_config_candidates():
            if cand.exists():
                path = cand
                break
        if path is None:
            path = _default_config_candidates()[-1]
    if not path.exists():
        _exit_with_error(f"Config file {path} not found. Pass --config to specify an explicit file.")
    return path


def _load_raw_config_dict(config_path: Path) -> Dict[str, Any]:
    try:
        data = config_mod._load_raw(config_path)  # type: ignore[attr-defined]
    except ConfigError as exc:
        _exit_with_error(str(exc))
    if not isinstance(data, dict):
        return {}
    return data


def _resolve_date(date_value: Optional[str], run_section: Dict[str, Any]) -> dt.date:
    candidate = date_value or run_section.get("date")
    if candidate:
        try:
            return dt.date.fromisoformat(str(candidate))
        except ValueError:
            _exit_with_error("date must be YYYY-MM-DD")
    return dt.date.today()


def _expand_date_template(value: str, date_obj: dt.date) -> str:
    try:
        return date_obj.strftime(value)
    except Exception:
        return value.replace("%F", date_obj.isoformat())


def _config_path(run_section: Dict[str, Any], key: str, date_obj: dt.date) -> Optional[Path]:
    val = run_section.get(key)
    if val is None:
        return None
    text = str(val)
    if "%" in text:
        text = _expand_date_template(text, date_obj)
    return Path(text)


def _determine_output_path(
    cli_output: Optional[Path],
    run_section: Dict[str, Any],
    fmt: str,
    date_obj: dt.date,
) -> Path:
    if cli_output is not None:
        return cli_output
    cfg_path = _config_path(run_section, "output", date_obj)
    if cfg_path is not None:
        return cfg_path
    if fmt.lower() == "json":
        return Path("json") / f"{date_obj.isoformat()}.json"
    return Path(f"results.{fmt}")


def _coerce_bool(value: Optional[bool], cfg_value: Any, default: bool = False) -> bool:
    if value is not None:
        return bool(value)
    if cfg_value is None:
        return default
    if isinstance(cfg_value, str):
        return cfg_value.strip().lower() in {"1", "true", "yes", "on"}
    return bool(cfg_value)


def _write_scenario(path: Path, scenario: Scenario) -> None:
    _exit_with_error("Scenario writer removed with CLI config. Re-add if needed.")


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
    config: Optional[Path] = typer.Option(
        None,
        "--config",
        help="Scenario YAML/JSON file. Defaults to etc/config.tuned.yaml then etc/config.yaml.",
    ),
    date: Optional[str] = typer.Option(
        None,
        help="Target date (YYYY-MM-DD). Defaults to run.date in config or today.",
    ),
    timestep: Optional[str] = typer.Option(
        None,
        help="Forecast timestep, e.g. 1h or 15m. Defaults to run.timestep in config, else 1h.",
    ),
    weather_label: Optional[str] = typer.Option(
        None,
        help="Meaning of weather timestamps: 'end' (backward-averaged), 'start' (forward-averaged), or 'center'. Defaults to run.weather_label or 'end'.",
    ),
    weather_source: Optional[str] = typer.Option(
        None,
        help="Weather provider: 'open-meteo', 'pvgis-tmy', or 'composite'. Defaults to run.weather_source or 'open-meteo'.",
    ),
    weather_mode: Optional[str] = typer.Option(
        None,
        help="Weather processing mode: 'standard' (use provider irradiance) or 'cloud-scaled' (clear-sky scaled by cloud cover). Defaults to run.weather_mode or 'standard'.",
    ),
    scale_factor: Optional[float] = typer.Option(
        None,
        "--scale-factor",
        help="Optional constant scaling applied to DC/AC outputs (empirical calibration knob). Defaults to run.scale_factor or 1.0.",
    ),
    iam_model: Optional[str] = typer.Option(
        None,
        help="Incidence angle modifier model (ashrae). If unset, defaults to array iam_model when provided.",
    ),
    iam_coefficient: Optional[float] = typer.Option(
        None,
        help="Coefficient for IAM model (e.g., ASHRAE b0).",
    ),
    output_shape: Optional[str] = typer.Option(
        None,
        help="JSON output shape when --format=json: 'hierarchical' (meta+sites) or 'records' (flat list). Defaults to run.output_shape or 'hierarchical'.",
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
    debug: Optional[Path] = typer.Option(
        None,
        help="Write debug JSONL/JSON to this path (json = single document). Defaults to run.debug.",
    ),
    format: Optional[str] = typer.Option(
        None,
        "--format",
        "-f",
        help="Output format: json or csv. Defaults to run.format or json.",
    ),
    output: Optional[Path] = typer.Option(
        None,
        help="Output file path. Defaults to run.output (supports strftime tokens) or json/YYYY-MM-DD.json.",
    ),
    intervals: Optional[Path] = typer.Option(
        None,
        help="Optional per-interval output (.json or .csv) with pac_net_w, poa_global, interval_h, wh_period, wh_cum. Defaults to run.intervals when set.",
    ),
    force: Optional[bool] = typer.Option(
        None,
        "--force/--no-force",
        help="Run even if output already generated today (defaults to run.force or false).",
        show_default=False,
    ),
):
    """Run a daily simulation for the provided scenario."""

    timestep = _unwrap_typer_default(timestep)
    weather_label = _unwrap_typer_default(weather_label)
    weather_source = _unwrap_typer_default(weather_source)
    weather_mode = _unwrap_typer_default(weather_mode)
    scale_factor = _unwrap_typer_default(scale_factor)
    iam_model = _unwrap_typer_default(iam_model)
    iam_coefficient = _unwrap_typer_default(iam_coefficient)
    output_shape = _unwrap_typer_default(output_shape)
    pvgis_cache_dir = _unwrap_typer_default(pvgis_cache_dir)
    qc_pvgis = _unwrap_typer_default(qc_pvgis)
    actual_kwh_today = _unwrap_typer_default(actual_kwh_today)
    actual_limit_suppress = _unwrap_typer_default(actual_limit_suppress)
    actual_as_of = _unwrap_typer_default(actual_as_of)
    base_load_w = _unwrap_typer_default(base_load_w)
    min_duration_min = _unwrap_typer_default(min_duration_min)
    required_wh = _unwrap_typer_default(required_wh)
    debug = _unwrap_typer_default(debug)
    format = _unwrap_typer_default(format)
    output = _unwrap_typer_default(output)
    intervals = _unwrap_typer_default(intervals)
    force = _unwrap_typer_default(force)

    config_path = _resolve_config_path(config)
    raw_cfg = _load_raw_config_dict(config_path)
    run_section = raw_cfg.get("run", {}) if isinstance(raw_cfg, dict) else {}
    scenario = load_scenario(config_path)

    date_obj = _resolve_date(date, run_section)

    fmt_value = format or run_section.get("format") or "json"
    fmt = str(fmt_value).lower()

    output_path = _determine_output_path(output, run_section, fmt, date_obj)
    debug_path = debug if debug else _config_path(run_section, "debug", date_obj)
    intervals_path = intervals if intervals else _config_path(run_section, "intervals", date_obj)
    pvgis_cache_dir = pvgis_cache_dir or _config_path(run_section, "pvgis_cache_dir", date_obj)

    weather_label = (weather_label or run_section.get("weather_label") or "end").lower()
    weather_source = (weather_source or run_section.get("weather_source") or "open-meteo").lower()
    effective_weather_mode = (weather_mode or run_section.get("weather_mode") or "standard").lower()

    debug_collector = build_debug_collector(debug_path) if debug_path else NullDebugCollector()
    debug_target = debug_path

    output_path.parent.mkdir(parents=True, exist_ok=True)

    force_flag = _coerce_bool(force, run_section.get("force"), default=False)
    target_date_str = date_obj.isoformat()
    if not force_flag and output_path.exists():
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

        if generated_at and payload_date == target_date_str:
            gen_dt = dt.datetime.fromisoformat(generated_at)
            if gen_dt.date() == dt.date.today():
                typer.echo(
                    f"Output {output_path} for {payload_date} already generated today ({generated_at}); use --force to rerun."
                )
                return output_path

    weather_mode = effective_weather_mode

    # Optional global output scaling (calibration).
    scale_factor = _unwrap_typer_default(scale_factor)
    cfg_scale = run_section.get("scale_factor") if run_section else None
    effective_scale = scale_factor if scale_factor is not None else cfg_scale
    if effective_scale is None:
        effective_scale = 1.0
    try:
        effective_scale = float(effective_scale)
    except Exception:
        _exit_with_error("scale_factor must be numeric")
    if effective_scale <= 0:
        _exit_with_error("scale_factor must be > 0")

    # Optional per-array scale factors (fine-grained calibration).
    raw_array_scales = run_section.get("array_scale_factors") if isinstance(run_section, dict) else None
    if raw_array_scales is None:
        raw_array_scales = {}
    if not isinstance(raw_array_scales, dict):
        _exit_with_error("run.array_scale_factors must be a mapping of array_id (or site/array) -> scale")
    array_scale_factors: dict[str, float] = {}
    for k, v in raw_array_scales.items():
        key = str(k)
        try:
            fval = float(v)
        except Exception:
            _exit_with_error(f"run.array_scale_factors[{key!r}] must be numeric")
        if fval <= 0:
            _exit_with_error(f"run.array_scale_factors[{key!r}] must be > 0")
        array_scale_factors[key] = fval

    weather_source = weather_source.lower()
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
        iam_model=iam_model,
        iam_coefficient=iam_coefficient,
    )

    if effective_scale != 1.0:
        result = apply_output_scale(result, effective_scale, debug=debug_collector)

    if array_scale_factors:
        result = apply_array_scale_factors(result, array_scale_factors, debug=debug_collector)

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
            result = apply_actual_adjustment(
                result,
                effective_actual,
                debug_collector,
                now_ts=parsed_as_of,
                series_label=weather_label,
            )
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

        # Compute a clear-sky POA ceiling (kWh/m²) on the same time grid so we can
        # clamp only when forecasts exceed a physical maximum.
        clearsky_poa_map = _compute_clearsky_poa_kwh_m2(
            scenario,
            result.timeseries,
            weather_label=weather_label,
            debug=debug_collector,
            iam_model=iam_model,
            iam_coefficient=iam_coefficient,
        )

        # Attach PVGIS POA baseline onto the main daily output so MQTT can publish it.
        daily_with_pvgis = result.daily.copy()
        daily_with_pvgis["pvgis_poa_kwh_m2"] = daily_with_pvgis.apply(
            lambda row: pvgis_poa_map.get((row["site"], row["array"])), axis=1
        )

        # Warn on deviations vs PVGIS typical-year baseline, but only clamp when the
        # forecast exceeds a clear-sky ceiling (i.e., physically implausible).
        ceiling_margin = 1.15  # allow modeling / timestamp differences; clamp only on clear outliers
        scale_map = {}
        capped_rows = []
        for _, row in daily_with_pvgis.iterrows():
            key = (row.get("site"), row.get("array"))
            poa = row.get("poa_kwh_m2")
            forecast_poa = float(poa or 0.0)

            # Track scale factors so we can apply any ceiling clamp to timeseries for consistency.
            scale_map[key] = 1.0

            # PVGIS comparison is informational (warn-first).
            pvgis = row.get("pvgis_poa_kwh_m2")
            baseline_poa = None
            ratio = float("inf")
            if pvgis is not None and not pd.isna(pvgis) and float(pvgis) > 0:
                baseline_poa = float(pvgis)
                ratio = float(forecast_poa / baseline_poa)
            debug_collector.emit(
                "qc.pvgis_compare",
                {
                    "site": key[0],
                    "array": key[1],
                    "ratio": ratio,
                    "baseline_poa_kwh_m2": baseline_poa,
                    "forecast_poa_kwh_m2": forecast_poa,
                },
                ts=date_obj,
            )
            cloudy = forecast_poa < 0.6  # kWh/m2 per day rough cloud marker
            low, high = (0.3, 2.0) if cloudy else (0.6, 1.6)
            if baseline_poa is not None and (ratio < low or ratio > high):
                typer.echo(
                    f"QC warning: {key[0]}/{key[1]} PVGIS POA ratio {ratio:.2f} outside [{low},{high}] (cloudy={cloudy})",
                    err=True,
                )

            # Clear-sky ceiling clamp (hard guardrail).
            cs_poa = clearsky_poa_map.get(key)
            if cs_poa is not None and cs_poa > 0:
                cs_ratio = float(forecast_poa / cs_poa) if cs_poa else float("inf")
                debug_collector.emit(
                    "qc.clearsky_compare",
                    {
                        "site": key[0],
                        "array": key[1],
                        "ratio": cs_ratio,
                        "clearsky_poa_kwh_m2": float(cs_poa),
                        "forecast_poa_kwh_m2": forecast_poa,
                        "margin": ceiling_margin,
                    },
                    ts=date_obj,
                )
                max_allowed = float(cs_poa) * ceiling_margin
                if forecast_poa > max_allowed and forecast_poa > 0:
                    scale = max_allowed / forecast_poa
                    scale_map[key] = scale

            row = row.copy()
            scale = scale_map[key]
            if scale != 1.0:
                row["poa_kwh_m2"] = float(row["poa_kwh_m2"]) * scale
                row["energy_kwh"] = float(row["energy_kwh"]) * scale
                row["peak_kw"] = float(row["peak_kw"]) * scale
                row["qc_clipped"] = True
                row["qc_clip_reason"] = "clearsky_ceiling"
                row["qc_ratio"] = ratio
                row["qc_clearsky_poa_kwh_m2"] = float(cs_poa) if cs_poa is not None else None
                row["qc_clearsky_ratio"] = float(forecast_poa / cs_poa) if cs_poa else None
            capped_rows.append(row)
        daily_with_pvgis = pd.DataFrame(capped_rows)

        # Apply the same scale to timeseries for arrays that were capped so debug/intervals stay consistent.
        scaled_ts = {}
        for key, df in result.timeseries.items():
            scale = scale_map.get(key, 1.0)
            if scale != 1.0 and df is not None:
                df_scaled = df.copy()
                for col in ("pdc_w", "pac_w", "pac_net_w", "poa_global"):
                    if col in df_scaled.columns:
                        df_scaled[col] = df_scaled[col] * scale
                scaled_ts[key] = df_scaled
            else:
                scaled_ts[key] = df

        result = type(result)(daily=daily_with_pvgis, timeseries=scaled_ts)

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

    if fmt == "json":
        serializable = daily.copy()
        for col in serializable.columns:
            if str(serializable[col].dtype).startswith("datetime64"):
                serializable[col] = serializable[col].astype(str)

        records_payload = serializable.to_dict(orient="records")

        effective_output_shape = (output_shape or run_section.get("output_shape") or "hierarchical").lower()
        if effective_output_shape == "records":
            payload = records_payload if load_windows is None else {"results": records_payload, "load_windows": load_windows}
        elif effective_output_shape == "hierarchical":
            # Build hierarchical shape with meta/sites to avoid shell-script postprocessing.
            sites: Dict[str, Dict[str, Any]] = {}
            for rec in records_payload:
                site_id = rec.get("site") or "unknown"
                arr_id = rec.get("array") or "array"
                site = sites.setdefault(site_id, {"id": site_id, "arrays": []})
                site["arrays"].append(rec)
            sites_list: List[Dict[str, Any]] = []
            for site_id, site in sites.items():
                arrays_sorted = sorted(site["arrays"], key=lambda a: a.get("array") or a.get("id") or "")
                total_energy = round(sum(float(a.get("energy_kwh", 0) or 0) for a in arrays_sorted), 3)
                site_entry = {"id": site_id, "arrays": arrays_sorted, "total_energy_kwh": total_energy}
                sites_list.append(site_entry)
            sites_list = sorted(sites_list, key=lambda s: s.get("id") or "")

            meta = {
                "generated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
                "date": target_date_str,
                "timestep": effective_timestep,
                "provider": weather_source,
                "scale_factor": effective_scale,
                "total_energy_kwh": round(sum(s["total_energy_kwh"] for s in sites_list), 3) if sites_list else 0,
                "site_count": len(sites_list),
                "array_count": sum(len(s["arrays"]) for s in sites_list),
            }
            payload: Dict[str, Any] = {"meta": meta, "sites": sites_list}
            if load_windows is not None:
                payload["load_windows"] = load_windows
        else:
            _exit_with_error("output-shape must be 'hierarchical' or 'records'")

        output_path.write_text(json.dumps(payload, indent=2))
    elif fmt == "csv":
        daily.to_csv(output_path, index=False)
    else:
        _exit_with_error("format must be json or csv")

    # Optional per-interval export
    if intervals_path:
        intervals_path.parent.mkdir(parents=True, exist_ok=True)
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
    if debug_target:
        # flush single-JSON collector if used
        if isinstance(debug_collector, JsonDebugWriter):
            debug_collector.finalize()
        typer.echo(f"Debug events -> {debug_target}")

    return output_path


@app.command()
def go(
    config: Optional[Path] = typer.Option(
        None,
        "--config",
        help="Scenario config. Defaults to etc/config.tuned.yaml then etc/config.yaml.",
    ),
    date: Optional[str] = typer.Option(
        None,
        help="Target date (YYYY-MM-DD). Defaults to run.date or today.",
    ),
    publish: Optional[bool] = typer.Option(
        None,
        "--publish/--no-publish",
        help="Publish to MQTT after running. Defaults to mqtt.enable.",
        show_default=False,
    ),
    force: Optional[bool] = typer.Option(
        None,
        "--force/--no-force",
        help="Force the simulation even if today's output already exists (defaults to run.force).",
        show_default=False,
    ),
    mqtt_force: Optional[bool] = typer.Option(
        None,
        "--mqtt-force/--no-mqtt-force",
        help="Force MQTT publish even if unchanged/older (defaults to mqtt.force).",
        show_default=False,
    ),
    verify: Optional[bool] = typer.Option(
        None,
        "--verify/--no-verify",
        help="MQTT verification toggle. Defaults to mqtt.verify (false when unset).",
        show_default=False,
    ),
    publish_retries: Optional[int] = typer.Option(
        None,
        "--publish-retries",
        help="Retry MQTT publish N times (defaults to mqtt.publish_retries or 1).",
    ),
    retry_delay: Optional[float] = typer.Option(
        None,
        "--retry-delay",
        help="Delay between MQTT publish retries in seconds (defaults to mqtt.retry_delay or cfg retry_delay).",
    ),
    skip_if_fresh: Optional[bool] = typer.Option(
        None,
        "--skip-if-fresh/--no-skip-if-fresh",
        help="Skip MQTT publish when broker already has same/newer payload (defaults to mqtt.skip_if_fresh).",
        show_default=False,
    ),
):
    """Run the forecast and (optionally) publish to MQTT using config defaults."""

    publish = _unwrap_typer_default(publish)
    force = _unwrap_typer_default(force)
    mqtt_force = _unwrap_typer_default(mqtt_force)
    verify = _unwrap_typer_default(verify)
    publish_retries = _unwrap_typer_default(publish_retries)
    retry_delay = _unwrap_typer_default(retry_delay)
    skip_if_fresh = _unwrap_typer_default(skip_if_fresh)

    config_path = _resolve_config_path(config)
    raw_cfg = _load_raw_config_dict(config_path)
    run_section = raw_cfg.get("run", {}) if isinstance(raw_cfg, dict) else {}
    mqtt_section = raw_cfg.get("mqtt", {}) if isinstance(raw_cfg, dict) else {}

    date_obj = _resolve_date(date, run_section)
    target_date = date_obj.isoformat()

    output_path = run(
        config=config_path,
        date=target_date,
        timestep=None,
        weather_label=None,
        weather_source=None,
        weather_mode=None,
        scale_factor=None,
        iam_model=None,
        iam_coefficient=None,
        output_shape=None,
        pvgis_cache_dir=None,
        qc_pvgis=None,
        actual_kwh_today=None,
        actual_limit_suppress=None,
        actual_as_of=None,
        base_load_w=None,
        min_duration_min=None,
        required_wh=None,
        debug=None,
        format=None,
        output=None,
        intervals=None,
        force=force,
    )

    publish_flag = _coerce_bool(publish, (mqtt_section or {}).get("enable"), default=False)
    if not publish_flag:
        typer.echo("MQTT publish disabled via config or flag; skipping.")
        return output_path

    verify_flag = _coerce_bool(verify, mqtt_section.get("verify"), default=False)
    mqtt_force_flag = _coerce_bool(mqtt_force, mqtt_section.get("force"), default=False)
    skip_if_fresh_flag = _coerce_bool(skip_if_fresh, mqtt_section.get("skip_if_fresh"), default=False)
    publish_retries_val = publish_retries if publish_retries is not None else mqtt_section.get("publish_retries")
    if publish_retries_val is None:
        publish_retries_val = 1
    publish_retries_val = int(publish_retries_val)
    retry_delay_val = retry_delay if retry_delay is not None else mqtt_section.get("retry_delay")
    retry_delay_val = float(retry_delay_val) if retry_delay_val is not None else None

    publish_mqtt(
        config=config_path,
        input=output_path,
        retry_delay=retry_delay_val,
        force=mqtt_force_flag,
        verify=verify_flag,
        publish_retries=publish_retries_val,
        skip_if_fresh=skip_if_fresh_flag,
    )

    return output_path


@app.command("ha-compare")
def ha_compare(
    config: Path = typer.Option(..., "--config", help="Scenario config YAML (same as run)."),
    ha_export: Path = typer.Option(..., "--ha", help="Home Assistant daily-max export JSON."),
    entity_id: str = typer.Option("sensor.total_pv_energy_today", "--entity", help="HA entity_id to compare against."),
    start: Optional[str] = typer.Option(None, help="Start date YYYY-MM-DD (defaults to earliest in export)."),
    end: Optional[str] = typer.Option(None, help="End date YYYY-MM-DD (defaults to latest in export)."),
    timestep: str = typer.Option("1h", help="Simulation timestep (e.g., 1h, 15m)."),
    weather_label: str = typer.Option("end", help="Meaning of timestamps: end/start/center (must match run)."),
    weather_source: str = typer.Option(
        "open-meteo",
        help="Weather provider: open-meteo (recent history), pvgis-tmy (climatology), or composite.",
    ),
    weather_mode: Optional[str] = typer.Option(None, help="Weather processing mode: standard or cloud-scaled."),
    scale_factor: Optional[float] = typer.Option(
        None,
        "--scale-factor",
        help="Optional constant scaling applied to DC/AC outputs when computing pred_kwh. Defaults to run.scale_factor or 1.0.",
    ),
    min_actual_kwh: float = typer.Option(
        1.0,
        "--min-actual-kwh",
        help="Ignore days with actual_kwh below this threshold when computing suggested scale.",
    ),
    min_pred_kwh: float = typer.Option(
        1.0,
        "--min-pred-kwh",
        help="Ignore days with pred_kwh below this threshold when computing suggested scale.",
    ),
    write_config: Optional[Path] = typer.Option(
        None,
        "--write-config",
        help="Write a copy of --config with run.scale_factor set to the suggested value (YAML/JSON based on extension).",
    ),
    out: Optional[Path] = typer.Option(None, "--output", help="Write CSV to this path (default: just print)."),
    debug: Optional[Path] = typer.Option(None, "--debug", help="Write debug JSONL/JSON here."),
):
    """Compare forecast daily totals against Home Assistant historical production."""

    debug_collector = build_debug_collector(debug) if debug else NullDebugCollector()
    raw_cfg = None
    try:
        raw_cfg = config_mod._load_raw(config)  # type: ignore[attr-defined] - internal helper, matches run()
    except Exception:
        raw_cfg = None
    scenario = load_scenario(config)
    run_section = raw_cfg.get("run", {}) if isinstance(raw_cfg, dict) else {}
    scale_factor = _unwrap_typer_default(scale_factor)
    cfg_scale = run_section.get("scale_factor") if run_section else None
    effective_scale = scale_factor if scale_factor is not None else cfg_scale
    if effective_scale is None:
        effective_scale = 1.0
    try:
        effective_scale = float(effective_scale)
    except Exception:
        _exit_with_error("scale_factor must be numeric")
    if effective_scale <= 0:
        _exit_with_error("scale_factor must be > 0")

    raw_array_scales = run_section.get("array_scale_factors") if isinstance(run_section, dict) else None
    if raw_array_scales is None:
        raw_array_scales = {}
    if not isinstance(raw_array_scales, dict):
        _exit_with_error("run.array_scale_factors must be a mapping of array_id (or site/array) -> scale")
    array_scale_factors: dict[str, float] = {}
    for k, v in raw_array_scales.items():
        key = str(k)
        try:
            fval = float(v)
        except Exception:
            _exit_with_error(f"run.array_scale_factors[{key!r}] must be numeric")
        if fval <= 0:
            _exit_with_error(f"run.array_scale_factors[{key!r}] must be > 0")
        array_scale_factors[key] = fval

    export = HaDailyMaxExport.from_path(ha_export, debug=debug_collector)
    df = export.to_frame(entities=[entity_id], debug=debug_collector)
    if df.empty:
        _exit_with_error(f"No rows for entity_id {entity_id} in {ha_export}")

    try:
        start_d = dt.date.fromisoformat(start) if start else df["day"].min()
        end_d = dt.date.fromisoformat(end) if end else df["day"].max()
    except ValueError:
        _exit_with_error("start/end must be YYYY-MM-DD")
    if start_d > end_d:
        _exit_with_error("start must be <= end")

    source = (weather_source or "open-meteo").lower()
    effective_weather_mode = (weather_mode or "standard").lower()
    locations = [
        {"id": str(site.id), "lat": float(site.location.lat), "lon": float(site.location.lon), "tz": str(site.location.tz)}
        for site in scenario.sites
    ]

    # Prefetch weather once for the whole window to avoid N network calls.
    window_start = start_d.isoformat()
    window_end = (end_d + dt.timedelta(days=1)).isoformat()

    if source == "open-meteo":
        raw_provider = default_weather_provider(debug=debug_collector)
        prefetched = raw_provider.get_forecast(locations, start=window_start, end=window_end, timestep=timestep)
        provider = PrefetchedWeatherProvider(prefetched)
    elif source == "pvgis-tmy":
        # PVGIS returns a full TMY year; cache_dir keeps the (lat,lon) request deterministic and fast.
        raw_provider = PVGISWeatherProvider(debug=debug_collector, cache_dir=".cache/pvgis")
        prefetched = raw_provider.get_forecast(locations, start=window_start, end=window_end, timestep="1h")
        provider = PrefetchedWeatherProvider(prefetched)
    elif source == "composite":
        primary_raw = default_weather_provider(debug=debug_collector)
        secondary_raw = PVGISWeatherProvider(debug=debug_collector, cache_dir=".cache/pvgis")
        primary_pref = PrefetchedWeatherProvider(primary_raw.get_forecast(locations, start=window_start, end=window_end, timestep=timestep))
        secondary_pref = PrefetchedWeatherProvider(secondary_raw.get_forecast(locations, start=window_start, end=window_end, timestep="1h"))
        provider = CompositeWeatherProvider(primary=primary_pref, secondary=secondary_pref, debug=debug_collector)
    else:
        _exit_with_error(f"Unsupported weather_source '{weather_source}'")

    day_to_actual = {r["day"]: float(r["energy_kwh"]) for r in df.to_dict(orient="records")}
    rows: List[Dict[str, Any]] = []

    min_actual_kwh = float(_unwrap_typer_default(min_actual_kwh))
    min_pred_kwh = float(_unwrap_typer_default(min_pred_kwh))

    cur = start_d
    while cur <= end_d:
        actual = day_to_actual.get(cur)
        if actual is None:
            cur += dt.timedelta(days=1)
            continue

        pred_kwh: float | None
        err: str | None
        try:
            res = simulate_day(
                scenario,
                date=cur,
                timestep=timestep,
                weather_provider=provider,
                snow_weather_provider=provider,
                debug=debug_collector,
                weather_label=weather_label,
                weather_mode=effective_weather_mode,
                iam_model=None,
                iam_coefficient=None,
            )
            if effective_scale != 1.0:
                res = apply_output_scale(res, effective_scale, debug=debug_collector)
            if array_scale_factors:
                res = apply_array_scale_factors(res, array_scale_factors, debug=debug_collector)
            pred_kwh = float(res.daily["energy_kwh"].sum()) if not res.daily.empty else 0.0
            err = None
        except Exception as exc:
            pred_kwh = None
            err = str(exc)
            if len(err) > 300:
                err = err[:300] + "…"

        ratio = (actual / pred_kwh) if (pred_kwh is not None and pred_kwh > 0) else None
        rows.append(
            {
                "date": cur.isoformat(),
                "actual_kwh": actual,
                "pred_kwh": pred_kwh,
                "ratio_actual_over_pred": ratio,
                "error": err,
            }
        )
        debug_collector.emit(
            "ha.compare.row",
            {"date": cur.isoformat(), "entity_id": entity_id, "actual_kwh": actual, "pred_kwh": pred_kwh, "error": err},
            ts=cur.isoformat(),
        )
        cur += dt.timedelta(days=1)

    out_df = pd.DataFrame(rows)
    ok = out_df[(out_df["pred_kwh"].notna()) & (out_df["pred_kwh"] >= min_pred_kwh) & (out_df["actual_kwh"] >= min_actual_kwh)]
    suggested_multiplier = float(ok["ratio_actual_over_pred"].median()) if not ok.empty else None
    suggested_new_scale = (effective_scale * suggested_multiplier) if suggested_multiplier is not None else None
    debug_collector.emit(
        "ha.compare.summary",
        {
            "entity_id": entity_id,
            "rows": int(len(out_df)),
            "ok_rows": int(len(ok)),
            "scale_factor": effective_scale,
            "median_actual_over_pred": suggested_multiplier,
            "suggested_scale_factor": suggested_new_scale,
        },
        ts=end_d.isoformat(),
    )

    typer.echo(out_df.tail(20).to_string(index=False))
    if suggested_multiplier is not None:
        typer.echo(f"Suggested multiplier (median actual/pred): {suggested_multiplier:.3f}")
        typer.echo(f"Suggested run.scale_factor: {suggested_new_scale:.3f} (current {effective_scale:.3f})")
        if write_config is not None:
            cfg_out = dict(raw_cfg) if isinstance(raw_cfg, dict) else {}
            run_out = cfg_out.get("run") or {}
            if not isinstance(run_out, dict):
                run_out = {}
            run_out["scale_factor"] = float(round(float(suggested_new_scale), 6))
            cfg_out["run"] = run_out
            if write_config.suffix.lower() == ".json":
                write_config.write_text(json.dumps(cfg_out, indent=2))
            else:
                write_config.write_text(yaml.safe_dump(cfg_out, sort_keys=False))
            typer.echo(f"Wrote tuned config to {write_config}")
    if out is not None:
        out.write_text(out_df.to_csv(index=False))
        typer.echo(f"Wrote {len(out_df)} rows to {out}")
    if debug and isinstance(debug_collector, JsonDebugWriter):
        debug_collector.finalize()


@app.command("ha-tune")
def ha_tune(
    config: Path = typer.Option(..., "--config", help="Scenario config YAML/JSON (same as run)."),
    ha_export: Path = typer.Option(..., "--ha", help="Home Assistant daily-max export JSON."),
    start: Optional[str] = typer.Option(None, help="Start date YYYY-MM-DD (default: last ~92 days)."),
    end: Optional[str] = typer.Option(None, help="End date YYYY-MM-DD (default: latest in export)."),
    timestep: str = typer.Option("1h", help="Simulation timestep (e.g., 1h, 15m)."),
    weather_label: str = typer.Option("end", help="Meaning of timestamps: end/start/center (must match run)."),
    weather_source: str = typer.Option(
        "open-meteo",
        help="Weather provider: open-meteo (recent history), pvgis-tmy (climatology), or composite.",
    ),
    weather_mode: Optional[str] = typer.Option(None, help="Weather processing mode: standard or cloud-scaled."),
    weather_debug: Optional[Path] = typer.Option(
        None,
        "--weather-debug",
        help="Optional debug JSONL containing stage=weather.raw (used to build a PrefetchedWeatherProvider and avoid network).",
    ),
    min_actual_kwh: float = typer.Option(
        1.0,
        "--min-actual-kwh",
        help="Ignore group-days with actual_kwh below this threshold when fitting scales.",
    ),
    min_pred_kwh: float = typer.Option(
        1.0,
        "--min-pred-kwh",
        help="Ignore group-days with pred_kwh below this threshold when fitting scales.",
    ),
    write_config: Optional[Path] = typer.Option(
        None,
        "--write-config",
        help="Write a copy of --config with run.array_scale_factors set (YAML/JSON based on extension).",
    ),
    out: Optional[Path] = typer.Option(None, "--output", help="Write per-day training CSV to this path (optional)."),
    debug: Optional[Path] = typer.Option(None, "--debug", help="Write debug JSONL/JSON here."),
):
    """Train per-array scale factors from Home Assistant subsystem sensors.

    This command is designed for "do it for me" operations:
    - auto-detect sensor groups (e.g. house_north vs pv_array_north_*)
    - run simulations over a historical window
    - compute median(actual/pred) per group
    - write run.array_scale_factors into a tuned config
    """

    debug_collector = build_debug_collector(debug) if debug else NullDebugCollector()
    raw_cfg = None
    try:
        raw_cfg = config_mod._load_raw(config)  # type: ignore[attr-defined]
    except Exception:
        raw_cfg = None
    scenario = load_scenario(config)
    run_section = raw_cfg.get("run", {}) if isinstance(raw_cfg, dict) else {}

    export = HaDailyMaxExport.from_path(ha_export, debug=debug_collector)
    ha_df = export.to_frame(entities=None, debug=debug_collector)
    if ha_df.empty:
        _exit_with_error(f"No rows in {ha_export}")

    try:
        end_d = dt.date.fromisoformat(end) if end else ha_df["day"].max()
    except ValueError:
        _exit_with_error("end must be YYYY-MM-DD")
    try:
        if start:
            start_d = dt.date.fromisoformat(start)
        else:
            # Default to last ~92 days to stay within Open-Meteo lookback.
            earliest = ha_df["day"].min()
            start_d = max(earliest, end_d - dt.timedelta(days=92))
    except ValueError:
        _exit_with_error("start must be YYYY-MM-DD")
    if start_d > end_d:
        _exit_with_error("start must be <= end")

    # Auto-map HA entities to arrays.
    groups = auto_calibration_groups(scenario, export.sensors, include_total=True)
    train_groups = [g for g in groups if g.name != "total_pv"]
    if not train_groups:
        _exit_with_error("No calibration groups detected; export sensors did not match any array ids")

    # Ensure arrays are not assigned to multiple groups (except total_pv which is excluded above).
    assigned: dict[Tuple[str, str], str] = {}
    for g in train_groups:
        for key in g.arrays:
            if key in assigned and assigned[key] != g.name:
                _exit_with_error(f"Array {key[0]}/{key[1]} matched multiple groups: {assigned[key]} and {g.name}")
            assigned[key] = g.name

    effective_weather_mode = (weather_mode or run_section.get("weather_mode") or "standard").lower()
    source = (weather_source or "open-meteo").lower()

    locations = [
        {"id": str(site.id), "lat": float(site.location.lat), "lon": float(site.location.lon), "tz": str(site.location.tz)}
        for site in scenario.sites
    ]
    window_start = start_d.isoformat()
    window_end = (end_d + dt.timedelta(days=1)).isoformat()

    if weather_debug is not None:
        prefetched = build_prefetched_weather_from_debug_jsonl(
            weather_debug,
            site_ids=[str(site.id) for site in scenario.sites],
            debug=debug_collector,
        )
        provider = PrefetchedWeatherProvider(prefetched)
        debug_collector.emit(
            "calibration.weather_provider",
            {"source": "debug-jsonl", "path": str(weather_debug), "window_start": window_start, "window_end": window_end},
            ts=window_start,
        )
    else:
        # Prefetch weather once for the whole window to avoid N network calls.
        if source == "open-meteo":
            raw_provider = default_weather_provider(debug=debug_collector)
            prefetched = raw_provider.get_forecast(locations, start=window_start, end=window_end, timestep=timestep)
            provider = PrefetchedWeatherProvider(prefetched)
        elif source == "pvgis-tmy":
            raw_provider = PVGISWeatherProvider(debug=debug_collector, cache_dir=".cache/pvgis")
            prefetched = raw_provider.get_forecast(locations, start=window_start, end=window_end, timestep="1h")
            provider = PrefetchedWeatherProvider(prefetched)
        elif source == "composite":
            primary_raw = default_weather_provider(debug=debug_collector)
            secondary_raw = PVGISWeatherProvider(debug=debug_collector, cache_dir=".cache/pvgis")
            primary_pref = PrefetchedWeatherProvider(primary_raw.get_forecast(locations, start=window_start, end=window_end, timestep=timestep))
            secondary_pref = PrefetchedWeatherProvider(secondary_raw.get_forecast(locations, start=window_start, end=window_end, timestep="1h"))
            provider = CompositeWeatherProvider(primary=primary_pref, secondary=secondary_pref, debug=debug_collector)
        else:
            _exit_with_error(f"Unsupported weather_source '{weather_source}'")

    # Build a day -> entity -> energy map for quick lookups.
    day_entity: dict[dt.date, dict[str, float]] = {}
    for r in ha_df.to_dict(orient="records"):
        day = r.get("day")
        ent = r.get("entity_id")
        val = r.get("energy_kwh")
        if isinstance(day, dt.date) and isinstance(ent, str):
            try:
                day_entity.setdefault(day, {})[ent] = float(val)
            except Exception:
                continue

    rows: list[dict[str, Any]] = []
    min_actual_kwh = float(_unwrap_typer_default(min_actual_kwh))
    min_pred_kwh = float(_unwrap_typer_default(min_pred_kwh))

    cur = start_d
    while cur <= end_d:
        try:
            res = simulate_day(
                scenario,
                date=cur,
                timestep=timestep,
                weather_provider=provider,
                snow_weather_provider=provider,
                debug=debug_collector,
                weather_label=weather_label,
                weather_mode=effective_weather_mode,
                iam_model=None,
                iam_coefficient=None,
            )
        except Exception as exc:
            # Still emit rows for visibility; group-level rows get error set.
            for g in train_groups:
                rows.append(
                    {
                        "date": cur.isoformat(),
                        "group": g.name,
                        "actual_kwh": None,
                        "pred_kwh": None,
                        "ratio_actual_over_pred": None,
                        "error": str(exc)[:300],
                    }
                )
            cur += dt.timedelta(days=1)
            continue

        pred_map: dict[Tuple[str, str], float] = {}
        if res.daily is not None and not res.daily.empty:
            for _, r in res.daily.iterrows():
                try:
                    pred_map[(str(r["site"]), str(r["array"]))] = float(r["energy_kwh"])
                except Exception:
                    continue

        day_actual = day_entity.get(cur, {})
        for g in train_groups:
            actual_vals = []
            missing = []
            for ent in g.ha_entities:
                if ent in day_actual:
                    actual_vals.append(float(day_actual[ent]))
                else:
                    missing.append(ent)
            if missing:
                # Skip silently; we don't want partial subsystem totals.
                continue
            actual_kwh = float(sum(actual_vals))
            pred_kwh = float(sum(pred_map.get(a, 0.0) for a in g.arrays))
            ratio = (actual_kwh / pred_kwh) if pred_kwh > 0 else None
            rows.append(
                {
                    "date": cur.isoformat(),
                    "group": g.name,
                    "actual_kwh": actual_kwh,
                    "pred_kwh": pred_kwh,
                    "ratio_actual_over_pred": ratio,
                    "error": None,
                }
            )
        cur += dt.timedelta(days=1)

    out_df = pd.DataFrame(rows)
    if out_df.empty:
        _exit_with_error("No training rows produced (missing sensors or empty simulation output)")

    # Fit one scale per group.
    suggested: dict[str, float] = {}
    group_rows = []
    for g in train_groups:
        gdf = out_df[(out_df["group"] == g.name) & (out_df["error"].isna())]
        ok = gdf[
            (gdf["pred_kwh"].notna())
            & (gdf["actual_kwh"].notna())
            & (gdf["pred_kwh"] >= min_pred_kwh)
            & (gdf["actual_kwh"] >= min_actual_kwh)
            & (gdf["ratio_actual_over_pred"].notna())
        ]
        scale = float(ok["ratio_actual_over_pred"].median()) if not ok.empty else None
        if scale is not None:
            suggested[g.name] = scale
        group_rows.append(
            {
                "group": g.name,
                "ha_entities": ",".join(sorted(g.ha_entities)),
                "arrays": ",".join(f"{s}/{a}" for s, a in g.arrays),
                "rows": int(len(gdf)),
                "ok_rows": int(len(ok)),
                "median_actual_over_pred": scale,
            }
        )

    summary_df = pd.DataFrame(group_rows).sort_values("group")
    typer.echo(summary_df.to_string(index=False))

    if not suggested:
        _exit_with_error("No groups produced a scale factor (too few ok rows); try lowering min thresholds")

    # Build run.array_scale_factors mapping by applying each group's scale to its arrays.
    array_scale_factors: dict[str, float] = {}
    for g in train_groups:
        scale = suggested.get(g.name)
        if scale is None:
            continue
        for site_id, array_id in g.arrays:
            key = f"{site_id}/{array_id}"
            array_scale_factors[key] = float(round(scale, 6))

    debug_collector.emit(
        "ha.tune.summary",
        {
            "groups": int(len(train_groups)),
            "scales": int(len(array_scale_factors)),
            "start": start_d.isoformat(),
            "end": end_d.isoformat(),
        },
        ts=end_d.isoformat(),
    )

    if write_config is not None:
        cfg_out = dict(raw_cfg) if isinstance(raw_cfg, dict) else {}
        run_out = cfg_out.get("run") or {}
        if not isinstance(run_out, dict):
            run_out = {}
        # Ensure we don't accidentally double-scale if the input config already had scale_factor.
        run_out["scale_factor"] = 1.0
        run_out["array_scale_factors"] = array_scale_factors
        cfg_out["run"] = run_out
        if write_config.suffix.lower() == ".json":
            write_config.write_text(json.dumps(cfg_out, indent=2))
        else:
            write_config.write_text(yaml.safe_dump(cfg_out, sort_keys=False))
        typer.echo(f"Wrote tuned config to {write_config}")

    if out is not None:
        out.write_text(out_df.to_csv(index=False))
        typer.echo(f"Wrote {len(out_df)} rows to {out}")

    if debug and isinstance(debug_collector, JsonDebugWriter):
        debug_collector.finalize()


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
    *args,
    **kwargs,
):
    """Config CLI removed. This command now exits with an error."""
    _exit_with_error("config command disabled; CLI config flow removed.")


@app.callback()
def version_callback(
    version: bool = typer.Option(False, "--version", help="Show version and exit"),
):
    if version:
        typer.echo(__version__)
        raise typer.Exit()


@app.command("publish-mqtt")
def publish_mqtt(
    config: Optional[Path] = typer.Option(
        None,
        "--config",
        help="Combined scenario + mqtt config (defaults to etc/config.tuned.yaml, then etc/config.yaml).",
    ),
    input: Optional[Path] = typer.Option(
        None,
        "--input",
        "-i",
        help="Forecast JSON produced by run/go. Defaults to mqtt.input or run.output.",
    ),
    mqtt_host: Optional[str] = typer.Option(None, help="Override MQTT host"),
    mqtt_port: Optional[int] = typer.Option(None, help="Override MQTT port"),
    mqtt_username: Optional[str] = typer.Option(None, help="Override MQTT username"),
    mqtt_password: Optional[str] = typer.Option(None, help="Override MQTT password"),
    base_topic: Optional[str] = typer.Option(None, help="Override base topic"),
    discovery_prefix: Optional[str] = typer.Option(None, help="Override discovery prefix"),
    connect_retries: Optional[int] = typer.Option(None, help="MQTT connection retries"),
    retry_delay: Optional[float] = typer.Option(None, help="Delay between retries in seconds"),
    verbose: Optional[bool] = typer.Option(
        None,
        "--verbose/--no-verbose",
        help="Enable chatty logging (defaults to mqtt.verbose from config).",
        show_default=False,
    ),
    force: Optional[bool] = typer.Option(
        None,
        "--force/--no-force",
        help="Publish even if unchanged or older (defaults to mqtt.force).",
        show_default=False,
    ),
    no_state: bool = typer.Option(False, "--no-state", help="Skip retained state blob"),
    publish_topics: Optional[bool] = typer.Option(
        None,
        "--publish-topics/--no-publish-topics",
        help="Also publish scalar topics (retained). Defaults to config.",
        show_default=False,
    ),
    verify: Optional[bool] = typer.Option(
        None,
        "--verify/--no-verify",
        help="Read back retained state and ensure it matches (defaults to mqtt.verify).",
        show_default=False,
    ),
    publish_retries: Optional[int] = typer.Option(
        None,
        "--publish-retries",
        help="Retry publish+verify N times on failure (defaults to mqtt.publish_retries or 1).",
    ),
    no_discovery: bool = typer.Option(False, "--no-discovery", help="Skip HA discovery publish"),
    skip_if_fresh: Optional[bool] = typer.Option(
        None,
        "--skip-if-fresh/--no-skip-if-fresh",
        help="If broker already has same/newer generated_at, do nothing (defaults to mqtt.skip_if_fresh).",
        show_default=False,
    ),
):
    """Publish forecast JSON to MQTT using the same config format as the simulator."""

    config = _unwrap_typer_default(config)
    input = _unwrap_typer_default(input)
    mqtt_host = _unwrap_typer_default(mqtt_host)
    mqtt_port = _unwrap_typer_default(mqtt_port)
    mqtt_username = _unwrap_typer_default(mqtt_username)
    mqtt_password = _unwrap_typer_default(mqtt_password)
    base_topic = _unwrap_typer_default(base_topic)
    discovery_prefix = _unwrap_typer_default(discovery_prefix)
    connect_retries = _unwrap_typer_default(connect_retries)
    retry_delay = _unwrap_typer_default(retry_delay)
    verbose = _unwrap_typer_default(verbose)
    force = _unwrap_typer_default(force)
    publish_topics = _unwrap_typer_default(publish_topics)
    verify = _unwrap_typer_default(verify)
    publish_retries = _unwrap_typer_default(publish_retries)
    skip_if_fresh = _unwrap_typer_default(skip_if_fresh)

    config_path = _resolve_config_path(config)
    raw_cfg = _load_raw_config_dict(config_path)
    mqtt_section = raw_cfg.get("mqtt", {}) if isinstance(raw_cfg, dict) else {}

    effective_force = _coerce_bool(force, mqtt_section.get("force"), default=False)
    verify_flag = _coerce_bool(verify, mqtt_section.get("verify"), default=False)
    skip_if_fresh_flag = _coerce_bool(skip_if_fresh, mqtt_section.get("skip_if_fresh"), default=False)
    publish_retries_val = publish_retries if publish_retries is not None else mqtt_section.get("publish_retries")
    if publish_retries_val is None:
        publish_retries_val = 1
    publish_retries_val = int(publish_retries_val)

    # Reuse argparse-based merger by mimicking the Namespace shape.
    args = type(
        "Args",
        (),
        {
            "config": config_path,
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
            "force": effective_force,
            "no_state": no_state,
            "publish_topics": publish_topics,
            "verify": verify_flag,
            "publish_retries": publish_retries_val,
            # Let config decide by default; CLI can force-disable with --no-discovery.
            "publish_discovery": None if not no_discovery else False,
            "skip_if_fresh": skip_if_fresh_flag,
        },
    )()

    input_path, cfg = ha_mqtt._merge_config(args)
    debug_info: Dict[str, Any] = {}
    published = ha_mqtt.publish_forecast(
        input_path,
        cfg,
        force=effective_force,
        verify=verify_flag,
        publish_retries=publish_retries_val,
        retry_delay_sec=args.retry_delay or cfg.retry_delay_sec,
        skip_if_fresh=skip_if_fresh_flag,
        debug=debug_info,
    )
    if published:
        typer.echo("Published new forecast to MQTT.")
    else:
        typer.echo("No publish needed (unchanged or not newer).")
    if verbose and debug_info:
        typer.echo(f"MQTT debug: {debug_info}")

    return published


def main() -> None:  # pragma: no cover - thin wrapper for console_script
    app()


__all__ = ["app", "main", "default_weather_provider"]


if __name__ == "__main__":  # pragma: no cover
    main()
