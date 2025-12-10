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
from typing import Callable, List, Optional

import typer
import yaml

from solarpredict.core.config import ConfigError, load_scenario
from solarpredict.core import config as config_mod
from solarpredict.core.debug import JsonlDebugWriter, NullDebugCollector
from solarpredict.core.models import Location, PVArray, Scenario, Site, ValidationError
from solarpredict.engine.simulate import simulate_day
from solarpredict.weather.open_meteo import OpenMeteoWeatherProvider
from solarpredict.weather.pvgis import PVGISWeatherProvider
from solarpredict.weather.composite import CompositeWeatherProvider
from solarpredict.integrations import ha_mqtt

__version__ = "0.1.0"

app = typer.Typer(add_completion=False, help="Solar generation predictor CLI")


def default_weather_provider(debug) -> OpenMeteoWeatherProvider:
    """Factory separated for easy monkeypatching in tests."""

    return OpenMeteoWeatherProvider(debug=debug)


def _exit_with_error(msg: str) -> None:
    typer.echo(f"Error: {msg}", err=True)
    raise typer.Exit(code=1)


def _scenario_to_dict(scenario: Scenario) -> dict:
    def loc_dict(loc: Location) -> dict:
        return {
            "id": loc.id,
            "lat": loc.lat,
            "lon": loc.lon,
            "tz": loc.tz,
            "elevation_m": loc.elevation_m,
        }

    def arr_dict(arr: PVArray) -> dict:
        return {
            "id": arr.id,
            "tilt_deg": arr.tilt_deg,
            "azimuth_deg": arr.azimuth_deg,
            "pdc0_w": arr.pdc0_w,
            "gamma_pdc": arr.gamma_pdc,
            "dc_ac_ratio": arr.dc_ac_ratio,
            "eta_inv_nom": arr.eta_inv_nom,
            "losses_percent": arr.losses_percent,
            "temp_model": arr.temp_model,
        }

    return {
        "sites": [
            {
                "id": site.id,
                "location": loc_dict(site.location),
                "arrays": [arr_dict(arr) for arr in site.arrays],
            }
            for site in scenario.sites
        ]
    }


def _write_scenario(path: Path, scenario: Scenario) -> None:
    data = _scenario_to_dict(scenario)
    if path.suffix.lower() in {".yaml", ".yml", ""}:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(yaml.safe_dump(data, sort_keys=False))
    elif path.suffix.lower() == ".json":
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(data, indent=2, sort_keys=False))
    else:
        _exit_with_error(f"Unsupported config extension: {path.suffix}")


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

    arr_id = typer.prompt("Array id", default=existing.id if existing else "array1")
    tilt_deg = prompt_float("Tilt deg", existing.tilt_deg if existing else 30.0)
    azimuth_deg = prompt_float("Azimuth deg", existing.azimuth_deg if existing else 0.0)
    pdc0_w = prompt_float("pdc0_w", existing.pdc0_w if existing else 5000.0)
    gamma_pdc = prompt_float("gamma_pdc", existing.gamma_pdc if existing else -0.004)
    dc_ac_ratio = prompt_float("dc_ac_ratio", existing.dc_ac_ratio if existing else 1.1)
    eta_inv_nom = prompt_float("eta_inv_nom", existing.eta_inv_nom if existing else 0.96)
    losses_percent = prompt_float("losses_percent", existing.losses_percent if existing else 5.0)
    temp_model = typer.prompt("temp_model", default=existing.temp_model if existing else "close_mount_glass_glass")
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
    if not path.exists():
        return []
    try:
        scenario = load_scenario(path)
        typer.echo(f"Loaded existing scenario with {len(scenario.sites)} site(s)")
        return list(scenario.sites)
    except ConfigError as exc:
        typer.echo(f"Could not load existing config: {exc}", err=True)
        return []


@app.command()
def run(
    config: Path = typer.Option(Path("etc/config.yaml"), exists=True, readable=True, help="Scenario YAML/JSON file"),
    date: str = typer.Option(..., help="Target date (YYYY-MM-DD)"),
    timestep: str = typer.Option("1h", help="Forecast timestep, e.g. 1h or 15m"),
    weather_label: str = typer.Option(
        "end",
        help="Meaning of weather timestamps: 'end' (backward-averaged, default), 'start' (forward-averaged), or 'center'.",
    ),
    weather_source: str = typer.Option(
        "open-meteo",
        help="Weather provider: 'open-meteo' (default), 'pvgis-tmy' (typical meteorological year), or 'composite' (open-meteo primary with PVGIS fallback).",
    ),
    pvgis_cache_dir: Optional[Path] = typer.Option(
        None,
        help="Directory to cache PVGIS TMY responses (keyed by lat/lon). Only used when --weather-source=pvgis-tmy.",
    ),
    qc_pvgis: Optional[bool] = typer.Option(
        None,
        help="Compare forecast against PVGIS TMY baseline (sanity check). If omitted, falls back to config run.qc_pvgis (default false).",
    ),
    debug: Optional[Path] = typer.Option(None, help="Write debug JSONL to this path"),
    format: str = typer.Option("json", "--format", "-f", help="Output format: json or csv"),
    output: Optional[Path] = typer.Option(None, help="Output file path; defaults to results.<format>"),
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

    result = simulate_day(
        scenario,
        date=date_obj,
        timestep=timestep,
        weather_provider=provider,
        debug=debug_collector,
        weather_label=weather_label,
    )

    # Optional PVGIS QC sanity check: compare forecast energy/POA vs climatology.
    qc_enabled = qc_pvgis if qc_pvgis is not None else (raw_cfg.get("run", {}).get("qc_pvgis") if raw_cfg else False)
    if qc_enabled:
        qc_provider = PVGISWeatherProvider(debug=debug_collector, cache_dir=pvgis_cache_dir)
        qc_weather = qc_provider.get_forecast(
            [{"id": site.id, "lat": site.location.lat, "lon": site.location.lon} for site in scenario.sites],
            start=date_obj.isoformat(),
            end=(date_obj + dt.timedelta(days=1)).isoformat(),
            timestep="1h",
        )
        warnings = []
        for site in scenario.sites:
            qc_df = qc_weather[str(site.id)]
            # align to forecast window and aggregate POA energy baseline
            # reuse solar + pipeline by re-running simulate_day with qc provider? heavy; just integrate POA proxy.
            # Using ghi_wm2 + dni/dhi would need geometry; instead compute baseline POA via scaling ghi ~ poa
            # keep it simple: compare ghi and temp proxies
            # forecast data is already in result.timeseries
            for arr in site.arrays:
                key = (site.id, arr.id)
                forecast_ts = result.timeseries[key]
                # Energy proxies
                forecast_ghi = forecast_ts["poa_global"]
                baseline_ghi = qc_df["ghi_wm2"].reindex(forecast_ghi.index, method="nearest")
                ratio = float(forecast_ghi.sum() / baseline_ghi.sum()) if baseline_ghi.sum() else float("inf")
                debug_collector.emit(
                    "qc.pvgis_compare",
                    {"site": site.id, "array": arr.id, "ratio": ratio},
                    ts=forecast_ghi.index[0] if len(forecast_ghi.index) else None,
                )
                # Basic heuristic thresholds
                cloudy = forecast_ghi.mean() < 150  # W/m2 average rough heuristic
                low, high = (0.3, 2.0) if cloudy else (0.6, 1.6)
                if ratio < low or ratio > high:
                    warnings.append(f"{site.id}/{arr.id} PVGIS ratio {ratio:.2f} outside [{low},{high}] (cloudy={cloudy})")
        for w in warnings:
            typer.echo(f"QC warning: {w}", err=True)

    daily = result.daily
    output_path = output or Path(f"results.{format}")

    fmt = format.lower()
    if fmt == "json":
        serializable = daily.copy()
        for col in serializable.columns:
            if str(serializable[col].dtype).startswith("datetime64"):
                serializable[col] = serializable[col].astype(str)
        output_path.write_text(serializable.to_json(orient="records", indent=2))
    elif fmt == "csv":
        daily.to_csv(output_path, index=False)
    else:
        _exit_with_error("format must be json or csv")

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


@app.command()
def config(path: Path = typer.Argument(..., help="Path to save scenario YAML/JSON")) -> None:
    """Interactive scenario builder/editor."""

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
