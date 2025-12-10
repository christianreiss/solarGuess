"""Config TUI for `solarpredict config`.

Goals for this rewrite:
- Keep state transitions deterministic/testable (no reliance on a real terminal).
- Make the layout self-explanatory: left tree, right detail pane, sticky footer.
- Provide shortcuts a nerdy-but-impatient user will actually find (Aunty Snarks friendly).
"""
from __future__ import annotations

import json
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Callable, List, Optional

from prompt_toolkit import Application
from prompt_toolkit.key_binding import KeyBindings
from prompt_toolkit.layout import HSplit, VSplit, Layout
from prompt_toolkit.layout.controls import FormattedTextControl
from prompt_toolkit.layout.dimension import D
from prompt_toolkit.layout.margins import ScrollbarMargin
from prompt_toolkit.layout.containers import Window, ConditionalContainer, WindowAlign
from prompt_toolkit.shortcuts import button_dialog, input_dialog, message_dialog, yes_no_dialog
from prompt_toolkit.styles import Style

from solarpredict.core.config import ConfigError, load_scenario
from solarpredict.core.debug import JsonlDebugWriter, NullDebugCollector
from solarpredict.core.models import Location, PVArray, Scenario, Site, ValidationError
from solarpredict.cli_utils import scenario_to_dict, write_scenario, load_mqtt, load_run


@dataclass(frozen=True)
class NodeRef:
    level: str  # "site" | "array"
    site_idx: int
    array_idx: Optional[int] = None


@dataclass
class EditorState:
    sites: List[Site]
    mqtt: dict
    run: dict
    selected: Optional[NodeRef] = None
    dirty: bool = False
    message: str = "↑/↓ move • Enter edit • a add site • A add array • m mqtt • r run • Ctrl+S save"

    def clone(self) -> "EditorState":
        return replace(
            self,
            sites=list(self.sites),
            selected=self.selected,
            dirty=self.dirty,
            message=self.message,
        )


def _initial_state(path: Path, debug) -> EditorState:
    try:
        if path.exists():
            scenario = load_scenario(path)
            mqtt = load_mqtt(path)
            run_cfg = load_run(path)
            debug.emit("config.load", {"path": str(path), "sites": len(scenario.sites)}, ts=None)
            return EditorState(sites=list(scenario.sites), mqtt=mqtt, run=run_cfg, selected=NodeRef("site", 0) if scenario.sites else None)
    except ConfigError as exc:
        debug.emit("config.load_error", {"path": str(path), "error": str(exc)}, ts=None)
    return EditorState(sites=[], mqtt={}, run={}, selected=None)


def _render_tree(state: EditorState) -> List[tuple[str, str]]:
    lines: List[tuple[str, str]] = []
    if not state.sites:
        lines.append(("class:text.dim", "➖ no sites yet (press a to add)"))
        return lines
    for s_idx, site in enumerate(state.sites):
        is_selected = state.selected and state.selected.level == "site" and state.selected.site_idx == s_idx
        prefix = "▶ " if is_selected else "  "
        lines.append(("class:text.site", f"{prefix}{site.id} · {len(site.arrays)} arrays"))
        for a_idx, arr in enumerate(site.arrays):
            is_arr = state.selected and state.selected.level == "array" and state.selected.site_idx == s_idx and state.selected.array_idx == a_idx
            prefix = "    ▶ " if is_arr else "      "
            lines.append(("class:text.array", f"{prefix}{arr.id} · tilt {arr.tilt_deg}° · az {arr.azimuth_deg}°"))
    return lines


def _render_mqtt_summary(mqtt: dict) -> str:
    if not mqtt:
        return "(mqtt not set)"
    host = mqtt.get("host", "?")
    base = mqtt.get("base_topic", "?")
    state = mqtt.get("publish_state", True)
    topics = mqtt.get("publish_topics", False)
    return f"mqtt: {host} • base {base} • state {'on' if state else 'off'} • topics {'on' if topics else 'off'}"


def _render_run_summary(run: dict) -> str:
    if not run:
        return "run: (default)"
    ts = run.get("timestep", "1h")
    fmt = run.get("format", "json")
    return f"run: timestep {ts} • format {fmt}"


def _ensure_selected(state: EditorState) -> None:
    if state.selected is None and state.sites:
        state.selected = NodeRef("site", 0)


def _select_next(state: EditorState) -> None:
    _ensure_selected(state)
    if state.selected is None:
        return
    sel = state.selected
    if sel.level == "site":
        # move into first array if exists, else next site
        site = state.sites[sel.site_idx]
        if site.arrays:
            state.selected = NodeRef("array", sel.site_idx, 0)
            return
        if sel.site_idx + 1 < len(state.sites):
            state.selected = NodeRef("site", sel.site_idx + 1)
            return
    else:  # array
        if sel.array_idx is None:
            return
        if sel.array_idx + 1 < len(state.sites[sel.site_idx].arrays):
            state.selected = NodeRef("array", sel.site_idx, sel.array_idx + 1)
            return
        if sel.site_idx + 1 < len(state.sites):
            state.selected = NodeRef("site", sel.site_idx + 1)


def _select_prev(state: EditorState) -> None:
    _ensure_selected(state)
    if state.selected is None:
        return
    sel = state.selected
    if sel.level == "array":
        if sel.array_idx is not None and sel.array_idx > 0:
            state.selected = NodeRef("array", sel.site_idx, sel.array_idx - 1)
            return
        state.selected = NodeRef("site", sel.site_idx)
        return
    if sel.level == "site" and sel.site_idx > 0:
        prev_site = state.sites[sel.site_idx - 1]
        if prev_site.arrays:
            state.selected = NodeRef("array", sel.site_idx - 1, len(prev_site.arrays) - 1)
        else:
            state.selected = NodeRef("site", sel.site_idx - 1)


def _prompt_location(existing: Optional[Location]) -> Optional[Location]:
    def ask(label: str, default: str) -> Optional[str]:
        return input_dialog(title="Location", text=label, default=default).run(in_thread=True)

    lat = ask("Latitude", str(existing.lat if existing else 0.0))
    if lat is None:
        return None
    lon = ask("Longitude", str(existing.lon if existing else 0.0))
    if lon is None:
        return None
    tz = ask("Timezone", existing.tz if existing else "auto")
    if tz is None:
        return None
    elev = ask("Elevation meters (blank to skip)", str(existing.elevation_m) if existing and existing.elevation_m is not None else "")
    if elev is None:
        return None
    loc_id = ask("Location id", existing.id if existing else "loc1")
    if loc_id is None:
        return None
    elevation_m = float(elev) if elev.strip() else None
    try:
        return Location(id=loc_id, lat=float(lat), lon=float(lon), tz=tz, elevation_m=elevation_m)
    except ValidationError as exc:
        message_dialog(title="Invalid location", text=str(exc)).run()
        return _prompt_location(existing)


def _prompt_array(existing: Optional[PVArray]) -> Optional[PVArray]:
    def ask(label: str, default: str) -> Optional[str]:
        return input_dialog(title="Array", text=label, default=default).run(in_thread=True)

    arr_id = ask("Array id", existing.id if existing else "array1")
    if arr_id is None:
        return None
    def pf(label: str, value: float) -> float:
        raw = ask(label, str(value))
        if raw is None:
            raise KeyboardInterrupt
        return float(raw)

    try:
        tilt = pf("Tilt deg", existing.tilt_deg if existing else 30.0)
        az = pf("Azimuth deg", existing.azimuth_deg if existing else 0.0)
        pdc0 = pf("pdc0_w", existing.pdc0_w if existing else 5000.0)
        gamma = pf("gamma_pdc", existing.gamma_pdc if existing else -0.004)
        dcac = pf("dc_ac_ratio", existing.dc_ac_ratio if existing else 1.1)
        eta = pf("eta_inv_nom", existing.eta_inv_nom if existing else 0.96)
        losses = pf("losses_percent", existing.losses_percent if existing else 5.0)
    except KeyboardInterrupt:
        return None
    temp_model = ask("temp_model", existing.temp_model if existing else "close_mount_glass_glass")
    if temp_model is None:
        return None
    inv_group = ask("Inverter group id (blank=none)", existing.inverter_group_id if existing else "")
    if inv_group is None:
        return None
    inv_pdc = ask("Inverter pdc0_w (blank=derive)", str(existing.inverter_pdc0_w) if existing and existing.inverter_pdc0_w is not None else "")
    if inv_pdc is None:
        return None
    inverter_pdc0_w = float(inv_pdc) if inv_pdc.strip() else None
    try:
        return PVArray(
            id=arr_id,
            tilt_deg=tilt,
            azimuth_deg=az,
            pdc0_w=pdc0,
            gamma_pdc=gamma,
            dc_ac_ratio=dcac,
            eta_inv_nom=eta,
            losses_percent=losses,
            temp_model=temp_model,
            inverter_group_id=inv_group.strip() or None,
            inverter_pdc0_w=inverter_pdc0_w,
        )
    except ValidationError as exc:
        message_dialog(title="Invalid array", text=str(exc)).run()
        return _prompt_array(existing)


def _prompt_site(existing: Optional[Site]) -> Optional[Site]:
    site_id = input_dialog(title="Site", text="Site id", default=existing.id if existing else "site1").run(in_thread=True)
    if site_id is None:
        return None
    loc = _prompt_location(existing.location if existing else None)
    if loc is None:
        return None
    arrays: List[PVArray] = list(existing.arrays) if existing else []
    if not arrays:
        arr = _prompt_array(None)
        if arr:
            arrays.append(arr)
    while True:
        action = button_dialog(
            title="Arrays",
            text="Manage arrays",
            buttons=[("Add", "add"), ("Edit", "edit"), ("Delete", "del"), ("Done", "done")],
        ).run()
        if action == "add":
            arr = _prompt_array(None)
            if arr:
                arrays.append(arr)
        elif action == "edit" and arrays:
            names = [a.id for a in arrays]
            choice = button_dialog(title="Choose array", text="Edit which array?", buttons=[(n, n) for n in names]).run()
            if choice:
                idx = names.index(choice)
                edited = _prompt_array(arrays[idx])
                if edited:
                    arrays[idx] = edited
        elif action == "del" and arrays:
            names = [a.id for a in arrays]
            choice = button_dialog(title="Choose array", text="Delete which array?", buttons=[(n, n) for n in names]).run()
            if choice:
                arrays = [a for a in arrays if a.id != choice]
        else:
            break
    try:
        return Site(id=site_id, location=loc, arrays=arrays)
    except ValidationError as exc:
        message_dialog(title="Invalid site", text=str(exc)).run()
        return _prompt_site(existing)


def _save(path: Path, state: EditorState, debug) -> bool:
    try:
        scenario = Scenario(sites=state.sites)
    except ValidationError as exc:
        message_dialog(title="Invalid scenario", text=str(exc)).run()
        return False
    tmp = path.with_suffix(path.suffix + ".tmp")
    write_scenario(tmp, scenario, mqtt=state.mqtt, run=state.run)
    tmp.replace(path)
    state.dirty = False
    debug.emit("config.save", {"path": str(path), "sites": len(state.sites)}, ts=None)
    message_dialog(title="Saved", text=f"Scenario written to {path}").run()
    state.message = f"Saved → {path}"
    return True


def _edit_mqtt(state: EditorState) -> None:
    fields = [
        ("host", "MQTT host", state.mqtt.get("host", "")),
        ("port", "Port", str(state.mqtt.get("port", 1883))),
        ("username", "Username", state.mqtt.get("username", "")),
        ("password", "Password", state.mqtt.get("password", "")),
        ("base_topic", "Base topic", state.mqtt.get("base_topic", "solarguess")),
        ("discovery_prefix", "Discovery prefix", state.mqtt.get("discovery_prefix", "homeassistant")),
        ("input", "Input path", state.mqtt.get("input", "live_results.json")),
        ("connect_retries", "Connect retries", str(state.mqtt.get("connect_retries", 3))),
        ("retry_delay", "Retry delay (sec)", str(state.mqtt.get("retry_delay", 1.0))),
    ]
    new_mqtt = dict(state.mqtt)
    for key, label, default in fields:
        val = input_dialog(title="MQTT", text=label, default=str(default)).run(in_thread=True)
        if val is None:
            return
        if key in {"port", "connect_retries"}:
            try:
                new_mqtt[key] = int(val)
            except ValueError:
                message_dialog(title="Invalid", text=f"{label} must be an integer").run()
                return
        elif key == "retry_delay":
            try:
                new_mqtt[key] = float(val)
            except ValueError:
                message_dialog(title="Invalid", text=f"{label} must be a number").run()
                return
        else:
            new_mqtt[key] = val

    def ask_bool(label: str, current: bool) -> bool:
        choice = button_dialog(title="MQTT", text=label, buttons=[("Yes", True), ("No", False)], default=current).run()
        return bool(choice)

    # Keep backward-compatible: bool enables default topics; structured dict allows fine control.
    new_mqtt["publish_topics"] = ask_bool("Publish scalar topics?", bool(new_mqtt.get("publish_topics", False)))
    new_mqtt["publish_state"] = ask_bool("Publish retained state blob?", bool(new_mqtt.get("publish_state", True)))
    new_mqtt["publish_discovery"] = ask_bool("Publish HA discovery?", bool(new_mqtt.get("publish_discovery", True)))
    new_mqtt["verbose"] = ask_bool("Verbose logging?", bool(new_mqtt.get("verbose", False)))

    state.mqtt = new_mqtt
    state.dirty = True
    state.message = "MQTT updated"


def _edit_run(state: EditorState) -> None:
    fields = [
        ("timestep", "Timestep (e.g., 1h, 15m)", state.run.get("timestep", "1h")),
        ("format", "Output format (json/csv)", state.run.get("format", "json")),
        ("output", "Output path", state.run.get("output", "results.json")),
    ]
    new_run = dict(state.run)
    for key, label, default in fields:
        val = input_dialog(title="Run", text=label, default=str(default)).run(in_thread=True)
        if val is None:
            return
        new_run[key] = val

    def ask_bool(label: str, current: bool) -> bool:
        choice = button_dialog(title="Run", text=label, buttons=[("Yes", True), ("No", False)], default=current).run()
        return bool(choice)

    new_run["qc_pvgis"] = ask_bool("Enable PVGIS QC?", bool(new_run.get("qc_pvgis", False)))
    state.run = new_run
    state.dirty = True
    state.message = "Run settings updated"


def _render_details(state: EditorState) -> List[str]:
    """Describe the currently selected node in a human-readable way."""
    _ensure_selected(state)
    if state.selected is None:
        return ["No selection", "Add a site with 'a' to get started."]
    sel = state.selected
    site = state.sites[sel.site_idx]
    lines: List[str] = [f"Site {site.id}", f"lat {site.location.lat} • lon {site.location.lon} • tz {site.location.tz}"]
    if sel.level == "site":
        lines.append(f"Arrays: {len(site.arrays)}")
        lines.append("Press A to add array, Enter to edit site, x to delete")
        return lines
    if sel.array_idx is None:
        return lines
    arr = site.arrays[sel.array_idx]
    lines.extend(
        [
            f"Array {arr.id}",
            f"Tilt {arr.tilt_deg}° • Az {arr.azimuth_deg}°",
            f"Pdc0 {arr.pdc0_w} W • gamma {arr.gamma_pdc}",
            f"dc/ac {arr.dc_ac_ratio} • inverter η {arr.eta_inv_nom}",
            f"Losses {arr.losses_percent}% • temp {arr.temp_model}",
            f"Inverter group: {arr.inverter_group_id or '—'}",
        ]
    )
    if arr.inverter_pdc0_w is not None:
        lines.append(f"Inverter pdc0_w: {arr.inverter_pdc0_w} W")
    lines.append("Enter to edit array · x to delete")
    return lines


def _update_message(state: EditorState, text: str) -> None:
    state.message = text


def launch_config_tui(path: Path, debug_path: Optional[Path] = None) -> None:
    debug = JsonlDebugWriter(debug_path) if debug_path else NullDebugCollector()
    state = _initial_state(path, debug)

    kb = KeyBindings()

    @kb.add("c-c")
    def _(event):
        if state.dirty:
            if yes_no_dialog(title="Quit", text="Discard unsaved changes?").run():
                event.app.exit(result=None)
        else:
            event.app.exit(result=None)

    @kb.add("c-s")
    def _(event):
        _save(path, state, debug)
        event.app.invalidate()

    @kb.add("down")
    def _(event):
        _select_next(state)
        event.app.invalidate()

    @kb.add("up")
    def _(event):
        _select_prev(state)
        event.app.invalidate()

    @kb.add("a")
    def _(event):
        site = _prompt_site(None)
        if site:
            state.sites.append(site)
            state.selected = NodeRef("site", len(state.sites) - 1)
            state.dirty = True
            _update_message(state, f"Added site {site.id}")
            event.app.invalidate()

    @kb.add("A")
    def _(event):
        _ensure_selected(state)
        if state.selected is None:
            message_dialog(title="No site", text="Select a site first (use ↑/↓ or press a to add).").run()
            return
        site = state.sites[state.selected.site_idx]
        arr = _prompt_array(None)
        if arr:
            site.arrays.append(arr)
            state.selected = NodeRef("array", state.selected.site_idx, len(site.arrays) - 1)
            state.dirty = True
            _update_message(state, f"Added array {arr.id} to {site.id}")
            event.app.invalidate()

    def edit_selected():
        if state.selected is None:
            return False
        if state.selected.level == "site":
            site = state.sites[state.selected.site_idx]
            edited = _prompt_site(site)
            if edited:
                state.sites[state.selected.site_idx] = edited
                state.dirty = True
                _update_message(state, f"Updated site {edited.id}")
            return True
        site = state.sites[state.selected.site_idx]
        if state.selected.array_idx is None:
            return False
        arr = site.arrays[state.selected.array_idx]
        edited = _prompt_array(arr)
        if edited:
            new_arrays = list(site.arrays)
            new_arrays[state.selected.array_idx] = edited
            state.sites[state.selected.site_idx] = Site(id=site.id, location=site.location, arrays=new_arrays)
            state.dirty = True
            _update_message(state, f"Updated array {edited.id}")
        return True

    @kb.add("e")
    def _(event):
        if edit_selected():
            event.app.invalidate()

    @kb.add("enter")
    def _(event):
        if edit_selected():
            event.app.invalidate()

    @kb.add("m")
    def _(event):
        _edit_mqtt(state)
        event.app.invalidate()

    @kb.add("r")
    def _(event):
        _edit_run(state)
        event.app.invalidate()

    @kb.add("d")
    def _(event):
        if state.selected is None:
            return
        if state.selected.level == "site":
            if yes_no_dialog(title="Delete site", text="Delete this site?").run():
                del state.sites[state.selected.site_idx]
                state.selected = None
                state.dirty = True
                _update_message(state, "Site deleted")
        else:
            site = state.sites[state.selected.site_idx]
            if state.selected.array_idx is None:
                return
            if yes_no_dialog(title="Delete array", text=f"Delete array {site.arrays[state.selected.array_idx].id}?").run():
                new_arrays = [a for idx, a in enumerate(site.arrays) if idx != state.selected.array_idx]
                state.sites[state.selected.site_idx] = Site(id=site.id, location=site.location, arrays=new_arrays)
                state.selected = NodeRef("site", state.selected.site_idx)
                state.dirty = True
                _update_message(state, "Array deleted")
        event.app.invalidate()

    def get_body():
        lines = _render_tree(state)
        text = [(style, line + "\n") for style, line in lines]
        return Window(
            content=FormattedTextControl(text),
            wrap_lines=False,
            right_margins=[ScrollbarMargin(display_arrows=True)],
            height=D(weight=1),
        )

    help_text = lambda: f"{'DIRTY • ' if state.dirty else ''}{state.message}"
    detail_text = lambda: "\n".join(_render_details(state))

    root_container = HSplit(
        [
            Window(
                height=1,
                content=FormattedTextControl(lambda: f"SolarPredict config • {path}"),
                align=WindowAlign.CENTER,
            ),
            VSplit(
                [
                    HSplit(
                        [
                            Window(height=1, content=FormattedTextControl(lambda: _render_mqtt_summary(state.mqtt))),
                            Window(height=1, content=FormattedTextControl(lambda: _render_run_summary(state.run))),
                            get_body(),
                        ],
                        width=D(weight=3),
                    ),
                    Window(width=1, char="│", style="class:sep"),
                    HSplit(
                        [
                            Window(height=1, content=FormattedTextControl("Details")),
                            Window(
                                content=FormattedTextControl(detail_text),
                                wrap_lines=True,
                                height=D(weight=1),
                            ),
                        ],
                        width=D(weight=2),
                    ),
                ]
            ),
            Window(height=1, content=FormattedTextControl(lambda: "Help: Enter edit • a add site • A add array • x delete • m mqtt • r run • Ctrl+S save • Ctrl+C quit")),
            Window(height=1, content=FormattedTextControl(help_text), style="class:status"),
        ]
    )

    app = Application(
        layout=Layout(root_container),
        key_bindings=kb,
        full_screen=True,
        mouse_support=True,
        style=Style.from_dict(
            {
                "text.site": "bold",
                "text.array": "",
                "text.dim": "fg:#666666",
                "status": "reverse",
                "sep": "fg:#444444",
            }
        ),
    )

    app.run()


__all__ = [
    "launch_config_tui",
    "EditorState",
    "NodeRef",
    "_render_tree",
    "_select_next",
    "_select_prev",
    "_render_details",
]
