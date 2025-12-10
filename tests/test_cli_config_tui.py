from pathlib import Path

import pytest

from solarpredict.cli_config_tui import (
    EditorState,
    NodeRef,
    _render_tree,
    _render_details,
    _select_next,
    _select_prev,
)
from solarpredict.core.models import Location, PVArray, Site


def _site(idx: int, arrays: int) -> Site:
    return Site(
        id=f"s{idx}",
        location=Location(id=f"loc{idx}", lat=0, lon=0, tz="UTC"),
        arrays=[
            PVArray(
                id=f"a{idx}_{j}",
                tilt_deg=20,
                azimuth_deg=0,
                pdc0_w=5000,
                gamma_pdc=-0.004,
                dc_ac_ratio=1.1,
                eta_inv_nom=0.96,
                losses_percent=5.0,
                temp_model="close_mount_glass_glass",
            )
            for j in range(arrays)
        ],
    )


def test_render_tree_formats_selection():
    state = EditorState(sites=[_site(1, 2)], mqtt={}, run={}, selected=NodeRef("array", 0, 1))
    lines = _render_tree(state)
    assert any("a1_1" in line for _, line in lines)


def test_select_next_walks_hierarchy():
    state = EditorState(sites=[_site(1, 1), _site(2, 1)], mqtt={}, run={}, selected=NodeRef("site", 0))
    _select_next(state)
    assert state.selected == NodeRef("array", 0, 0)
    _select_next(state)
    assert state.selected == NodeRef("site", 1)


def test_select_prev_walks_backwards():
    state = EditorState(sites=[_site(1, 2)], mqtt={}, run={}, selected=NodeRef("array", 0, 0))
    _select_prev(state)
    assert state.selected == NodeRef("site", 0)
    state.selected = NodeRef("site", 0)
    _select_prev(state)  # stays at first
    assert state.selected == NodeRef("site", 0)


def test_select_prev_wraps_to_last_array():
    state = EditorState(sites=[_site(1, 2), _site(2, 1)], mqtt={}, run={}, selected=NodeRef("site", 1))
    _select_prev(state)
    assert state.selected == NodeRef("array", 0, 1)


def test_render_details_for_array():
    state = EditorState(sites=[_site(1, 1)], mqtt={}, run={}, selected=NodeRef("array", 0, 0))
    lines = _render_details(state)
    assert any("Array a1_0" in ln for ln in lines)
