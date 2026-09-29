from pathlib import Path
import json
import shutil
import subprocess

import pytest
from fastapi.testclient import TestClient

from app.main import app


def test_index_uses_local_plotly_and_exposes_core_controls():
    response = TestClient(app).get("/")

    assert response.status_code == 200
    html = response.text
    assert 'src="/static/vendor/plotly-2.35.2.min.js"' in html
    assert 'src="/static/loop-selection.js"' in html
    assert "cdn.plot.ly" not in html
    for element_id in [
        "fileInput", "datasetSelect", "calendar", "windowStartSlider",
        "signalMappingList", "segmentExportBtn", "dayChart",
        "tagSearch", "tagList", "tagHint", "clearTagSel",
        "dayHint", "mappingSearch", "loopSearch", "loopList", "loopHint",
    ]:
        assert f'id="{element_id}"' in html


def test_local_plotly_and_application_scripts_are_served():
    client = TestClient(app)

    plotly = client.get("/static/vendor/plotly-2.35.2.min.js")
    application = client.get("/static/app.js")
    loop_selection = client.get("/static/loop-selection.js")

    assert plotly.status_code == 200
    assert len(plotly.content) > 4_000_000
    assert application.status_code == 200
    assert loop_selection.status_code == 200
    assert b"saveSignalMappings" in application.content
    assert b"segmentExportBtn" in application.content
    assert b"selectAllTags" in application.content
    assert b"mappingDraft" in application.content
    assert b"renderLoopList" in application.content
    assert b"defaultTagsByLoop" in application.content
    assert b"LOOP_DEFAULT_CAP=12" in application.content
    assert b"['PV', 'SV', 'MV', 'OTHER']" in loop_selection.content
    assert b"loop:state.loop" in application.content
    assert b"defaultTagsByLoop" in loop_selection.content
    assert b"slice(0, cap)" in loop_selection.content
    assert b"function latestDataDate" in application.content
    assert b"state.selectedDate=latestDataDate()" in application.content
    assert b"else renderCalendar()" in application.content


def test_loop_selection_helper_prefers_one_signal_per_loop():
    node = shutil.which("node")
    if not node:
        pytest.skip("Node.js is required for the browser helper behavior test")
    script = f"""
global.window=global;
require({json.dumps(str(Path('app/static/loop-selection.js').resolve()))});
const signals=[];
for(let i=1;i<=15;i++){{
  signals.push({{tag:`L${{i}}.MV`,type:'MV'}});
  signals.push({{tag:`L${{i}}.PV`,type:'PV'}});
  signals.push({{tag:`L${{i}}.SV`,type:'SV'}});
}}
const picked=PidLoopSelection.defaultTagsByLoop(signals,{{types:['PV','SV','MV','OTHER'],cap:12}});
const selected=PidLoopSelection.defaultTagsByLoop(signals,{{types:['PV','SV','MV','OTHER'],loop:'L13',cap:12}});
console.log(JSON.stringify({{
  picked,
  loops:PidLoopSelection.listLoops(signals),
  unique:[...new Set(picked.map(PidLoopSelection.signalLoop))],
  selected
}}));
"""
    result = subprocess.run([node, "-e", script], check=True, capture_output=True, text=True)
    payload = json.loads(result.stdout)

    assert len(payload["loops"]) == 15
    assert len(payload["picked"]) == 12
    assert len(payload["unique"]) == 12
    assert all(tag.endswith(".PV") for tag in payload["picked"])
    assert set(payload["selected"]) == {"L13.PV", "L13.SV", "L13.MV"}


def test_loop_selection_helper_returns_all_signals_for_multiple_selected_loops():
    node = shutil.which("node")
    if not node:
        pytest.skip("Node.js is required for the browser helper behavior test")
    script = f"""
global.window=global;
require({json.dumps(str(Path('app/static/loop-selection.js').resolve()))});
const signals=[
  {{tag:'L1.PV',type:'PV'}},{{tag:'L1.MV',type:'MV'}},
  {{tag:'L2.PV',type:'PV'}},{{tag:'L2.SV',type:'SV'}},
  {{tag:'L3.PV',type:'PV'}}
];
console.log(JSON.stringify(PidLoopSelection.defaultTagsByLoop(signals,{{types:['PV','SV','MV','OTHER'],loops:['L1','L2'],cap:12}})));
"""
    result = subprocess.run([node, "-e", script], check=True, capture_output=True, text=True)
    assert set(json.loads(result.stdout)) == {"L1.PV", "L1.MV", "L2.PV", "L2.SV"}
