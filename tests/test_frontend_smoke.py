from fastapi.testclient import TestClient

from app.main import app


def test_index_uses_local_plotly_and_exposes_core_controls():
    response = TestClient(app).get("/")

    assert response.status_code == 200
    html = response.text
    assert 'src="/static/vendor/plotly-2.35.2.min.js"' in html
    assert "cdn.plot.ly" not in html
    for element_id in [
        "fileInput", "datasetSelect", "calendar", "windowStartSlider",
        "signalMappingList", "segmentExportBtn", "dayChart",
    ]:
        assert f'id="{element_id}"' in html


def test_local_plotly_and_application_scripts_are_served():
    client = TestClient(app)

    plotly = client.get("/static/vendor/plotly-2.35.2.min.js")
    application = client.get("/static/app.js")

    assert plotly.status_code == 200
    assert len(plotly.content) > 4_000_000
    assert application.status_code == 200
    assert b"saveSignalMappings" in application.content
    assert b"segmentExportBtn" in application.content
