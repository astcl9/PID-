import pandas as pd
from app.importer import normalize_file, classify_signal, detect_csv_encoding, read_csv_chunks

def test_classify_signal():
    assert classify_signal("TIC.PV") == "PV"
    assert classify_signal("TIC_SV") == "SV"
    assert classify_signal("TIC-MV") == "MV"

def test_normalize_csv(tmp_path):
    p = tmp_path / "x.csv"
    pd.DataFrame({"time":["2026-01-01 00:00", "2026-01-01 00:01"], "A.PV":[1,2], "A.SV":[1,1], "A.MV":[3,4]}).to_csv(p,index=False)
    _, frame, report = normalize_file(p)
    assert len(frame) == 6
    assert report["signals"]


def test_gb18030_csv_with_chinese_headers(tmp_path):
    p = tmp_path / "gbk.csv"
    text = "时间,回路1.PV,回路1.SV,回路1.MV\n2026-01-01 00:00,1,2,3\n"
    p.write_bytes(text.encode("gb18030"))
    assert detect_csv_encoding(p) == "gb18030"
    _, frame, report = normalize_file(p)
    assert len(frame) == 3
    assert {item["tag"] for item in report["signals"]} == {"回路1.PV", "回路1.SV", "回路1.MV"}


def test_utf8_bom_and_utf16_csv(tmp_path):
    frame = pd.DataFrame({"时间": ["2026-01-01 00:00"], "TIC.PV": [1]})
    bom = tmp_path / "bom.csv"
    frame.to_csv(bom, index=False, encoding="utf-8-sig")
    assert detect_csv_encoding(bom) == "utf-8-sig"
    assert len(read_csv_chunks(bom)) == 1

    utf16 = tmp_path / "utf16.csv"
    utf16.write_bytes("时间,TIC.PV\n2026-01-01 00:00,1\n".encode("utf-16"))
    assert detect_csv_encoding(utf16) == "utf-16"
    _, normalized, _ = normalize_file(utf16)
    assert len(normalized) == 1


def test_rejects_abnormally_long_csv_header_before_allocation(tmp_path):
    p = tmp_path / "malformed.csv"
    p.write_bytes(b"timestamp," + b"x," * (1024 * 1024) + b"\n")
    try:
        read_csv_chunks(p, chunksize=10)
    except ValueError as exc:
        assert "CSV 首行超过" in str(exc)
    else:
        raise AssertionError("malformed CSV should be rejected before pandas allocation")


def test_wide_csv_uses_smaller_input_batches(tmp_path):
    p = tmp_path / "wide.csv"
    frame = pd.DataFrame({"timestamp": ["2026-01-01 00:00"] * 600,
                          **{f"L{i}.PV": [i] * 600 for i in range(200)}})
    frame.to_csv(p, index=False)
    reader = read_csv_chunks(p, chunksize=25000)
    chunks = list(reader)
    assert len(chunks) == 2 and len(chunks[0]) <= 502
    # 200 signal columns cap a requested 25,000-row batch to 502 rows.
    assert reader is not None
