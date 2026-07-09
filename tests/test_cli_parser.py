"""CLI-product parser tests against REAL recorded product texts (tests/fixtures/cli/).

Fixtures were fetched live from api.weather.gov on 2026-07-09: one final (next-morning)
and one intermediate ("AS OF") report per city. The parser is the most fragile part of the
pipeline, so these are deliberately real texts, not synthetic ones.
"""

from datetime import date, datetime
from pathlib import Path

import pytest

from kalshi_weather.cli_parser import CLIParseError, parse_cli_product

FIXTURES = Path(__file__).parent / "fixtures" / "cli"

# station slug -> cli_site_name exactly as configured in config/stations.yaml
SITE_NAMES = {
    "nyc": "CENTRAL PARK",
    "chi": "CHICAGO-MIDWAY",
    "aus": "AUSTIN BERGSTROM",
    "den": "DENVER",
    "mia": "MIAMI",
    "phl": "PHILADELPHIA",
}


def load(name: str) -> str:
    return (FIXTURES / name).read_text()


@pytest.mark.parametrize("slug", SITE_NAMES)
def test_final_reports_parse_for_every_city(slug):
    report = parse_cli_product(load(f"{slug}_final.txt"), SITE_NAMES[slug])
    assert report.obs_date == date(2026, 7, 8)  # finals fetched 7/9 cover 7/8
    assert not report.is_intermediate

    by_var = {v.variable: v for v in report.values}
    assert "max_temp" in by_var and "min_temp" in by_var
    max_t, min_t = by_var["max_temp"].value, by_var["min_temp"].value
    assert max_t is not None and min_t is not None
    # sanity: July max/min temps for these cities in a plausible band
    assert 40 <= max_t <= 120
    assert 30 <= min_t <= 100
    assert max_t >= min_t
    assert by_var["max_temp"].unit == "degF"


@pytest.mark.parametrize("slug", SITE_NAMES)
def test_intermediate_reports_flagged(slug):
    report = parse_cli_product(load(f"{slug}_intermediate.txt"), SITE_NAMES[slug])
    assert report.is_intermediate
    assert report.obs_date == date(2026, 7, 9)


def test_nyc_final_exact_values():
    # spot-checked by eye against tests/fixtures/cli/nyc_final.txt
    report = parse_cli_product(load("nyc_final.txt"), "CENTRAL PARK")
    by_var = {v.variable: v for v in report.values}
    assert by_var["max_temp"].value == 85.0
    assert by_var["max_temp"].value_time == datetime(2026, 7, 8, 15, 47)
    assert by_var["min_temp"].value == 64.0
    assert by_var["min_temp"].value_time == datetime(2026, 7, 8, 4, 40)
    assert by_var["precip"].value == 0.0
    assert by_var["snowfall"].value == 0.0


def test_colon_style_occurrence_times_parse():
    # LOT/EWX write "2:56 PM" where OKX/BOU write "347 PM" — both must parse
    chi = parse_cli_product(load("chi_final.txt"), "CHICAGO-MIDWAY")
    by_var = {v.variable: v for v in chi.values}
    assert by_var["max_temp"].value_time == datetime(2026, 7, 8, 14, 56)
    assert by_var["min_temp"].value_time == datetime(2026, 7, 8, 4, 31)
    aus = parse_cli_product(load("aus_final.txt"), "AUSTIN BERGSTROM")
    aus_max = next(v for v in aus.values if v.variable == "max_temp")
    assert aus_max.value_time == datetime(2026, 7, 8, 15, 38)


def test_record_suffix_on_value_parses():
    # real product from 2026-07-02: NYC hit 100F, printed as "100R" (record set/tied)
    report = parse_cli_product(load("nyc_record.txt"), "CENTRAL PARK")
    by_var = {v.variable: v for v in report.values}
    assert by_var["max_temp"].value == 100.0
    assert by_var["max_temp"].value_time == datetime(2026, 7, 2, 14, 47)
    assert by_var["min_temp"].value == 82.0


def test_trace_precip_maps_to_zero_with_flag():
    # nyc intermediate has "  TODAY            T" in PRECIPITATION
    report = parse_cli_product(load("nyc_intermediate.txt"), "CENTRAL PARK")
    precip = next(v for v in report.values if v.variable == "precip")
    assert precip.value == 0.0
    assert precip.trace


def test_wrong_site_name_fails_loudly():
    with pytest.raises(CLIParseError, match="CENTRAL PARK"):
        parse_cli_product(load("chi_final.txt"), "CENTRAL PARK")


def test_garbage_text_fails_loudly():
    with pytest.raises(CLIParseError):
        parse_cli_product("NOT A CLIMATE REPORT AT ALL", "CENTRAL PARK")


def test_missing_temperature_section_fails_loudly():
    text = load("nyc_final.txt").replace("TEMPERATURE (F)", "XXXX")
    with pytest.raises(CLIParseError, match="TEMPERATURE"):
        parse_cli_product(text, "CENTRAL PARK")


def test_multi_site_bulletin_selects_correct_block():
    # WFO bulletins can bundle several sites' blocks into one product — simulate by
    # concatenating two real single-site products and assert block selection works
    combined = load("nyc_final.txt") + "\n" + load("phl_final.txt")
    nyc = parse_cli_product(combined, "CENTRAL PARK")
    phl = parse_cli_product(combined, "PHILADELPHIA")
    nyc_max = next(v for v in nyc.values if v.variable == "max_temp")
    phl_max = next(v for v in phl.values if v.variable == "max_temp")
    assert nyc_max.value == 85.0
    assert phl_max.value != nyc_max.value or nyc.site_line != phl.site_line


def test_mm_missing_value_returns_none_not_guess():
    text = load("nyc_final.txt").replace(
        "  MAXIMUM         85    347 PM", "  MAXIMUM         MM"
    )
    report = parse_cli_product(text, "CENTRAL PARK")
    max_t = next(v for v in report.values if v.variable == "max_temp")
    assert max_t.value is None
