"""Parser for NWS Daily Climate Report (CLI) text products.

CLI products are semi-structured fixed-width text bulletins — the settlement ground truth
Kalshi resolves against, and the most fragile input in this pipeline. Design rule: fail
loudly (raise CLIParseError, caller logs + skips) rather than guess when the expected shape
isn't found. Tested against real recorded product texts in tests/fixtures/cli/.

Shape being parsed (see fixtures for full examples):

    ...THE CENTRAL PARK NY CLIMATE SUMMARY FOR JULY 8 2026...
    [VALID TODAY AS OF 0400 PM LOCAL TIME.]        <- present on intermediate reports
    ...
    TEMPERATURE (F)
     YESTERDAY            (or TODAY on intermediate reports)
      MAXIMUM         85    347 PM 100    1993  85      0       93
      MINIMUM         64    440 AM  56    1894  70     -6       73
    PRECIPITATION (IN)
      YESTERDAY        0.00 ...              (value may be T = trace, MM = missing)
    SNOWFALL (IN)
      YESTERDAY        0.0 ...

Notes:
- One WFO bulletin can bundle several sites' blocks; the correct block is located by the
  spelled-out site name from config (cli_site_name), never by ICAO code (CLI text has none).
- value_time is stored as a NAIVE local datetime exactly as printed (obs_date + clock time).
  The report's column header claims LST but intermediate "AS OF" stamps behave like local
  daylight time; rather than encode a possibly-wrong tz conversion, we keep it as printed.
- obs_date comes from the summary header line itself, never from re-bucketing timestamps —
  the report's climatological day is 1:00 AM-12:59 AM local during DST, not midnight-to-
  midnight, so deriving the date any other way is a bug.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, datetime, time

SUMMARY_RE = re.compile(
    r"\.\.\.THE (?P<site>.+?) CLIMATE SUMMARY FOR (?P<date>[A-Z]+ \d{1,2} \d{4})",
)
# occurrence time formats vary by WFO: OKX/BOU write "347 PM", LOT/EWX write "2:56 PM".
# a value may carry an R suffix ("100R") meaning a record was set or tied.
TEMP_LINE_RE = re.compile(
    r"^\s{1,4}(?P<kind>MAXIMUM|MINIMUM)\s+(?P<value>-?\d+|MM)R?"
    r"(?:\s+(?P<time>\d{1,2}:?\d{2} (?:AM|PM)))?",
)
# first data line of the PRECIPITATION / SNOWFALL sections
AMOUNT_LINE_RE = re.compile(
    r"^\s{1,4}(?:YESTERDAY|TODAY)\s+(?P<value>T|MM|\d+\.\d+)R?\b",
)
MONTHS = {
    m: i
    for i, m in enumerate(
        ["JANUARY", "FEBRUARY", "MARCH", "APRIL", "MAY", "JUNE", "JULY",
         "AUGUST", "SEPTEMBER", "OCTOBER", "NOVEMBER", "DECEMBER"],
        start=1,
    )
}


class CLIParseError(Exception):
    """The product text didn't match the expected CLI shape — skip, don't guess."""


@dataclass(frozen=True)
class ParsedClimateValue:
    variable: str                 # 'max_temp', 'min_temp', 'precip', 'snowfall'
    value: float | None           # None when the report shows MM (missing)
    unit: str                     # 'degF' or 'in'
    value_time: datetime | None = None  # naive local time-of-occurrence, temps only
    trace: bool = False           # True when the report shows T (trace amount -> 0.0)


@dataclass(frozen=True)
class ParsedClimateReport:
    site_line: str                # site name exactly as it appears in the header
    obs_date: date                # the climatological day the report covers
    is_intermediate: bool         # True for same-day "AS OF" reports (not the final)
    values: list[ParsedClimateValue]


def _parse_header_date(raw: str) -> date:
    month_name, day, year = raw.split()
    try:
        return date(int(year), MONTHS[month_name], int(day))
    except (KeyError, ValueError) as e:
        raise CLIParseError(f"unparseable summary date {raw!r}") from e


def _parse_clock(raw: str, obs_date: date) -> datetime:
    clock, meridiem = raw.split()
    hour, minute = divmod(int(clock.replace(":", "")), 100)
    if meridiem == "PM" and hour != 12:
        hour += 12
    elif meridiem == "AM" and hour == 12:
        hour = 0
    if not (0 <= hour <= 23 and 0 <= minute <= 59):
        raise CLIParseError(f"unparseable occurrence time {raw!r}")
    return datetime.combine(obs_date, time(hour, minute))


def _select_site_block(text: str, site_name: str) -> tuple[str, str, str]:
    """Return (site_line, header_date_raw, block_text) for the requested site."""
    matches = list(SUMMARY_RE.finditer(text))
    if not matches:
        raise CLIParseError("no 'CLIMATE SUMMARY FOR' header found in product text")
    want = site_name.upper()
    for i, m in enumerate(matches):
        if want in m.group("site").upper():
            end = matches[i + 1].start() if i + 1 < len(matches) else len(text)
            return m.group("site"), m.group("date"), text[m.start():end]
    found = [m.group("site") for m in matches]
    raise CLIParseError(f"no block for site {site_name!r}; product contains {found}")


def _section(block: str, header_prefix: str) -> str | None:
    """Return the lines of one report section (header line up to the next blank line)."""
    lines = block.splitlines()
    for i, line in enumerate(lines):
        if line.strip().startswith(header_prefix):
            out = []
            for follower in lines[i + 1:]:
                if not follower.strip():
                    break
                out.append(follower)
            return "\n".join(out)
    return None


def _parse_temperatures(block: str, obs_date: date) -> list[ParsedClimateValue]:
    section = _section(block, "TEMPERATURE")
    if section is None:
        raise CLIParseError("no TEMPERATURE section found in site block")
    values = []
    for kind, variable in (("MAXIMUM", "max_temp"), ("MINIMUM", "min_temp")):
        for line in section.splitlines():
            m = TEMP_LINE_RE.match(line)
            if m and m.group("kind") == kind:
                raw = m.group("value")
                values.append(
                    ParsedClimateValue(
                        variable=variable,
                        value=None if raw == "MM" else float(raw),
                        unit="degF",
                        value_time=(
                            _parse_clock(m.group("time"), obs_date)
                            if m.group("time")
                            else None
                        ),
                    )
                )
                break
        else:
            raise CLIParseError(f"no {kind} line found in TEMPERATURE section")
    return values


def _parse_amount(block: str, header_prefix: str, variable: str) -> ParsedClimateValue | None:
    """Parse the daily value line of PRECIPITATION/SNOWFALL. Absent section -> None."""
    section = _section(block, header_prefix)
    if section is None:
        return None
    for line in section.splitlines():
        m = AMOUNT_LINE_RE.match(line)
        if m:
            raw = m.group("value")
            if raw == "MM":
                return ParsedClimateValue(variable=variable, value=None, unit="in")
            if raw == "T":
                return ParsedClimateValue(variable=variable, value=0.0, unit="in", trace=True)
            return ParsedClimateValue(variable=variable, value=float(raw), unit="in")
    return None


def parse_cli_product(text: str, site_name: str) -> ParsedClimateReport:
    """Parse one CLI product's text, extracting the block for `site_name`.

    Raises CLIParseError when the site's block or a required field isn't in the
    expected shape — callers should log and skip, never guess.
    """
    site_line, date_raw, block = _select_site_block(text, site_name)
    obs_date = _parse_header_date(date_raw)
    # "VALID TODAY AS OF 0400 PM" (some WFOs: "VALID AS OF 0400 PM") marks a same-day
    # intermediate report; the final report for a date arrives the next morning and
    # supersedes it (newer issuanceTime)
    is_intermediate = bool(re.search(r"VALID\b.*\bAS OF", block))

    values = _parse_temperatures(block, obs_date)
    for header, variable in (("PRECIPITATION", "precip"), ("SNOWFALL", "snowfall")):
        parsed = _parse_amount(block, header, variable)
        if parsed is not None:
            values.append(parsed)

    return ParsedClimateReport(
        site_line=site_line,
        obs_date=obs_date,
        is_intermediate=is_intermediate,
        values=values,
    )
