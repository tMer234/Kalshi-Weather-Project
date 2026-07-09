from datetime import datetime, timedelta, timezone

import pytest

from kalshi_weather.time_utils import horizon_hours, parse_interval, to_utc_naive

UTC = timezone.utc


class TestParseInterval:
    def test_simple_hourly(self):
        start, end = parse_interval("2019-07-04T18:00:00+00:00/PT3H")
        assert start == datetime(2019, 7, 4, 18, 0, tzinfo=UTC)
        assert end == datetime(2019, 7, 4, 21, 0, tzinfo=UTC)

    def test_one_hour(self):
        start, end = parse_interval("2026-07-09T05:00:00+00:00/PT1H")
        assert end - start == timedelta(hours=1)

    def test_multi_day_duration(self):
        # maxTemperature/minTemperature layers often span a whole day or more
        start, end = parse_interval("2026-07-08T08:00:00+00:00/P1D")
        assert end == datetime(2026, 7, 9, 8, 0, tzinfo=UTC)

    def test_compound_duration(self):
        start, end = parse_interval("2026-07-08T18:00:00+00:00/P1DT6H")
        assert end - start == timedelta(days=1, hours=6)

    def test_nonzero_utc_offset_normalized(self):
        # offsets other than +00:00 must land at the correct UTC instant
        start, _ = parse_interval("2026-07-08T01:00:00-05:00/PT13H")
        assert start == datetime(2026, 7, 8, 6, 0, tzinfo=UTC)

    def test_result_is_utc(self):
        start, end = parse_interval("2026-07-08T01:00:00-05:00/PT13H")
        assert start.tzinfo == UTC and end.tzinfo == UTC

    def test_malformed_raises(self):
        with pytest.raises(ValueError):
            parse_interval("2026-07-08T01:00:00+00:00")  # no duration part
        with pytest.raises(ValueError):
            parse_interval("not-a-time/PT1H")
        with pytest.raises(ValueError):
            parse_interval("2026-07-08T01:00:00+00:00/NOPE")


class TestHorizonHours:
    def test_basic(self):
        issued = datetime(2026, 7, 8, 12, 0, tzinfo=UTC)
        valid = datetime(2026, 7, 9, 0, 0, tzinfo=UTC)
        assert horizon_hours(issued, valid) == 12.0

    def test_fractional(self):
        issued = datetime(2026, 7, 8, 12, 0, tzinfo=UTC)
        valid = datetime(2026, 7, 8, 12, 30, tzinfo=UTC)
        assert horizon_hours(issued, valid) == 0.5

    def test_negative_when_valid_before_issue(self):
        # gridpoint payloads include periods already underway; horizon can be negative
        issued = datetime(2026, 7, 8, 12, 0, tzinfo=UTC)
        valid = datetime(2026, 7, 8, 6, 0, tzinfo=UTC)
        assert horizon_hours(issued, valid) == -6.0

    def test_mixed_offsets(self):
        issued = datetime(2026, 7, 8, 7, 0, tzinfo=timezone(timedelta(hours=-5)))
        valid = datetime(2026, 7, 9, 0, 0, tzinfo=UTC)
        assert horizon_hours(issued, valid) == 12.0


class TestToUtcNaive:
    def test_aware_converted(self):
        dt = datetime(2026, 7, 8, 1, 0, tzinfo=timezone(timedelta(hours=-5)))
        assert to_utc_naive(dt) == datetime(2026, 7, 8, 6, 0)

    def test_naive_rejected(self):
        with pytest.raises(ValueError):
            to_utc_naive(datetime(2026, 7, 8, 1, 0))
