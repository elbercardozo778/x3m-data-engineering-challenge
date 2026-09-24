import datetime as dt

import pytest

from pipeline.snapshot import StaleSnapshotError, check_snapshot_date

TODAY = dt.date(2026, 9, 24)


@pytest.mark.parametrize("snapshot_date", [TODAY, TODAY - dt.timedelta(days=1)])
def test_today_and_yesterday_are_accepted(snapshot_date):
    check_snapshot_date(snapshot_date, today=TODAY)


def test_older_dates_are_rejected_pointing_to_transform():
    with pytest.raises(StaleSnapshotError, match="relanzá solo el grupo `transform`"):
        check_snapshot_date(TODAY - dt.timedelta(days=2), today=TODAY)
