"""Time-weighted country physical flow series (all timestamps are interval starts)."""
import datetime as dt
from collections import defaultdict

from django.utils.dateparse import parse_duration


RESOLUTIONS = {"15m": dt.timedelta(minutes=15), "h": dt.timedelta(hours=1), "d": dt.timedelta(days=1)}


def build_flow_series(rows, country, start, end, resolution):
    """Average each directed EIC pair over observed time, then sum border averages.

    Empty buckets are omitted. Partial buckets are flagged against pairs observed
    anywhere in the request; entirely absent borders cannot be detected here.
    Overlapping readings for a pair prefer the finer, then newer interval.
    """
    step = RESOLUTIONS[resolution]
    epoch = dt.datetime(1970, 1, 1, tzinfo=dt.timezone.utc)
    pairs = defaultdict(list)
    for row in rows:
        duration = parse_duration(row["resolution"] or "PT15M")
        if duration is None or not dt.timedelta(0) < duration <= dt.timedelta(days=1):
            raise ValueError("Stored flow resolution must be a positive duration of at most one day.")
        timestamp = row["datetime_utc"]
        left, right = max(timestamp, start), min(timestamp + duration, end)
        if left >= right:
            continue
        direction = "exports_mw" if row["country_from_id"] == country else "imports_mw"
        pair = (direction, row["out_domain_eic"] or row["country_from_id"],
                row["in_domain_eic"] or row["country_to_id"])
        pairs[pair].append((left, right, duration, timestamp, row["quantity_mw"]))

    buckets = defaultdict(dict)
    for pair, intervals in pairs.items():
        # Sweep boundaries so overlapping source resolutions never double count.
        events = defaultdict(lambda: {"start": [], "end": []})
        for index, (left, right, *_rest) in enumerate(intervals):
            events[left]["start"].append(index)
            events[right]["end"].append(index)
        active = set()
        boundaries = sorted(events)
        for index, left in enumerate(boundaries[:-1]):
            active.difference_update(events[left]["end"])
            active.update(events[left]["start"])
            if not active:
                continue
            chosen = min(active, key=lambda i: (intervals[i][2], -intervals[i][3].timestamp()))
            quantity = intervals[chosen][4]
            right = boundaries[index + 1]
            while left < right:
                bucket = epoch + ((left - epoch) // step) * step
                segment_end = min(right, bucket + step)
                seconds = (segment_end - left).total_seconds()
                totals = buckets[bucket].setdefault(pair, [0.0, 0.0])
                totals[0] += quantity * seconds
                totals[1] += seconds
                left = segment_end

    items = []
    for bucket, values in sorted(buckets.items()):
        expected_seconds = (min(bucket + step, end) - max(bucket, start)).total_seconds()
        item = {"datetime_utc": bucket.isoformat().replace("+00:00", "Z"),
                "imports_mw": 0.0, "exports_mw": 0.0,
                "is_partial": len(values) < len(pairs)}
        for pair, (weighted, seconds) in values.items():
            item[pair[0]] += weighted / seconds
            item["is_partial"] |= seconds < expected_seconds
        item["net_mw"] = item["imports_mw"] - item["exports_mw"]
        items.append(item)
    return items
