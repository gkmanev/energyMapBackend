# `fetch_flows` usage examples

The `fetch_flows` management command queries ENTSO-E cross-border physical flows for pairs of neighboring control areas. Countries must be passed explicitly via `--countries`.

## Single-country example (only the country's neighbors)
```bash
sudo docker compose exec web python manage.py fetch_flows \
  --countries BG \
  --start '2025-11-20T00:00:00Z' \
  --end   '2025-11-21T00:00:00Z'
```

Use `--dry-run` to inspect which neighbor pairs will be queried without hitting the API or database.

## All configured EU/ENTSO-E countries
The neighbor map in `data_api/settings.py` lists the supported control areas. To fetch flows between **every configured country and its neighbors**, pass the full ISO list as JSON:

```bash
sudo docker compose exec web python manage.py fetch_flows \
  --countries '["AL","AT","BA","BE","BG","BY","CH","CY","CZ","DE","DK","EE","ES","FI","FR","GB","GR","HR","HU","IE","IT","LT","LU","LV","MD","ME","MK","MT","NL","NO","PL","PT","RO","RS","SE","SI","SK","TR","UA","XK","GE"]' \
  --start '2025-11-20T00:00:00Z' \
  --end   '2025-11-21T00:00:00Z'
```

### Dry-run first
Before running a full fetch, validate the neighbor expansion:
```bash
sudo docker compose exec web python manage.py fetch_flows \
  --countries '["AL","AT","BA","BE","BG","BY","CH","CY","CZ","DE","DK","EE","ES","FI","FR","GB","GR","HR","HU","IE","IT","LT","LU","LV","MD","ME","MK","MT","NL","NO","PL","PT","RO","RS","SE","SI","SK","TR","UA","XK","GE"]' \
  --dry-run
```

The command expands multi-zone countries to all their EIC codes and queries each pair bidirectionally.
