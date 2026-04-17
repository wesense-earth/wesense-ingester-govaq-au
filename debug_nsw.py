import json
from adapters.nsw import NSWAdapter

s = json.load(open('/app/cache/govaq_au_nsw_state.json'))
items = sorted(s.items(), key=lambda x: x[1], reverse=True)[:5]
print('Saved timestamps:')
for k, v in items:
    print(f'  station {k}: {v}')

a = NSWAdapter('nsw', {
    'sites_url': 'https://data.airquality.nsw.gov.au/api/Data/get_SiteDetails',
    'observations_url': 'https://data.airquality.nsw.gov.au/api/Data/get_Observations',
})
stations = a.fetch_stations()
r = a.fetch_readings(stations[0])
print(f'\nStation {stations[0]["name"]}: {len(r)} readings (no dedup)')
for x in r[:5]:
    print(f'  ts={x["timestamp"]} {x["reading_type"]}={x["value"]}')

# Now test with restored state
a2 = NSWAdapter('nsw', {
    'sites_url': 'https://data.airquality.nsw.gov.au/api/Data/get_SiteDetails',
    'observations_url': 'https://data.airquality.nsw.gov.au/api/Data/get_Observations',
})
a2.set_last_timestamps(s)
stations2 = a2.fetch_stations()
r2 = a2.fetch_readings(stations2[0])
print(f'\nStation {stations2[0]["name"]}: {len(r2)} readings (with restored state)')
for x in r2[:5]:
    print(f'  ts={x["timestamp"]} {x["reading_type"]}={x["value"]}')
