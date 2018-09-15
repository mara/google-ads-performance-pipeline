import io
import json
import sys

stream = io.open(sys.stdin.fileno(), encoding='utf-8')
data = json.loads(stream.read())

for row in data:
    print('\t'.join([row['Day'], row['Ad ID'], row['Device'], row['Network (with search partners)'],
                     row['Active View viewable impressions'], row['Avg. position'], row['Clicks'],
                     row['Conversions'].replace(',', ''), row['Total conv. value'].replace(',', ''),
                     row['Cost'], row['Impressions']]))
