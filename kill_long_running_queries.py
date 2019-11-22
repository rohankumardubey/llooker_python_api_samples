
import yaml ### install the pyyaml package
from lookerapi import LookerApi
from datetime import datetime
from pprint import pprint
from pytz import timezone

### ------- HERE ARE PARAMETERS TO CONFIGURE -------
# Set the host that you'd like to access (as aliased in config.yml)
host = 'sandbox'
# Set the number of max number of seconds a query can be run before it's killed
threshold = 500
# Set query sources you don't want to kill
# Possible values:
# - API 3
# - CSV Dashboard Download
# - Dashboard
# - Dashboard Prefetch
# - Drill Modal
# - Explore
# - Merge Query
# - PDT Regenerator
# - Private Embed
# - Public Embed
# - Query
# - Renderer
# - Saved Look
# - Scheduled Task
# - SQL Runner
# - Suggest Filter

sources_to_exclude = []

### ------- OPEN THE CONFIG FILE and INSTANTIATE API -------
f = open('config.yml')
params = yaml.load(f)
f.close()

my_host = params['hosts'][host]['host']
my_secret = params['hosts'][host]['secret']
my_token = params['hosts'][host]['token']

looker = LookerApi(host=my_host,
                 token=my_token,
                 secret = my_secret)


queries = looker.get_running_queries()


kill_count = 0
for i in queries:
    query_created_at = datetime.strptime(
        i['created_at'].replace('T',' '), '%Y-%m-%d %H:%M:%S.%f+00:00'
    )
    tz = i['query']['query_timezone']
    # Compare query start time with system time.
    # Need to ensure timezones are setup correctly
    source = i['source']
    running_time = (
        datetime.utcnow().astimezone(timezone('UTC')) -
        query_created_at.astimezone(timezone(tz))
        ).total_seconds()
    print(running_time)
    if running_time > threshold and source not in sources_to_exclude:
        print('killing query: {}'.format(i['query_task_id']))
        looker.kill_query(i['query_task_id'])
        kill_count +=1
print('Killed {} queries'.format(kill_count))
