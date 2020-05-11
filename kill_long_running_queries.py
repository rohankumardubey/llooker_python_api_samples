
import yaml ### install the pyyaml package
import logging
from lookerapi import LookerApi
from datetime import datetime
from pprint import pprint
from pytz import timezone
import logging
logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S',
    filename='query_kills.log'
)

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



for i in queries:
    query_created_at = datetime.strptime(
        i['created_at'].split('.')[0].replace('T',' '),
        '%Y-%m-%d %H:%M:%S'
    )
    source = i['source']
    history_id = i['id']
    # Assumes system time is in UTC
    # Small, negative query running times can be expected if system times differ across the Looker MySQL database
    # and the machine from which the script is being executed
    running_time = (
        datetime.utcnow() -
        query_created_at).total_seconds()
    if running_time > threshold and source not in sources_to_exclude:
        print('killing query: {}'.format(i['query_task_id']))
        looker.kill_query(i['query_task_id'])
        logging.warning(
        "Killed query with history_id {}. Runtime exceed threshold of {} secods. Runtime at time of killing: {}".format(
            history_id, threshold, running_time
         ))  
