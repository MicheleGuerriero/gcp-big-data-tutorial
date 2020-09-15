import sys
from google.cloud import pubsub
import random
from datetime import datetime
from datetime import timedelta
import pytz
import time

local_tz = pytz.timezone('Europe/Rome')

def utc_to_local(utc_dt):
    local_dt = utc_dt.replace(tzinfo=pytz.utc).astimezone(local_tz)
    return local_tz.normalize(local_dt)

PROJECT = 'big-data-env'#sys.argv[1]
TOTAL_TIME = int(sys.argv[2])  # in minutes
RATE = int(sys.argv[3])  # in dpis per minute

ONE_MINUTE = 60
TOPIC_NAME = 'dpi-topic'

publisher = pubsub.PublisherClient()
topic_url = 'projects/{project_id}/topics/{topic}'.format(
    project_id=PROJECT,
    topic=TOPIC_NAME,
)
num_dpis = 0

def generate_dpi():
    # Generates a single random dpi.
    numbers=['123','456','789','321','654','987']
    signatures = ['facebook', 'tweeter', 'vodafone', 'polimi']
    usages = range(1000, 10000, 250)
    delays = range(0, 30)
    global num_dpis
    number = random.choice(numbers)
    signature = random.choice(signatures)
    timestamp = (utc_to_local(datetime.now()) - timedelta(seconds=random.choice(delays))).strftime("%d/%m/%Y-%H:%M:%S")
    usage = random.choice(usages)
    dpi = ','.join([number, signature, str(usage), timestamp])
    publisher.publish(topic_url, dpi.encode('utf-8'))
    num_dpis += 1
    return dpi

now = start_time = time.time()
while now < start_time + TOTAL_TIME * ONE_MINUTE:
    dpi = generate_dpi()
    num_dpis += 1
    print('Dpi record sent: %s' % dpi)
    time.sleep(ONE_MINUTE/float(RATE))

elapsed_time = time.time() - start_time
print("Elapsed time: %s minutes" % (elapsed_time / 60))
print("Number of generated dpis: %s" % num_dpis)
