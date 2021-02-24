from influxLogger import InfluxLogger
import time

# Test stuff
dl = InfluxLogger(host='influx.antonverbeek.nl', username="REDACTED", password="REDACTED", database="REDACTED")

tStart = time.time()

t0 = time.time()
tLastPrint = time.time()

while (time.time() - tStart) < 60 :
    p = dl.addPoint(fields={"value":time.time() - t0 } )

    time.sleep(0.001)

    if time.time() - tLastPrint > 1.0:
        print(p)
        tLastPrint = time.time()

dl.stop()
