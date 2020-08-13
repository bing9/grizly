from grizly.scheduling import agent
import time

while True:
    results = agent.get_scheduled_jobs()
    print(results)
    time.sleep(30)