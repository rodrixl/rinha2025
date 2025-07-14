import redis
import os
import json
import httpx
import time

redis_client = redis.Redis(host=os.getenv("REDIS_HOST", "localhost"), port=6379, db=0)

PAYMENT_PROCESSOR_URL_DEFAULT = os.getenv("PAYMENT_PROCESSOR_URL_DEFAULT")
PAYMENT_PROCESSOR_URL_FALLBACK = os.getenv("PAYMENT_PROCESSOR_URL_FALLBACK")

def check_health(url, service_name):
    try:
        with httpx.Client() as client:
            response = client.get(f"{url}/payments/service-health", timeout=2.0)
            response.raise_for_status()
            health_data = response.json()
            redis_client.set(f"{service_name}_health", json.dumps(health_data))
            print(f"{service_name} is healthy: {health_data}")
    except httpx.RequestError as exc:
        print(f"An error occurred while requesting {exc.request.url!r}.")
        redis_client.set(f"{service_name}_health", json.dumps({"failing": True, "minResponseTime": 99999}))
    except Exception as e:
        print(f"An unexpected error occurred for {service_name}: {e}")
        redis_client.set(f"{service_name}_health", json.dumps({"failing": True, "minResponseTime": 99999}))

if __name__ == "__main__":
    print("Health checker started")
    while True:
        check_health(PAYMENT_PROCESSOR_URL_DEFAULT, "default")
        check_health(PAYMENT_PROCESSOR_URL_FALLBACK, "fallback")
        time.sleep(5) # Adheres to the 5-second rate limit
