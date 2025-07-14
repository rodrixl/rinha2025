import redis
import os
import json
import httpx
import time

redis_client = redis.Redis(host=os.getenv("REDIS_HOST", "localhost"), port=6379, db=0)

PAYMENT_PROCESSOR_URL_DEFAULT = os.getenv("PAYMENT_PROCESSOR_URL_DEFAULT")
PAYMENT_PROCESSOR_URL_FALLBACK = os.getenv("PAYMENT_PROCESSOR_URL_FALLBACK")

def get_health_status():
    default_status = redis_client.get("default_health")
    fallback_status = redis_client.get("fallback_health")
    return json.loads(default_status) if default_status else None, json.loads(fallback_status) if fallback_status else None

def process_payment(payment_data):
    default_health, fallback_health = get_health_status()

    url = None
    processor_name = None

    if default_health and not default_health.get('failing'):
        url = PAYMENT_PROCESSOR_URL_DEFAULT
        processor_name = "default"
    elif fallback_health and not fallback_health.get('failing'):
        url = PAYMENT_PROCESSOR_URL_FALLBACK
        processor_name = "fallback"
    else:
        # Re-queue and wait for a healthy processor
        redis_client.rpush("payment_queue", json.dumps(payment_data))
        time.sleep(1) # Avoid busy-waiting
        return

    try:
        with httpx.Client() as client:
            response = client.post(f"{url}/payments", json=payment_data, timeout=10.0)
            response.raise_for_status()

            redis_client.incr(f"{processor_name}_total_requests")
            redis_client.incrbyfloat(f"{processor_name}_total_amount", payment_data["amount"])

    except httpx.RequestError as exc:
        print(f"An error occurred while requesting {exc.request.url!r}.")
        # Re-queue the payment for later processing
        redis_client.rpush("payment_queue", json.dumps(payment_data))

if __name__ == "__main__":
    print("Worker started")
    while True:
        # Blocking pop from the queue
        _, payment_data_json = redis_client.brpop("payment_queue")
        payment_data = json.loads(payment_data_json)
        process_payment(payment_data)
