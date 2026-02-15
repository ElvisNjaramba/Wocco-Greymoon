import threading
import time
from .apify_service import wait_for_finish, fetch_results
from ..utils import process_results
import requests
from django.conf import settings

def scrape_background(run_id):
    try:
        while True:
            try:
                results = fetch_results(run_id)
                if results:
                    print(f"Fetched {len(results)} results so far")
                    process_results(results)
            except Exception as e:
                print("No dataset yet or fetch failed:", str(e))

            url = f"https://api.apify.com/v2/actor-runs/{run_id}"
            headers = {"Authorization": f"Bearer {settings.APIFY_TOKEN}"}
            status_res = requests.get(url, headers=headers).json()["data"]
            status = status_res["status"]
            print("Actor status:", status)
            if status in ["SUCCEEDED", "FAILED", "ABORTED", "TIMED-OUT"]:
                break

            time.sleep(5)  # poll every 5 seconds

        print(f"Final actor status: {status}")
        if status == "SUCCEEDED":
            print("Scraping finished successfully")
        else:
            print("Scraping did not complete successfully, but results may be partially saved")

    except Exception as e:
        print("Scraping failed:", str(e))



def start_scrape_thread(run_id, dataset_id):
    import threading
    import requests
    import time
    from django.conf import settings
    from ..utils import process_results

    def scrape_background():
        offset = 0  # tracks how many items we've already fetched

        while True:
            try:
                # Fetch only NEW items
                dataset_url = (
                    f"https://api.apify.com/v2/datasets/{dataset_id}/items"
                    f"?clean=true&offset={offset}&limit=100"
                )

                headers = {
                    "Authorization": f"Bearer {settings.APIFY_TOKEN}"
                }

                res = requests.get(dataset_url, headers=headers)
                res.raise_for_status()
                items = res.json()

                if items:
                    print(f"Fetched {len(items)} new items")
                    process_results(items)
                    offset += len(items)

            except Exception as e:
                print("Dataset fetch error:", str(e))

            # Check run status
            run_url = f"https://api.apify.com/v2/actor-runs/{run_id}"
            run_res = requests.get(run_url, headers=headers).json()["data"]
            status = run_res["status"]

            print("Actor status:", status)

            if status in ["SUCCEEDED", "FAILED", "ABORTED", "TIMED-OUT"]:
                print("Actor finished with status:", status)
                break

            time.sleep(5)

    thread = threading.Thread(target=scrape_background)
    thread.daemon = True
    thread.start()
