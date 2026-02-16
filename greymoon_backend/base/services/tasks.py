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


# def start_scrape_thread(run_id, dataset_id):
#     import threading
#     import requests
#     import time
#     from django.conf import settings
#     from django.utils import timezone
#     from ..utils import process_results
#     from ..models import ScrapeRun

#     def scrape_background():

#         offset = 0
#         total_collected = 0

#         # 🔹 Get scrape run instance
#         try:
#             scrape_run = ScrapeRun.objects.get(run_id=run_id)
#         except ScrapeRun.DoesNotExist:
#             print("ScrapeRun not found")
#             return

#         headers = {
#             "Authorization": f"Bearer {settings.APIFY_TOKEN}"
#         }

#         while True:
#             try:
#                 dataset_url = (
#                     f"https://api.apify.com/v2/datasets/{dataset_id}/items"
#                     f"?clean=true&offset={offset}&limit=100"
#                 )

#                 res = requests.get(dataset_url, headers=headers)
#                 res.raise_for_status()
#                 items = res.json()

#                 if items:
#                     print(f"Fetched {len(items)} new items")

#                     process_results(items)

#                     offset += len(items)
#                     total_collected += len(items)

#                     # 🔹 Update collected count live
#                     scrape_run.leads_collected = total_collected
#                     scrape_run.save(update_fields=["leads_collected"])

#             except Exception as e:
#                 print("Dataset fetch error:", str(e))

#             # 🔹 Check actor status
#             try:
#                 run_url = f"https://api.apify.com/v2/actor-runs/{run_id}"
#                 run_res = requests.get(run_url, headers=headers)
#                 run_res.raise_for_status()
#                 status = run_res.json()["data"]["status"]
#             except Exception as e:
#                 print("Status fetch error:", str(e))
#                 status = "FAILED"

#             print("Actor status:", status)

#             if status in ["SUCCEEDED", "FAILED", "ABORTED", "TIMED-OUT"]:
#                 print("Actor finished with status:", status)

#                 scrape_run.status = status
#                 scrape_run.finished_at = timezone.now()
#                 scrape_run.save(update_fields=["status", "finished_at"])

#                 break

#             time.sleep(5)

#     thread = threading.Thread(target=scrape_background)
#     thread.daemon = True
#     thread.start()

def start_scrape_thread(run_id, dataset_id):
    import threading
    import requests
    import time
    from django.conf import settings
    from django.utils import timezone
    from ..utils import process_results
    from ..models import ScrapeRun

    def scrape_background():

        offset = 0
        total_collected = 0
        processed_ids = set()  # 🔥 Prevent duplicate processing

        try:
            scrape_run = ScrapeRun.objects.get(run_id=run_id)
        except ScrapeRun.DoesNotExist:
            print("ScrapeRun not found")
            return

        headers = {
            "Authorization": f"Bearer {settings.APIFY_TOKEN}"
        }

        while True:
            try:
                dataset_url = (
                    f"https://api.apify.com/v2/datasets/{dataset_id}/items"
                    f"?clean=true&offset={offset}&limit=100"
                )

                res = requests.get(dataset_url, headers=headers, timeout=20)
                res.raise_for_status()
                items = res.json()

                if items:
                    print(f"Fetched {len(items)} items from dataset")

                    # 🔥 Filter out already processed in this thread
                    new_items = []
                    for item in items:
                        post_id = item.get("id")
                        if post_id and post_id not in processed_ids:
                            new_items.append(item)
                            processed_ids.add(post_id)

                    if new_items:
                        print(f"Processing {len(new_items)} NEW unique items")

                        process_results(new_items)

                        total_collected += len(new_items)

                        scrape_run.leads_collected = total_collected
                        scrape_run.save(update_fields=["leads_collected"])

                    offset += len(items)

            except Exception as e:
                print("Dataset fetch error:", str(e))

            # 🔹 Check actor status
            try:
                run_url = f"https://api.apify.com/v2/actor-runs/{run_id}"
                run_res = requests.get(run_url, headers=headers, timeout=20)
                run_res.raise_for_status()
                status = run_res.json()["data"]["status"]
            except Exception as e:
                print("Status fetch error:", str(e))
                status = "FAILED"

            print("Actor status:", status)

            if status in ["SUCCEEDED", "FAILED", "ABORTED", "TIMED-OUT"]:
                print("Actor finished with status:", status)

                scrape_run.status = status
                scrape_run.finished_at = timezone.now()
                scrape_run.save(update_fields=["status", "finished_at"])

                break

            time.sleep(5)

    thread = threading.Thread(target=scrape_background)
    thread.daemon = True
    thread.start()
