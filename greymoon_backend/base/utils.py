import re
from .models import ServiceLead
from django.utils.dateparse import parse_datetime

KEYWORDS = [
    # Cleaning
    "cleaning",
    "deep clean",
    "house cleaning",
    "office cleaning",
    "pressure washing",
    "window cleaning",

    # Maintenance
    "maintenance",
    "repair",
    "plumber",
    "plumbing",
    "painter",
    "painting",
    "electrical",
    "electrician",
    "hvac",
    "air conditioning",
    "roof repair",
    "handyman",
    "carpentry",

    # Waste management
    "junk removal",
    "junk",
    "waste",
    "trash",
    "garbage",
    "hauling",
    "dumpster",
    "septic",
    "drain cleaning"
]


def valid_service(title, description):
    text = (title + " " + description).lower()
    return any(word in text for word in KEYWORDS)


def extract_zip(text):
    match = re.search(r"\b\d{5}\b", text or "")
    return match.group() if match else None


def extract_phone(text):
    match = re.search(r"\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}", text or "")
    return match.group() if match else None


def extract_email(text):
    match = re.search(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", text or "")
    return match.group() if match else None

def process_results(results):
    print("Processing results count:", len(results))

    for item in results:
        title = item.get("title", "")
        description = item.get("post", "")

        # FILTER HERE
        if not valid_service(title, description):
            continue

        try:
            phone_numbers = item.get("phoneNumbers", [])
            phone = phone_numbers[0] if phone_numbers else None
            email = extract_email(description)
            zip_code = extract_zip(description)

            ServiceLead.objects.update_or_create(
                post_id=item.get("id"),
                defaults={
                    "url": item.get("url"),
                    "title": title,
                    "datetime": item.get("datetime"),
                    "location": item.get("location"),
                    "category": item.get("category"),
                    "label": item.get("label"),
                    "longitude": item.get("longitude"),
                    "latitude": item.get("latitude"),
                    "map_accuracy": item.get("mapAccuracy"),
                    "post": description,
                    "phone": phone,
                    "email": email,
                    "zip_code": zip_code,
                    "raw_json": item,
                }
            )
        except Exception as e:
            print("Error saving item:", e)

