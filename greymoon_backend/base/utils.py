import re
from .models import ServiceLead
from django.utils.dateparse import parse_datetime

KEYWORDS = [
    "cleaning",
    "maintenance",
    "junk",
    "waste",
    "trash",
    "repair"
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
        print("Saving:", item.get("id"), item.get("title"))
        try:
            phone_numbers = item.get("phoneNumbers", [])
            phone = phone_numbers[0] if phone_numbers else None
            email = extract_email(item.get("post", ""))
            zip_code = extract_zip(item.get("post", ""))

            ServiceLead.objects.update_or_create(
                post_id=item.get("id"),
                defaults={
                    "url": item.get("url"),
                    "title": item.get("title"),
                    "datetime": item.get("datetime"),
                    "location": item.get("location"),
                    "category": item.get("category"),
                    "label": item.get("label"),
                    "longitude": item.get("longitude"),
                    "latitude": item.get("latitude"),
                    "map_accuracy": item.get("mapAccuracy"),
                    "post": item.get("post"),
                    "phone": phone,
                    "email": email,
                    "zip_code": zip_code,
                    "raw_json": item,
                }
            )
        except Exception as e:
            print("Error saving item:", e)


