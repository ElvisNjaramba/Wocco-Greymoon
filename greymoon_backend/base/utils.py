# import re
# from .models import ServiceLead
# from .services.scoring import calculate_lead_score
# from geopy.geocoders import Nominatim
# from geopy.extra.rate_limiter import RateLimiter

# geolocator = Nominatim(user_agent="wocco_greymoon_scraper")
# geocode = RateLimiter(geolocator.reverse, min_delay_seconds=1)


# def extract_zip(text):
#     match = re.search(r"\b\d{5}\b", text or "")
#     return match.group() if match else None


# def extract_phone(text):
#     match = re.search(r"\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}", text or "")
#     return match.group() if match else None


# def extract_email(text):
#     match = re.search(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", text or "")
#     return match.group() if match else None


# def reverse_geocode(lat, lon):
#     """
#     Returns (state, zip_code) from latitude/longitude.
#     Returns (None, None) if lookup fails.
#     """
#     try:
#         location = geocode((lat, lon), language='en')
#         if location and location.raw:
#             address = location.raw.get('address', {})
#             state = address.get('state')
#             zip_code = address.get('postcode')
#             return state, zip_code
#     except Exception as e:
#         print(f"Reverse geocoding failed for ({lat},{lon}): {e}")
#     return None, None


# def process_results(results):
#     print("Processing results count:", len(results))

#     # Get all existing post_ids in one query to skip duplicates
#     incoming_ids = [item.get("id") for item in results if item.get("id")]
#     existing_ids = set(
#         ServiceLead.objects.filter(post_id__in=incoming_ids)
#         .values_list("post_id", flat=True)
#     )

#     saved = 0
#     skipped_duplicate = 0
#     skipped_no_id = 0

#     for item in results:
#         post_id = item.get("id")

#         if not post_id:
#             skipped_no_id += 1
#             continue

#         if post_id in existing_ids:
#             skipped_duplicate += 1
#             continue

#         title = item.get("title", "") or ""
#         description = item.get("post", "") or ""

#         try:
#             phone_numbers = item.get("phoneNumbers", [])
#             phone = phone_numbers[0] if phone_numbers else None
#             email = extract_email(description)
#             zip_code = extract_zip(description)  # fallback from description text

#             # ✅ Correct assignment: lat → latitude, lon → longitude
#             lat = item.get("latitude")
#             lon = item.get("longitude")
#             state = None

#             # Reverse geocode if coordinates available
#             if lat and lon:
#                 try:
#                     lat_float = float(lat)
#                     lon_float = float(lon)
#                     rev_state, rev_zip = reverse_geocode(lat_float, lon_float)
#                     if rev_state:
#                         state = rev_state
#                     if rev_zip:
#                         zip_code = rev_zip  # override with geocoded zip
#                 except (ValueError, TypeError):
#                     pass

#             score, score_reason = calculate_lead_score(item)

#             ServiceLead.objects.create(
#                 post_id=post_id,
#                 url=item.get("url"),
#                 title=title,
#                 datetime=item.get("datetime"),
#                 location=item.get("location"),
#                 category=item.get("category"),
#                 label=item.get("label"),
#                 latitude=lat,   # ✅ fixed
#                 longitude=lon,  # ✅ fixed
#                 map_accuracy=item.get("mapAccuracy"),
#                 post=description,
#                 phone=phone,
#                 email=email,
#                 zip_code=zip_code,
#                 state=state,
#                 raw_json=item,
#                 status="NEW",
#                 score=score,
#                 score_reason=score_reason,
#             )
#             saved += 1

#         except Exception as e:
#             print("Error saving item:", e)

#     print(f"Done — saved: {saved}, duplicates skipped: {skipped_duplicate}, no ID: {skipped_no_id}")


import re
import hashlib
from .models import ServiceLead
from .services.scoring import calculate_lead_score
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter

geolocator = Nominatim(user_agent="wocco_greymoon_scraper")
geocode = RateLimiter(geolocator.reverse, min_delay_seconds=1)


def extract_zip(text):
    match = re.search(r"\b\d{5}\b", text or "")
    return match.group() if match else None


def extract_email(text):
    match = re.search(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", text or "")
    return match.group() if match else None


def make_content_hash(title, description, phone, email):
    """
    Creates a fingerprint based on content, not post ID.
    Normalizes text so minor spacing/punctuation differences don't bypass the check.
    """
    def normalize(text):
        if not text:
            return ""
        text = text.lower().strip()
        text = re.sub(r"[^\w\s]", "", text)
        text = re.sub(r"\s+", " ", text)
        return text

    fingerprint = "|".join([
        normalize(title),
        normalize((description or "")[:200]),
        normalize(phone or ""),
        normalize(email or ""),
    ])
    return hashlib.sha256(fingerprint.encode()).hexdigest()


def reverse_geocode(lat, lon):
    try:
        location = geocode((lat, lon), language='en')
        if location and location.raw:
            address = location.raw.get('address', {})
            return address.get('state'), address.get('postcode')
    except Exception as e:
        print(f"Reverse geocoding failed for ({lat},{lon}): {e}")
    return None, None


def process_results(results):
    print("Processing results count:", len(results))

    incoming_ids = [item.get("id") for item in results if item.get("id")]
    existing_post_ids = set(
        ServiceLead.objects.filter(post_id__in=incoming_ids)
        .values_list("post_id", flat=True)
    )

    existing_hashes = set(
        ServiceLead.objects.exclude(content_hash__isnull=True)
        .exclude(content_hash="")
        .values_list("content_hash", flat=True)
    )

    saved = 0
    skipped_duplicate_id = 0
    skipped_duplicate_content = 0
    skipped_no_id = 0
    seen_hashes_this_batch = set()

    for item in results:
        post_id = item.get("id")

        if not post_id:
            skipped_no_id += 1
            continue

        if post_id in existing_post_ids:
            skipped_duplicate_id += 1
            continue

        title = item.get("title", "") or ""
        description = item.get("post", "") or ""

        try:
            phone_numbers = item.get("phoneNumbers", [])
            phone = phone_numbers[0] if phone_numbers else None
            email = extract_email(description)

            content_hash = make_content_hash(title, description, phone, email)

            if content_hash in existing_hashes or content_hash in seen_hashes_this_batch:
                skipped_duplicate_content += 1
                print(f"Skipping repost: '{title[:60]}' (content duplicate)")
                continue

            seen_hashes_this_batch.add(content_hash)

            zip_code = extract_zip(description)
            lat = item.get("latitude")
            lon = item.get("longitude")
            state = None

            if lat and lon:
                try:
                    rev_state, rev_zip = reverse_geocode(float(lat), float(lon))
                    if rev_state:
                        state = rev_state
                    if rev_zip:
                        zip_code = rev_zip
                except (ValueError, TypeError):
                    pass

            score, score_reason = calculate_lead_score(item)

            ServiceLead.objects.create(
                post_id=post_id,
                content_hash=content_hash,
                url=item.get("url"),
                title=title,
                datetime=item.get("datetime"),
                location=item.get("location"),
                category=item.get("category"),
                label=item.get("label"),
                latitude=lat,
                longitude=lon,
                map_accuracy=item.get("mapAccuracy"),
                post=description,
                phone=phone,
                email=email,
                zip_code=zip_code,
                state=state,
                raw_json=item,
                status="NEW",
                score=score,
                score_reason=score_reason,
            )
            saved += 1

        except Exception as e:
            print("Error saving item:", e)

    print(
        f"Done — saved: {saved} | "
        f"skipped (same post_id): {skipped_duplicate_id} | "
        f"skipped (repost): {skipped_duplicate_content} | "
        f"no ID: {skipped_no_id}"
    )