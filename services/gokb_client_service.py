import httpx

## GOKB Client (Hardcoding base_url for now)
GOKB_BASE_URL = "https://gokb.org/gokb/api"

def stream_http_records(page_size=1000):
    # Pipeline which gets individual pages from http client
    with httpx.Client(timeout=60.0) as client:
        url = f"{GOKB_BASE_URL}/scroll?componentType=TIPP&scrollSize={page_size}"
        has_more = True

        ## Track has_more
        while has_more:
            response = client.get(url)
            if response.status_code != 200:
                break

            data = response.json()
            records = data.get("records", [])

            if not records:
                break

            for record in records:
                yield record

                # Check scroll status from response payload
                has_more = data.get("hasMoreRecords", False)
                scroll_id = data.get("scrollId")

                # Subsequent scroll requests use scrollId
                if has_more and scroll_id:
                    url = f"{GOKB_BASE_URL}/scroll?scrollId={scroll_id}"
                else:
                    has_more = False