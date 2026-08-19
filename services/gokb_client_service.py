import httpx

## GOKB Client (Hardcoding base_url for now)
GOKB_BASE_URL = "https://gokb.org/gokb/api"
GOKBT_BASE_URL = "https://gokbt.gbv.de/gokb/api"

def build_scroll_url(scroll_size = 1000, component_type='TIPP', scroll_id = None):
    gokb_url = f"{GOKBT_BASE_URL}/scroll?componentType={component_type}&scrollSize={scroll_size}"

    if scroll_id:
        gokb_url = f"{gokb_url}&scrollId={scroll_id}"
    return gokb_url

def stream_http_records(page_size=1000):
    # Pipeline which gets individual pages from http client
    with httpx.Client(timeout=60.0) as client:
        url = build_scroll_url(page_size)
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
                    url = build_scroll_url(page_size, 'TIPP', scroll_id)
                else:
                    has_more = False