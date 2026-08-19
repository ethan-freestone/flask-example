import time
from itertools import batched

from flask import current_app

from extensions import db
from models.tipp import Title, Identifier
from services.gokb_client_service import stream_http_records


# Single test title ingest
def run_test_ingest():
  start_time = time.perf_counter_ns()

  title = Title(
    title="test",
    publication_type="Serial",
    raw_record={'test': 1234, 'other_key': 'abcd'}
  )

  title.identifiers.append(Identifier(
    id_type="ISSN",
    id_value="1234-5678"
  ))

  db.session.add(title)
  db.session.commit()

  duration = time.perf_counter_ns() - start_time

  return {
    "ingest_count": 1,
    "execution_time": duration
  }


## Handle this with generator methods instead of rxpy

def process_and_commit_batch(batch, app):
  with app.app_context():
    titles_to_add = []
    for record in batch:
      title = Title(
        title=record.get("title", record.get("tippTitleName", "Untitled")),
        publication_type=record.get("publicationType", "Serial"),
        raw_record=record,
      )
      for id_info in record.get("titleIdentifiers", []):
        title.identifiers.append(
          Identifier(
            id_type=id_info.get("namespace", "UNKNOWN"),
            id_value=id_info.get("value", ""),
          )
        )
      titles_to_add.append(title)

    db.session.add_all(titles_to_add)
    db.session.commit()
    return len(titles_to_add)

def run_scroll_ingest(batch_size=500, page_size=1000):
  start_time = time.perf_counter_ns()
  total_ingested = 0
  app = current_app._get_current_object()

  # Pull chunks lazily and commit sequentially without threads or queues
  for batch in batched(stream_http_records(page_size), batch_size):
    total_ingested += process_and_commit_batch(batch, app)

  duration = time.perf_counter_ns() - start_time
  return {
    "ingest_count": total_ingested,
    "execution_time": duration
  }