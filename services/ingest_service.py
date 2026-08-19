import time

from flask import current_app
from reactivex import from_iterable, operators as ops

from extensions import db, pool_scheduler  # This is the centralised DB connection
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


def _ingest_batch(batch):
  if not batch:  # Kick out if handed nothing
    return 0

  titles_to_add = []
  for record in batch:
    title = Title(
      title=record.get("title", record.get("tippTitleName", "Untitled")),
      publication_type=record.get("publicationType", "Serial"),
      raw_record=record,
    )

    # Map nested identifier objects if present in raw GOKb payload
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

  def _ingest_batch_internal(batch):
    nonlocal total_ingested
    total_ingested += _ingest_batch(batch)

  ## Now rx streaming stuff
  from_iterable(stream_http_records(page_size)).pipe(
    ops.buffer_with_count(batch_size)
  ).subscribe(
    on_next=_ingest_batch_internal,
    on_error=lambda err: print(f"GOKb Ingest Error: {err}"),
    on_completed=lambda: print("GOKb harvest pipeline complete.")
  )

  duration = time.perf_counter_ns() - start_time
  return {
    "ingest_count": total_ingested,
    "execution_time": duration
  }


## ATTEMPT to handle this with streaming

def _process_and_commit_batch(batch, app):
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


def stream_scroll_ingest(batch_size=500, page_size=1000):
  app = current_app._get_current_object()
  return from_iterable(stream_http_records(page_size)).pipe(
    ops.subscribe_on(pool_scheduler), ## TODO I'm not 100% sure what's happening once we're handling threads like this
    ops.buffer_with_count(batch_size),
    ops.map(lambda batch: (batch, _process_and_commit_batch(batch, app))),
  )