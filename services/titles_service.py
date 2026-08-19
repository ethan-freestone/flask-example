import time
from typing import Optional

from reactivex import from_iterable, operators as ops

from extensions import db
from models.tipp import Title, Identifier


def fetch_record_ids(chunk_size=1000):
  """Yields remaining record IDs in batches.

    Since records are deleted after each batch commit, we repeatedly
    fetch the top remaining IDs until the query returns empty.
    """
  while True:
    # Fetch the next top batch of remaining IDs
    batch = db.session.query(Title.id).limit(chunk_size).all()

    if not batch:
      break

    for row in batch:
      yield row.id


def nuke_titles():
  start_time = time.perf_counter_ns()
  deleted_count = _delete_batch()
  duration = time.perf_counter_ns() - start_time
  return {
    "deleted_count": deleted_count,
    "execution_time": duration
  }


def nuke_titles_chunked(batch_size=1000):
  start_time = time.perf_counter_ns()
  total_deleted = 0

  def handle_batch(id_batch):
    nonlocal total_deleted
    total_deleted += _delete_batch(id_batch)

  from_iterable(fetch_record_ids(chunk_size=batch_size)).pipe(
    ops.buffer_with_count(batch_size)
  ).subscribe(
    on_next=handle_batch,
    on_error=lambda err: print(f"RxPY Error: {err}")
  )

  duration = time.perf_counter_ns() - start_time
  return {
    "deleted_count": total_deleted,
    "execution_time": duration
  }


def _delete_batch(id_batch: Optional[list[str]] = None):
  if id_batch is None:
    db.session.query(Identifier).delete(synchronize_session=False)
    deleted_count = db.session.query(Title).delete(synchronize_session=False)
  else:
    db.session.query(Identifier).filter(Identifier.title_id.in_(id_batch)).delete(synchronize_session=False)
    deleted_count = db.session.query(Title).filter(Title.id.in_(id_batch)).delete(synchronize_session=False)

  db.session.commit()
  return deleted_count