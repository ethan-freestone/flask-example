from extensions import db  # This is the centralised DB connection
from models.tipp import Title, Identifier

# Single test title ingest
def run_test_ingest():
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

    return 1
