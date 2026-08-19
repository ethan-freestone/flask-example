from typing import Optional

from sqlalchemy import ForeignKey, String
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy.dialects.postgresql import JSONB
# Can I import the DB connnection like this?
from extensions import db
# --- Domain Models ---

class Title(db.Model):
    __tablename__ = "titles"

    ## TODO For now sequential id is fine, look into UUID5
    id: Mapped[int] = mapped_column(primary_key=True)
    raw_record: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=False)
    title: Mapped[str] = mapped_column(String(255), nullable=False)
    publication_type: Mapped[Optional[str]] = mapped_column(String(255))

    # TODO Setting up a JOIN directly -- figure out how ownership mappings work
    identifiers: Mapped[list["Identifier"]] = relationship(
        "Identifier", back_populates="title", cascade="all, delete-orphan"
    )

    def to_dict(self):
        return {
            "id": self.id,
            "title": self.title,
            "raw_record": self.raw_record,
            "publication_type": self.publication_type,
            "identifiers": [i.to_dict() for i in self.identifiers],
        }


class Identifier(db.Model):
    __tablename__ = "identifiers"

    id: Mapped[int] = mapped_column(primary_key=True)
    title_id: Mapped[int] = mapped_column(ForeignKey("titles.id"), nullable=False)
    id_type: Mapped[str] = mapped_column(String(50), nullable=False)  # e.g., 'issn', 'eissn', 'doi'
    id_value: Mapped[str] = mapped_column(String(100), nullable=False)

    title: Mapped["Title"] = relationship("Title", back_populates="identifiers")

    def to_dict(self):
        return {"id_type": self.id_type, "id_value": self.id_value}