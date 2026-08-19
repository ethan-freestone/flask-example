from typing import Optional

from sqlalchemy import ForeignKey, String, Index
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy.dialects.postgresql import JSONB

from extensions import db

# --- Domain Models ---

class Title(db.Model):
    __tablename__ = "titles"

    ## TODO For now sequential id is fine, look into UUID5
    id: Mapped[int] = mapped_column(primary_key=True)
    raw_record: Mapped[dict] = mapped_column(JSONB, nullable=False)
    # Added index for title searches/sorting
    title: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    publication_type: Mapped[Optional[str]] = mapped_column(String(255))

    identifiers: Mapped[list["Identifier"]] = relationship(
        "Identifier", back_populates="title", cascade="all, delete-orphan"
    )

    def to_dict(self) -> dict:
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
    # index=True on ForeignKey solves the slow DELETE issue
    title_id: Mapped[int] = mapped_column(
        ForeignKey("titles.id", ondelete="CASCADE"),
        nullable=False,
        index=True
    )
    id_type: Mapped[str] = mapped_column(String(50), nullable=False)  # e.g., 'issn', 'doi'
    id_value: Mapped[str] = mapped_column(String(100), nullable=False)

    title: Mapped["Title"] = relationship("Title", back_populates="identifiers")

    # Composite index for fast identifier resolution lookups (e.g. type='issn' AND value='1234-5678')
    __table_args__ = (
        Index("idx_identifier_type_value", "id_type", "id_value"),
    )

    def to_dict(self) -> dict:
        return {"id_type": self.id_type, "id_value": self.id_value}