# Centralise DB connection /migrations once
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy.orm import DeclarativeBase

from flask_migrate import Migrate

from reactivex.scheduler import ThreadPoolScheduler

## Centralise pool_scheduler?
pool_scheduler = ThreadPoolScheduler(1)

class Base(DeclarativeBase):
    pass

db = SQLAlchemy(model_class=Base)
migrate = Migrate()