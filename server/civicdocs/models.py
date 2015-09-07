from uuid import uuid4
import hashlib
import datetime

from sqlalchemy.sql import func
from sqlalchemy_utils import UUIDType
from sqlalchemy import (
    Column,
    ForeignKey,
    Integer,
    UnicodeText,
    DateTime,
    Float,
    Index,
    Boolean,
)

from sqlalchemy import(
    update,
    desc,
)

from sqlalchemy.ext.declarative import declarative_base

from sqlalchemy.orm import (
    relationship,
    scoped_session,
    sessionmaker,
)

from zope.sqlalchemy import ZopeTransactionExtension
import transaction

DBSession = scoped_session(sessionmaker(
    extension=ZopeTransactionExtension(keep_session=True),
    expire_on_commit=False))
Base = declarative_base()


class TimeStampMixin(object):
    creation_datetime = Column(DateTime, server_default=func.now())
    modified_datetime = Column(DateTime, server_default=func.now())

class CreationMixin():

    id = Column(UUIDType(binary=False), primary_key=True)

    @classmethod
    def add(cls, **kwargs):
        with transaction.manager:
            thing = cls(**kwargs)
            if thing.id is None:
                thing.id = uuid4()
            DBSession.add(thing)
            transaction.commit()
        return thing

    @classmethod
    def get_all(cls):
        with transaction.manager:
            things = DBSession.query(
                cls,
            ).all()
        return things


    @classmethod
    def get_paged(cls, start=0, count=10):
        with transaction.manager:
            things = DBSession.query(
                cls,
            ).slice(
                start,
                start+count
            ).all()
        return things

    @classmethod
    def get_by_id(cls, id):
        with transaction.manager:
            thing = DBSession.query(
                cls,
            ).filter(
                cls.id == id,
            ).first()
        return thing

    @classmethod
    def delete_by_id(cls, id):
        with transaction.manager:
            thing = cls.get_by_id(id)
            if thing is not None:
                DBSession.delete(thing)
            transaction.commit()
        return thing

    @classmethod
    def update_by_id(cls, id, **kwargs):
        with transaction.manager:
            keys = set(cls.__dict__)
            thing = cls.get_by_id(id)
            if thing is not None:
                for k in kwargs:
                    if k in keys:
                        setattr(thing, k, kwargs[k])
                DBSession.add(thing)
                transaction.commit()
        return thing

    @classmethod
    def reqkeys(cls):
        keys = []
        for key in cls.__table__.columns:
            if '__required__' in type(key).__dict__:
                keys.append(str(key).split('.')[1])
        return keys

    def to_dict(self):
        return {
            'id': str(self.id),
            'creation_datetime': str(self.creation_datetime),
        }


class Users(Base, CreationMixin, TimeStampMixin):
    __tablename__ = 'users'

    first = Column(UnicodeText, nullable=False)
    last = Column(UnicodeText, nullable=False)
    email = Column(UnicodeText, nullable=False)
    pass_salt = Column(UnicodeText, nullable=True)
    pass_hash = Column(UnicodeText, nullable=True)
    login_datetime = Column(DateTime, nullable=True)
    
    home_municipality_id = Column(
        UUIDType(binary=False),
        ForeignKey('municipalities.id'),
        nullable=True,
    )

    scrapers = relationship('Scrapers', backref='owner', lazy='joined')

    @classmethod
    def add(self, password, **kwargs):
        user = super(Users, self).add(**kwargs)
        with transaction.manager:
            pass_salt = str(uuid4())
            pass_hash = hashlib.sha256('{0}{1}'.format(
                password,
                pass_salt,
            ).encode('utf-8')).hexdigest()
            user.pass_salt = pass_salt
            user.pass_hash = pass_hash
            transaction.commit()
        return user

    def to_dict(self):
        resp = super(Users, self).to_dict()
        resp.update(
            first = self.first,
            last = self.last,
            email = self.email,
        )
        return resp


class Municipalities(Base, CreationMixin, TimeStampMixin):
    __tablename__ = 'municipalities'

    name = Column(UnicodeText, nullable=False)
    description = Column(UnicodeText, nullable=False)
    url = Column(UnicodeText, nullable=False)

    addresses = relationship(
        "Addresses", backref="municipality", lazy="joined"
    )
    entities = relationship(
         "Entities", backref="municipality", lazy="joined"
    )
    #locations = relationship(
    #    "Locations", backref="locations", lazy="joined"
    #)

    jobs = relationship("Jobs", backref="municipality", lazy="joined")

    def to_dict(self):
        resp = super(Municipalities, self).to_dict()
        resp.update(
            name = self.name,
            description = self.description,
            addresses = [a.to_dict() for a in self.addresses],
            entities = [o.to_dict() for o in self.entities],
        )
        return resp


Index('index_municipalities_id', Municipalities.id, unique=True)
Index(
    'index_municipalities_name', 
    Municipalities.name,
    unique=True,
    mysql_length=255
)
#Index('index_municipalities_description', Municipalities.description, unique=True, mysql_length=4096)
#Index('index_municipalities_url', Municipalities.url, unique=True, mysql_length=255)


class Entities(Base, CreationMixin, TimeStampMixin):
    __tablename__ = 'entities'

    name = Column(UnicodeText, nullable=False)
    description = Column(UnicodeText, nullable=False)
    municipality_id = Column(
        UUIDType(binary=False),
        ForeignKey('municipalities.id'),
        nullable=False,
    )

    addresses = relationship("Addresses", backref="entity", lazy="joined");

    def to_dict(self):
        resp = super(Entities, self).to_dict()
        resp.update(
            name = self.name,
            description = self.description,
            municipality_id = self.municipality_id,
            addresses = [a.to_dict() for a in self.addresses],
        )
        return resp


Index('index_entities_id', Entities.id, unique=True)
Index(
    'index_entities_name',
    Entities.name,
    unique=True,
    mysql_length=255
)
#Index('index_entities_description', Entities.description, unique=True, mysql_length=4096)


class Addresses(Base, CreationMixin, TimeStampMixin):
    __tablename__ = 'addresses'

    name = Column(UnicodeText, nullable=False)
    description = Column(UnicodeText, nullable=False)
    address_0 = Column(UnicodeText, nullable=False)
    address_1 = Column(UnicodeText, nullable=False)
    city = Column(UnicodeText, nullable=False)
    state = Column(UnicodeText, nullable=False)
    zipcode = Column(UnicodeText, nullable=False)
    lat = Column(Float, nullable=False)
    lng = Column(Float, nullable=False)
    
    municipality_id = Column(
        UUIDType(binary=False),
        ForeignKey('municipalities.id'),
        nullable=True
    )
    entity_id = Column(
        UUIDType(binary=False),
        ForeignKey('entities.id'),
        nullable=True,
    )

    def to_dict(self):
        resp = super(Addresses, self).to_dict()
        resp.update(
            name = self.name,
            description = self.description,
            address_0 = self.address_0,
            address_1 = self.address_1,
            city = self.city,
            state = self.state,
            zipcode = self.zipcode,
        )
        return resp


Index('index_addresses_id', Addresses.name, unique=True)
Index(
    'index_addresses_name',
    Addresses.name,
    unique=True,
    mysql_length=255,
)


class DocumentTypes(Base, CreationMixin, TimeStampMixin):
    __tablename__ = 'document_types'

    name = Column(UnicodeText, nullable=False)
    description = Column(UnicodeText, nullable=False)
    mime_type = Column(UnicodeText, nullable=False)

    documents = relationship(
        'Documents',
        backref='document_type',
        lazy='joined',
    )

    @classmethod
    def get_by_mine(self, mime_type):
        with transaction.manager:
            document_type = DBSession.query(
                DocumentTypes,
            ).filter(
                DocumentTypes.mime_type == mime_type,
            ).first()
        return document_type

    def to_dict(self):
        resp = super(DocumentTypes, self).to_dict()
        resp.update(
            name = self.name,
            description = self.description,
            mime_type = self.mime_type,
        )
        return resp


Index('index_document_types_id', DocumentTypes.id, unique=True)
Index(
    'index_document_types_name',
    DocumentTypes.name,
    unique=True,
    mysql_length=255,
)
    

class DocumentTypeAssignments(Base, CreationMixin, TimeStampMixin):
    __tablename__ = 'document_type_assignments'
    
    document_type_id = Column(
        UUIDType(binary=False),
        ForeignKey('document_types.id'),
        nullable=False
    )
    job_id = Column(
        UUIDType(binary=False),
        ForeignKey('jobs.id'),
        nullable=False,
    )


Index(
    'index_document_type_assignments_id',
    DocumentTypeAssignments.id,
    unique=True
)
Index(
    'index_document_type_assignments_document_typeid',
    DocumentTypeAssignments.document_type_id,
    unique=True,
)
Index(
    'index_document_type_assignments_job_id',
    DocumentTypeAssignments.job_id,
    unique=True,
)


class Jobs(Base, CreationMixin, TimeStampMixin):
    __tablename__ = 'jobs'

    name = Column(UnicodeText, nullable=False)
    description = Column(UnicodeText, nullable=False)
    url = Column(UnicodeText, nullable=False)
    link_level = Column(Integer, nullable=False)
    in_process = Column(Boolean, nullable=False)
    start_run_datetime = Column(DateTime)
    last_run_datetime = Column(DateTime)

    municipality_id = Column(
        UUIDType(binary=False),
        ForeignKey('municipalities.id'),
        nullable=False,
    )

    job_runs = relationship('JobRuns', backref='job', lazy='joined')

    dispatcher = relationship(
        'Dispatchers',
        backref='current_job',
        lazy='joined',
    )

    #document_types = relationship(
    #    'DocumentTypes',
    #    secondary = DocumentTypeAssignments.__table__,
    #    backref = 'job',
    #    lazy = 'joined',
    #)

    @classmethod
    def get_job(self):
        '''
        This get's the next oldest job.  This uses the `in_process` as a
        semaphor against the row in the table.  We get the next oldest
        job that isn't in process.  This results in a round robin of all
        jobs being processed as fast as the available scraper clusters
        can handle them.
        '''
        with transaction.manager:
            q = update(Jobs).values({
                'in_process': True,
                'start_run_datetime': datetime.datetime.now(),
            }).where(
                Jobs.in_process == False,
            ).where(
                Jobs.id == DBSession.query(
                    Jobs.id,
                ).order_by(
                    desc(Jobs.last_run_datetime),
                ).limit(1).with_for_update().as_scalar()
            ).returning(Jobs.id)
            result = DBSession.execute(q)
            job = None
            for row in result:
                job = Jobs.get_by_id(row.id)
                break
            transaction.commit()
        return job

    def to_dict(self):
        resp = super(Jobs, self).to_dict()
        resp.update(
            name = self.name,
            description = self.description,
            url = self.url,
            link_level = self.link_level,
            start_run_datetime = str(self.start_run_datetime),
            in_process = self.in_process,
            last_run_datetime = str(self.last_run_datetime),
        )
        return resp


Index('index_jobs_id', Jobs.id, unique=True)
Index('index_jobs_name', Jobs.name, unique=True, mysql_length=255)
Index('index_jobs_url', Jobs.name, unique=True, mysql_length=1024)


class Scrapers(Base, CreationMixin, TimeStampMixin):
    __tablename__ = 'scrapers'

    name = Column(UnicodeText, nullable=False)
    description = Column(UnicodeText, nullable=False)
    token = Column(UnicodeText, nullable=False)
    owner_id = Column(
        UUIDType(binary=False),
        ForeignKey('users.id'),
        nullable=True, # TODO: may want to rethink this ...
    )
    dispatcher = relationship('Dispatchers', backref='scraper', lazy='joined')
    workers = relationship('Workers', backref='scraper', lazy='joined')

    @classmethod
    def get_by_token(self, token):
        with transaction.manager:
            scraper = DBSession.query(
                Scrapers,
            ).filter(
                Scrapers.token == token,
            ).first()
        return scraper

    def to_dict(self):
        resp = super(Scrapers, self).to_dict()
        resp.update(
            name = self.name,
            description = self.description,
            token = self.token,
            owner = self.owner.to_dict(),
        )
        return resp


Index('index_scrapers_id', Jobs.name, unique=True)
Index('index_scrapers_name', Jobs.name, unique=True, mysql_length=255)


class Workers(Base, CreationMixin, TimeStampMixin):

    __tablename__ = 'workers'
    scraper_id = Column(
        UUIDType(binary=False),
        ForeignKey('scrapers.id'),
        nullable=False,
    )
    dispatcher_id = Column(
        UUIDType(binary=False),
        ForeignKey('dispatchers.id'),
        nullable=False,
    )
    last_callin_datetime = Column(DateTime, nullable=False)
    up_time = Column(Integer, nullable=False)

    #dispatcher = relationship('Dispatchers', backref='worker', lazy='joined')

    def to_dict(self):
        resp = super(Workers, self).to_dict()
        resp.update(
            scraper_id=str(self.scraper_id),
            dispatcher_id=str(self.dispatcher_id),
            last_callin_datetime=str(self.last_callin_datetime),
            up_time=self.up_time,
        )
        return resp

class Dispatchers(Base, CreationMixin, TimeStampMixin):

    __tablename__ = 'dispatchers'
    scraper_id = Column(
        UUIDType(binary=False),
        ForeignKey('scrapers.id'),
        nullable=False,
    )
    current_job_id = Column(
        UUIDType(binary=False),
        ForeignKey('jobs.id'),
        nullable=True,
    )
    current_job_run_id = Column(
        UUIDType(binary=False),
        ForeignKey('job_runs.id'),
        nullable=True,
    )
    last_callin_datetime = Column(DateTime, nullable=False)
    up_time = Column(Integer, nullable=False)
    idle = Column(Boolean, nullable=False)
    workers = relationship('Workers', backref='dispatcher', lazy='joined')

    def to_dict(self):
        resp = super(Dispatchers, self).to_dict()
        current_job = None
        if self.current_job is not None:
            current_job = self.current_job.to_dict()
        current_job_run = None
        if self.current_job_run is not None:
            current_job_run = self.current_job_run.to_dict()
        resp.update(
            scraper=self.scraper.to_dict(),
            wrokers=[w.to_dict() for w in self.workers],
            current_job=self.current_job,
            current_job_run=self.current_job_run,
        )
        return resp


class JobRuns(Base, CreationMixin, TimeStampMixin):
    __tablename__ = 'job_runs'

    job_id = Column(
        UUIDType(binary=False),
        ForeignKey('jobs.id'),
        nullable=False,
    )
    scraper_id = Column(
        UUIDType(binary=False),
        ForeignKey('scrapers.id'),
        nullable=False
    )
    start_datetime = Column(DateTime, nullable=False)
    finished = Column(Boolean, nullable=False)
    finish_datetime = Column(DateTime, nullable=True)

    dispatcher = relationship(
        'Dispatchers',
        backref='current_job_run',
        lazy='joined',
    )

    def to_dict(self):
        resp = super(JobRuns, self).to_dict()
        resp.update(
            job = self.job.to_dict(),
            scraper = self.scraper.to_dict(),
            status = self.status,
            finish_datetime = str(self.finish_datetime),
        )
        return resp
            

class DocumentCategoryAssignments(Base, CreationMixin, TimeStampMixin):
    __tablename__ = 'document_category_assignments'

    document_id = Column(
        UUIDType(binary=False),
        ForeignKey('documents.id'),
        nullable=False,
    )
    document_category_id = Column(
        UUIDType(binary=False),
        ForeignKey('document_categories.id'),
        nullable=False,
    )


Index(
    'index_document_category_assignments_id',
    DocumentCategoryAssignments.id,
)
Index(
    'index_document_category_assignments_document_id',
    DocumentCategoryAssignments.document_id,
)
Index(
    'index_document_category_assignments_document_category_id',
    DocumentCategoryAssignments.document_category_id,
)


class Documents(Base, CreationMixin, TimeStampMixin):
    __tablename__ = 'documents'

    name = Column(UnicodeText, nullable=False)
    description = Column(UnicodeText, nullable=False)
    url = Column(UnicodeText, nullable=False)
    source_url = Column(UnicodeText, nullable=False)
    source_url_title = Column(UnicodeText, nullable=False)
    link_text = Column(UnicodeText, nullable=False)

    document_type_id = Column(
        UUIDType(binary=False),
        ForeignKey('document_types.id'),
        nullable=False,
    )

    categories = relationship(
        'DocumentCategories',
        secondary = DocumentCategoryAssignments.__table__,
        backref = 'document',
        lazy = 'joined',
    )

    def to_dict(self):
        resp = super(Documents, self).to_dict()
        resp.update(
            name = self.name,
            description = self.description,
            source_url = self.source_url,
            source_url_title = self.source_url_title,
            link_text = self.link_text,
            categories = [c.to_dict() for c in self.categories],
        )
        return resp


Index('index_documents_id', Documents.id, unique=True)
Index(
    'index_documents_name',
    Documents.name,
    #unique=True,
    #mysql_length=255,
)
Index(
    'index_documents_description',
    Documents.description,
    #unique=True,
    #mysql_length=255,
)


class DocumentCategories(Base, CreationMixin, TimeStampMixin):
    __tablename__ = 'document_categories'

    name = Column(UnicodeText, nullable=False)
    description = Column(UnicodeText, nullable=False)

    def to_dict(self):
        resp = super(Jobs, self).to_dict()
        resp.update(
            name = self.name,
            description = self.description,
        )
        return resp


Index('index_document_categories_id', DocumentCategories.id, unique=True)
Index(
    'index_document_categories_name',
    DocumentCategories.name,
    unique=True,
    mysql_length=255,
)

    
class DocumentAudits(Base, CreationMixin, TimeStampMixin):
    __tablename__ = 'document_audits'

    
class DocumentChangeAudits(Base, CreationMixin, TimeStampMixin):
    __tablename__ = 'document_change_audits'


