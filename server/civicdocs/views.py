from pyramid.response import Response
from pyramid.view import view_config

from sqlalchemy.exc import DBAPIError

from .models import (
    Users,
    Municipalities,
    Entities,
    Addresses,
    DocumentTypes,
    #DocumentTypeAssignments,
    Jobs,
    Scrapers,
    JobRuns,
    Documents,
    #DocumentCategoryAssignments,
    DocumentCategories,
)

@view_config(route_name='/dispatchers/jobs', renderer='json')
def view_dispatchers_jobs(request):

    job = None
    _job = Jobs.get_job()
    print("\r\n")
    print(_job)
    print("\r\n")
    if _job is not None:
        job = _job.to_dict()
    resp = dict(
        job=job,
    )
    return resp
