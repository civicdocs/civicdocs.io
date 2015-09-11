import datetime

from pyramid.response import Response
from pyramid.view import view_config

from sqlalchemy.exc import DBAPIError

from .models import (
    Users,
    Municipalities,
    Entities,
    Addresses,
    DocumentTypes,
    Jobs,
    Scrapers,
    Dispatchers,
    Workers,
    JobRuns,
    Documents,
    DocumentCategories,
)


def get_keys(cls):
    req_keys = []
    opt_keys = []
    table = str(cls.__table__)
    for col in cls.__table__.columns:
        name = str(col)[len(table)+1:]
        if not col.nullable:
            req_keys.append(name)
        else:
            opt_keys.append(name)
    req_keys.remove('id')  # do not need the id
    opt_keys.remove('creation_datetime')
    opt_keys.remove('modified_datetime')
    opt_keys.remove('last_callin_datetime')
    return req_keys, opt_keys


def auth_scraper(request):
    scraper = None
    try:
        token = request.GET.get('token', None)
        if token is not None:
            _scraper = Scrapers.get_by_token(token)
            if _scraper is not None:
                scraper = _scraper
                request.response.status = 200
            else:
                request.response.status = 403
        else:
            request.response.status = 403
    except:
        request.response.status = 400
    return scraper


def get_payload(request, cls, update=False):
    payload = None
    try:
        payload = request.json_body
        req_keys, opt_keys = get_keys(cls)
        all_keys = True
        if update:
            all_keys = all(key in payload for key in opt_keys)
        if all(key in payload for key in req_keys) and all_keys:
            request.response.status = 200
        else:
            payload = None
            request.response.status = 200
    except:
        request.response.status = 400
    return payload


def do_get(request, cls):
    id = request.matchdict['id']
    thing = cls.get_by_id(id)
    if thing:
        resp = {str(cls.__table__): thing.to_dict()}
        request.response.status = 200
    else:
        resp = {}
        request.response.status = 404
    return resp


def do_get_paged(request, cls):
    start = 0
    count = 50
    if 'start' in request.GET and request.GET['start'].isdigit():
        start = int(request.GET['start'])
    if 'count' in request.GET and request.GET['count'].isdigit():
        count = int(request.GET['count'])
    things = cls.get_paged(start, count)
    return things


def do_post(request, cls, extras={}):
    resp = {}
    payload = get_payload(request, cls)
    if payload:
        try:
            for key in extras:
                payload[key] = extras[key]
            thing = cls.add(**payload)
            if thing:
                resp = {str(cls.__table__): thing.to_dict()}
            else:
                request.response.status = 403
        except Exception as e:
            resp = dict(error=str(e))
            request.response.status = 500
    else:
        request.response.status = 400
    return resp


def do_put(request, cls, extras={}):
    resp = {}
    id = request.matchdict['id']
    payload = get_payload(request, cls, update=True)
    if payload:
        try:
            for key in extras:
                payload[key] = extras[key]
            thing = cls.update_by_id(id, **payload)
            if thing:
                resp = {str(cls.__table__): thing.to_dict()}
            else:
                request.response.status = 404
        except Exception as e:
            resp = {'error': str(e)}
            request.response.status = 500
    else:
        request.response.status = 400
    return resp


@view_config(request_method='POST', route_name='/dispatchers', renderer='json')
def view_dispatchers_post(request):
    payload = get_payload(request, Dispatchers, update=False)
    if payload and Scrapers.get_by_id(payload['scraper_id']):
        extras = dict(
            last_callin_datetime=datetime.datetime.now()
        )
        resp = do_post(request, Dispatchers, extras=extras)
    else:
        resp = {}
        request.response.status = 403
    return resp


@view_config(request_method='GET', route_name='/dispatchers', renderer='json')
def view_dispatchers_get(request):
    dispatchers = do_get_paged(request, Dispatchers)
    if dispatchers and isinstance(dispatchers, list):
        resp = dict(
            dispatchers=[w.to_dict() for w in dispatchers]
        )
        request.response.status = 200
    else:
        resp = dict(dispatchers=None)
        request.response.status = 404

    return resp


@view_config(request_method='GET', route_name='/dispatchers/{id}/jobs',
             renderer='json')
def view_dispatcher_jobs_get(request):
    dispatcher = Dispatchers.get_by_id(request.matchdict['id'])
    if dispatcher:
        _job = Jobs.get_job()
        if _job:
            resp = dict(job=_job.to_dict())
        else:
            resp = dict(job=None)
        request.response.status = 200
    else:
        resp = {}
        request.response.status = 404
    return resp


@view_config(request_method='PUT', route_name='/dispatchers/{id}',
             renderer='json')
def view_dispatcher_post(request):
    payload = get_payload(request, Dispatchers, update=False)
    if payload and Scrapers.get_by_id(payload['scraper_id']):
        extras = dict(
            last_callin_datetime=datetime.datetime.now()
        )
        resp = do_put(request, Dispatchers)
        request.response.status = 200
    else:
        resp = {}
        request.response.status = 400
    return resp


@view_config(request_method='POST', route_name='/workers', renderer='json')
def view_workers(request):
    payload = get_payload(request, Workers, update=False)
    if payload:
        if Scrapers.get_by_id(payload['scraper_id']) and \
                Dispatchers.get_by_id(payload['dispatcher_id']):
            extras = dict(
                last_callin_datetime=datetime.datetime.now()
            )
            resp = do_post(request, Workers, extras=extras)
        else:
            resp = {}
            request.response.status = 403
    else:
        resp = {}
        request.response.status = 400
    return resp


@view_config(request_method='GET', route_name='/workers', renderer='json')
def view_workers_get(request):
    workers = do_get_paged(request, Workers)
    if workers and isinstance(workers, list):
        resp = dict(
            workers=[w.to_dict() for w in workers]
        )
        request.response.status = 200
    else:
        resp = dict(workers=None)
        request.response.status = 404

    return resp


@view_config(request_method='PUT', route_name='/workers/{id}', renderer='json')
def view_worker_post(request):
    payload = get_payload(request, Workers, update=False)
    if payload and Scrapers.get_by_id(payload['scraper_id']):
        extras = dict(
            last_callin_datetime=datetime.datetime.now()
        )
        resp = do_put(request, Workers, extras=extras)
        request.response.status = 200
    else:
        resp = {}
        request.response.status = 400
    return resp


@view_config(request_method='POST', route_name='/workers/{id}/document',
             renderer='json')
def view_worker_documents_post(request):
    worker = Workers.get_by_id(request.matchdict['id'])
    if worker:
        payload = get_payload(request, Documents)
        if payload:
            document = Documents.add(**payload)
            if document:
                resp = dict(job=document.to_dict())
                request.response.status = 200
            else:
                resp = dict(job=None)
                request.response.status = 400
    else:
        resp = {}
        request.response.status = 404
    return resp


@view_config(request_method='GET', route_name='/documents', renderer='json')
def view_documents_get(request):

    return resp


@view_config(request_method='GET', route_name='/search', renderer='json')
def view_search_get(request):

    return resp
