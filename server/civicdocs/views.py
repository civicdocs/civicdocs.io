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
    #DocumentTypeAssignments,
    Jobs,
    Scrapers,
    Dispatchers,
    Workers,
    JobRuns,
    Documents,
    #DocumentCategoryAssignments,
    DocumentCategories,
)

def auth_scraper(request):
    scraper = None
    #try:
    if True:
        #payload = json.loads(request.json_body)
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
    #except:
    #    request.response.status = 400
    return scraper

@view_config(request_method='POST', route_name='/dispatchers/announce', renderer='json')
def view_dispatchers_announce(request):
    resp = {}
    scraper = auth_scraper(request)
    if scraper:
        dispatcher = Dispatchers.add(
            scraper_id=scraper.id,
            current_job_id=None,
            current_job_run_id=None,
            last_callin_datetime=datetime.datetime.now(),
            up_time=0,
            idle=True,
        )
        resp = dict(
            dispatcher=dispatcher.to_dict(),
        )
        request.response.status = 200
    else:
        resp = dict(
        )
        request.response.status = 403
    resp.update(status=request.response.status)
    return resp


@view_config(request_method='GET', route_name='/dispatchers/jobs', renderer='json')
def view_dispatchers_jobs(request):
    resp = {}
    scraper = auth_scraper(request)
    if scraper:
        job = None
        _job = Jobs.get_job()
        if _job is not None:
            job = _job.to_dict()
            job_run = JobRuns.add(
                job_id=_job.id,
                scraper_id=scraper.id,
                start_datetime=datetime.datetime.now(),
                finished=False,
                finish_datetime=None,
            )
            job.update(
                job_run_id=str(job_run.id),
            )
        resp = dict(job=job)
    else:
        resp = dict(
            job=None,
        )
    resp['status'] = request.response.status
    return resp


@view_config(request_method='POST', route_name='/dispatchers/status/{id}', renderer='json')
def view_dispatchers_status_post(request):
    resp = {}
    scraper = auth_scraper(request)
    if scraper:
        dispatcher = Dispatchers.get_by_id(request.matchdict['id'])
        if dispatcher:
            payload = {}
            try:
                payload = request.json_body
            except:
                request.response.status = 400
            keys = ('up_time', 'idle')
            if all(key in payload for key in keys):
                dispatcher = Dispatchers.get_by_id(id=request.matchdict['id'])
                if dispatcher is None:
                    dispatcher = Dispatchers.add(
                        scraper_id=scraper.id,
                        current_job_id=None,
                        current_job_run_id=None,
                        last_callin_datetime=datetime.datetime.now(),
                        up_time=payload['up_time'],
                        idle=payload['idle'],
                    )
                request.response.status = 200
        else:
            request.response.status = 400
    resp.update(status=request.response.status)
    return resp

@view_config(request_method='POST', route_name='/workers/announce', renderer='json')
def view_workers_announce(request):
    resp = {}
    scraper = auth_scraper(request)
    if scraper:
        payload = {}
        try:
            payload = request.json_body
        except:
            request.response.status = 400
        keys = ('dispatcher_id',)
        if all(key in payload for key in keys):
            worker = Workers.add(
                scraper_id=scraper.id,
                dispatcher_id=payload['dispatcher_id'],
                last_callin_datetime=datetime.datetime.now(),
                up_time=0,
            )
            resp = dict(
                worker=worker.to_dict(),
            )
            request.response.status = 200
        else:
            request.response.status = 400
    else:
        resp = dict(
        )
        request.response.status = 403
    resp.update(status=request.response.status)
    return resp

@view_config(request_method='POST', route_name='/workers/document', renderer='json')
def view_workers_documents(request):
    resp = {}
    scraper = auth_scraper(request)
    if scraper:
        payload = {}
        #try:
        if True:
            payload = request.json_body
        #except:
        #    request.response.status = 400
        #keys = ('',)
        #if all(key in payload for key in keys):
        if True:
            print("\r\n\r\n"); print(payload); print("\r\n\r\n")
            document_type = DocumentTypes.get_by_mine(payload['doc_type'])
            document = Documents.add(
                name="",
                description="",
                url=payload['url'],
                source_url=payload['source_url'],
                source_url_title="", #payload['source_url_title'],
                link_text="", #payload['link_text'],
                document_type_id=document_type.id,   
            )
            resp = dict(
                document=document.to_dict(),
            )
            request.response.status = 200
        else:
            request.response.status = 400
    else:
        resp = dict(
        )
        request.response.status = 403
    resp.update(status=request.response.status)
    return resp
