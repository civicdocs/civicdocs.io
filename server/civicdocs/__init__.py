from pyramid.config import Configurator
from sqlalchemy import engine_from_config

from .models import (
    DBSession,
    Base,
    )


def main(global_config, **settings):
    """ This function returns a Pyramid WSGI application.
    """
    engine = engine_from_config(settings, 'sqlalchemy.',
                                isolation_level="AUTOCOMMIT")
    DBSession.configure(bind=engine)
    Base.metadata.bind = engine
    config = Configurator(settings=settings)
    config.include('pyramid_chameleon')
    config.add_static_view('static', 'static', cache_max_age=3600)

    config.add_route('/dispatchers', '/dispatchers')
    config.add_route('/dispatchers/{id}', '/dispatchers/{id}')
    config.add_route('/dispatchers/{id}/jobs', '/dispatchers/{id}/jobs')

    config.add_route('/workers', '/workers')
    config.add_route('/workers/{id}', '/workers/{id}')
    config.add_route('/workers/{id}/document', '/workers/{id}/document')

    config.add_route('/documents', '/documents')

    config.add_route('/search', '/search')

    config.scan()
    return config.make_wsgi_app()
