import os
import sys
import uuid
import transaction


from sqlalchemy import engine_from_config

from pyramid.paster import (
    get_appsettings,
    setup_logging,
    )

from pyramid.scripts.common import parse_vars

from ..models import (
    DBSession,
    Base,
    DocumentTypes,
    Users,
    Scrapers,
    )


def usage(argv):
    cmd = os.path.basename(argv[0])
    print('usage: %s <config_uri> [var=value]\n'
          '(example: "%s development.ini")' % (cmd, cmd))
    sys.exit(1)


def main(argv=sys.argv):
    if len(argv) < 2:
        usage(argv)
    config_uri = argv[1]
    options = parse_vars(argv[2:])
    setup_logging(config_uri)
    settings = get_appsettings(config_uri, options=options)
    engine = engine_from_config(settings, 'sqlalchemy.')
    DBSession.configure(bind=engine)
    Base.metadata.create_all(engine)
    #with transaction.manager:
    #    model = MyModel(name='one', value=1)
    #    DBSession.add(model)

    application_pdf_doc_type = DocumentTypes.add(
        name="Adobe PDF",
        description="Adobe PDF file",
        mime_type="application/pdf",
    )

    system_owner = Users.add(
        first = "SYSTEM",
        last = "USERS",
        email = "system@localhost",
        password = "password",
    )

    default_scraper = Scrapers.add(
        name="Default Scraper",
        description="CivicDocs.IO loads with a single, defualt scraper.",
        token='{0}-{1}'.format(str(uuid.uuid4()), str(uuid.uuid4())),
        owner_id = system_owner.id,
    )
    print("DEFAULT SCRAPER TOKEN:\r\n{0}\r\n".format(default_scraper.token))
