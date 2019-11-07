import logging.config

import sentry_sdk

from .avro_from_sql import *
from .constants import *
from .kafka import *
from .main import *
from .options import *
from .tracked_tables import *
from .validation import *

sentry_sdk.init()

log_level = os.getenv('LOG_LEVEL', 'INFO').upper()

logging.config.dictConfig({
    'version': 1,
    'disable_existing_loggers': False,
    'loggers': {
        __name__: {
            'handlers': ['console'],
            'level': log_level,
            'propagate': True,
        },
    },
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler',
            'level': log_level,
            'formatter': 'simple',
        },
    },
    'formatters': {
        'simple': {
            'datefmt': '%Y-%m-%d %H:%M:%S',
            'format': '(%(threadName)s) %(asctime)s %(levelname)-8s [%(name)s:%(lineno)s] %(message)s',
        },
    },
})
