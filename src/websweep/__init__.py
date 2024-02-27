from .crawler.crawler import *
from .extractor.extractor import *

__version__ = '1.0.0'
__app_name__ = "websweep"
__status__ = "production"

(
    SUCCESS,
    DIR_ERROR,
    FILE_ERROR,
) = range(3)

ERRORS = {
    DIR_ERROR: "Config directory was not found or could not be created",
    FILE_ERROR: "Config file could not be created",
}
