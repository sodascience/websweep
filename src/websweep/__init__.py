from importlib import import_module
from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("websweep")
except PackageNotFoundError:
    __version__ = "0.0.0"
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


_LAZY_EXPORTS = {
    "Crawler": ("websweep.crawler.crawler", "Crawler"),
    "Extractor": ("websweep.extractor.extractor", "Extractor"),
    "FileExtractor": ("websweep.extractor.extractor", "FileExtractor"),
    "Consolidator": ("websweep.consolidator.consolidator", "Consolidator"),
}


def __getattr__(name):
    """Lazily import top-level exports to keep import time minimal."""
    if name in _LAZY_EXPORTS:
        module_name, attr = _LAZY_EXPORTS[name]
        module = import_module(module_name)
        return getattr(module, attr)
    raise AttributeError(f"module 'websweep' has no attribute '{name}'")


__all__ = [
    "__version__",
    "__app_name__",
    "__status__",
    "SUCCESS",
    "DIR_ERROR",
    "FILE_ERROR",
    "ERRORS",
    *_LAZY_EXPORTS.keys(),
]
