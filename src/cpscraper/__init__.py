__version__ = '0.1.0'
__app_name__ = "cpscraper"

(
    SUCCESS,
    DIR_ERROR,
    FILE_ERROR,
    JSON_ERROR,
) = range(4)

ERRORS = {
    DIR_ERROR: "config directory error",
    FILE_ERROR: "config file error",
}
