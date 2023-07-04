__version__ = '0.3.0'
__app_name__ = "websweep"
__status__ = "development"

(
    SUCCESS,
    DIR_ERROR,
    FILE_ERROR,
) = range(3)

ERRORS = {
    DIR_ERROR: "Config directory error",
    FILE_ERROR: "Config file error",
}
