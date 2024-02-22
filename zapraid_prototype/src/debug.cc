#include "debug.h"

#include <sys/time.h>

std::string getTime()
{
    struct timeval tv;
    struct tm* ptm;
    long milliseconds;
    char time_string[60];

    gettimeofday(&tv, NULL);
    ptm = localtime(&tv.tv_sec);
    strftime(time_string, sizeof(time_string), "%H:%M:%S", ptm);
    milliseconds = tv.tv_usec / 1000;
    sprintf(time_string, "%s.%03ld", time_string, milliseconds);
    return std::string(time_string);
}
