#include <regex>

extern "C" {

#include "regex.h"

int
RDB_regex_match(const char *s, const char *pattern, RDB_bool *result, RDB_exec_context *ecp)
{
    try {   
        std::regex txt_regex(pattern);
        *result = (RDB_bool)std::regex_search(s, txt_regex);
    } catch (const std::exception &err) {
        RDB_raise_invalid_argument(err.what(), ecp);
        return RDB_ERROR;
    }
    return RDB_OK;
}

}
