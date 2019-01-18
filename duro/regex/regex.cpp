#include <regex>

extern "C" {

#include "regex.h"

RDB_bool
	RDB_regex_match(const char *s, const char *pattern) {
	std::regex txt_regex(pattern);
	std::cmatch m;
	return (RDB_bool)std::regex_search(s, txt_regex);
}

}