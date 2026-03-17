#pragma once

#include <base/types.h>
#include <string_view>

namespace DB
{

/// Transforms the [I]LIKE expression into regexp re2. For example, abc%def -> ^abc.*def$
String likePatternToRegexp(std::string_view pattern);

/// Rewrites a LIKE pattern that uses a custom escape character into one that uses the standard backslash escape.
/// For example, with escape_char='#': "50#%off" -> "50\%off"
String rewriteLikePatternWithCustomEscape(std::string_view pattern, char escape_char);

}
