

def find_string_with_context(regex, string, context_length):
    """
    The idea is to avoid context matching using pure Python regex, staying away from something
    like: pattern = ".{0,30}%s.{0,30}". This is very slow in Python re implementation
    
    Instead we search for the EXACT string using regex, then do manual context 
    retrieval using string indexes.
    
    The result? About 2 times faster than a pure regex solution when looking for 13000 strings
    through 18 html pages (with markups stripped)
    
    Arguments:
    regex -- a regex object with exact string (no context) matching pattern already compiled in
    string -- the string to search for
    context_length -- how much context around the matched string we would like to extract
    
    return -- the matched string with context or None if nothing is matched
    """
    m = regex.search(string)
    if m:
        # Get index of string in html text line 
        (start, end) = m.span()
        # Calculate start & end indexes for including context
        start = start - context_length if start > context_length else 0
        end = end + context_length if len(string) - context_length > end else len(string)
        
        return string[start:end]
    else:
        return None