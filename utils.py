from re import compile as re_compile, match as re_match, IGNORECASE

URL_REGEX = re_compile(r'^(?:http|ftp)s?://' # http:// or https://
        r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+(?:[A-Z]{2,6}\.?|[A-Z0-9-]{2,}\.?)|' #domain...
        r'localhost|' #localhost...
        r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})' # ...or ip
        r'(?::\d+)?' # optional port
        r'(?:/?|[/?]\S+)$', IGNORECASE)

"""
The validater function will take url and validate it's format using regex
http/https is must

>>> re.match(reg, "https://www.example.com") is not None
True
>>> re.match(reg, "http://code.example.com/feed") is not None
True
>>> re.match(ref, "code.example.com/feed") is not None
False
"""
def is_url_valid(url: str) -> bool:
    return re_match(URL_REGEX, url) is not None