from email.utils import parseaddr
import validators.url

def _validate_email(s):
    return '@' in parseaddr(s)[1]

def _validate_url(s):
    r = validators.url(s)
    return (r == True)