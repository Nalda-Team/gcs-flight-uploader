import ssl
from requests.adapters import HTTPAdapter
class SSLAdapter(HTTPAdapter):
    def init_poolmanager(self, *args, **kwargs):
        context = ssl.create_default_context()
        context.options |= ssl.OP_NO_TLSv1 | ssl.OP_NO_TLSv1_1  # TLS 1.0과 1.1 비활성화
        kwargs['ssl_context'] = context
        super(SSLAdapter, self).init_poolmanager(*args, **kwargs)