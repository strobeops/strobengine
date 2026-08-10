"""HTTP method examples -- POST, PUT, DELETE, PATCH, HEAD, OPTIONS.

Uses go-httpbin endpoints. Start the test server with:
  podman run -d --rm -p 8080:8080 docker.io/mccutchen/go-httpbin
"""

from strobengine import RequestOptions, StrobEngine
from strobengine.reporter import print_summary

# GET
engine = StrobEngine(
    url="http://localhost:8080/get",
)
summary = engine.run()
print_summary(summary)

# POST with JSON body and auth header
engine = StrobEngine(
    url="http://localhost:8080/post",
    options=RequestOptions(
        method="POST",
        body='{"name": "test", "value": 42}',
        headers=[("Authorization", "Bearer token123")],
    ),
)
summary = engine.run()
print_summary(summary)

# PUT with body
engine = StrobEngine(
    url="http://localhost:8080/put",
    options=RequestOptions(method="PUT", body='{"update": true}'),
)
summary = engine.run()
print_summary(summary)

# DELETE
engine = StrobEngine(
    url="http://localhost:8080/delete",
    options=RequestOptions(method="DELETE"),
)
summary = engine.run()
print_summary(summary)

# PATCH with partial body
engine = StrobEngine(
    url="http://localhost:8080/patch",
    options=RequestOptions(method="PATCH", body='{"patch": 1}'),
)
summary = engine.run()
print_summary(summary)

# HEAD (check endpoint without downloading body)
engine = StrobEngine(
    url="http://localhost:8080/headers",
    options=RequestOptions(method="HEAD"),
)
summary = engine.run()
print_summary(summary)

# OPTIONS (CORS preflight)
engine = StrobEngine(
    url="http://localhost:8080/anything",
    options=RequestOptions(method="OPTIONS"),
)
summary = engine.run()
print_summary(summary)
