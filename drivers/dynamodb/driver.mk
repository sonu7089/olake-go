# dynamodb fragment for the root Makefile (see the driver-fragment contract there).

# DynamoDB Streams' 24h retention cannot guarantee gap-free CDC, so no 2PC suites.
NON_CDC_DRIVERS += dynamodb

# DynamoDB Local answers a bare GET with 400 (MissingAuthenticationToken); both
# 200 and 400 prove the service is up.
PROBE.dynamodb = curl -s --max-time 5 -o /dev/null -w '%{http_code}' http://localhost:8000/ | grep -qE '^(200|400)'