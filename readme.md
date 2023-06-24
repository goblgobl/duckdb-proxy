# duckdb-proxy
Full usage documentation at <https://www.pondz.dev/docs/duckdb-proxy/>.

Simple demo at <https://www.pondz.dev/demo/duckdb-proxy/>.

```bash
$ ./duckdb-proxy
@ts=1687579487928 @l=INFO @ctx=Log.setup level=Info note="alter via --log_level=LEVEL flag"
@ts=1687579487937 @l=INFO @ctx=http.listener address=http://127.0.0.1:8012 db_path=db.duckdb
```

Then POST your `sql` and `params` to the `/api/1/exec` route

```bash
$ curl "http://localhost:8012/api/1/exec" -d '{
  "sql": "select $1::int as over", 
  "params": [9000]
}'

{
 "cols": ["over"],
 "rows": [
   [9000]
 ]
```
