### Serilog.Sinks.CouchDB

Forked from https://github.com/serilog/serilog-sinks-couchdb

A Serilog sink that writes events to Apache [CouchDB](http://couchdb.org).

**Platforms** - NetStandard 1.4

You'll need to create a database on your CouchDB server. In the example shown, it is called `log`.

```csharp
var log = new LoggerConfiguration()
    .WriteTo.CouchDB("http://mycouchdb/log/")
    .CreateLogger();
```
