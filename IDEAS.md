* backed by SQLite, with a stable schema so it can be read from anywhere
* should have
  * stats (n, sum, pXX, avg, min, max)
  * aggregation periods / frequency (1s/10s?, 1min, 5min, hour, day?)
  * data retention per aggregation period
  * statsd / Graphite client compatibility (limited), so it can be used with existing clients
* possible architecture
  * one db for incoming metrics, one db for long term storage (to allow for many writers and many readers)
  * ingestor daemon batches metrics and adds them to the incoming metrics db
  * aggregator daemon reads from incoming metrics db and writes to long term db
  * daemons can be the same process or not
  * prototype in Python, rewrite in Rust for speed if required
  * have an easy way to get graphs in a browser (pandas/dask/bokeh)
  * no metric math initially, can be compiled to SQL if needed
