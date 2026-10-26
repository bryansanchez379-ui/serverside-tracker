# Serverside FiveM Tracker

Live statistics for every public FiveM server, shown at **[serversidedev.com/tracker](https://serversidedev.com/tracker/)**.

Every 30 minutes a GitHub Action pulls Cfx.re's public server list and writes two small files:

- `data/summary.json`: players and servers online, the top 100 servers, players by country, game builds, tags, and (measured on the top 300 servers by players) the most-used scripts, framework share and the biggest server owners.
- `data/history.json`: one reading per run, seven days kept.

Run it locally:

```bash
node collector/collect.mjs --top 300 --out data
```

Not affiliated with Cfx.re or Rockstar Games. Built by [Serverside](https://serversidedev.com).
