# tap-frontapp

This is a [Singer](https://singer.io) tap that produces JSON-formatted data following the [Singer spec](https://github.com/singer-io/getting-started/blob/master/docs/SPEC.md).

This tap:

- Pulls raw data from FrontApp's [API](https://dev.frontapp.com/)
- Extracts the following resources from FrontApp
  - [Analytics](https://dev.frontapp.com/reference/analytics)
      - Daily analytics of metrics
          - Accounts
          - Channels
          - Inboxes
          - Tags
          - Teammates
          - Teams
- Outputs the schema for each resource

## Setup

Building follows the conventional Singer setup:

python3 ./setup.py clean
python3 ./setup.py build
python3 ./setup.py install

## Configuration

This tap requires a `config.json` which specifies details regarding [API authentication](https://dev.frontapp.com/#authentication) and a cutoff date for syncing historical data. See [example.config.json](example.config.json) for an example.

Create the catalog:

```bash
› tap-frontapp --config config.json --discover > catalog.json
```

In `catalog.json`, add this entry in the `streams[].metadata` array of the streams you want to extract:

```json
{
  "metadata": {
      "selected": true
    },
  "breadcrumb": []
}
```

Then to run the extract:

```bash
› tap-frontapp --config config.json --properties catalog.json --state state.json 
```

Note that a typical state file looks like this:

```json
{"bookmarks": {"team_table": {"date_to_resume": "2018-08-01 00:00:00"}}}
```

---
## Replication

With each run of the integration, the following data set is extracted and replicated to the data warehouse:

- **Accounts Table**: Daily aggregated Accounts statistics since the last_update (last completed run of the integration) through the most recent day respectively.
- **Channels Table**: Daily aggregated Channels since the last_update (last completed run of the integration) through the most recent day respectively.
- **Inboxes Table**: Daily aggregated Inboxes statistics since the last_update (last completed run of the integration) through the most recent day respectively.
- **Tags Table**: Daily aggregated Tags statistics since the last_update (last completed run of the integration) through the most recent day respectively.
- **Teammates Table**: Daily aggregated Teammates statistics since the last_update (last completed run of the integration) through the most recent day respectively.
- **Team Table**: Daily aggregated Team statistics since the last_update (last completed run of the integration) through the most recent day respectively.

---

## Troubleshooting / Other Important Info

- **Timestamps**: All timestamp columns and resume_date state parameter are Unix timestamps.


Copyright &copy; 2018 Stitch
