# tap-frontapp

This is a [Singer](https://singer.io) tap that produces JSON-formatted data following the [Singer spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

This tap:

- Pulls raw data from FrontApp's [API](https://dev.frontapp.com/)
- Extracts the following resources from FrontApp
  - [Analytics](https://dev.frontapp.com#analytics)
      - Hourly/Daily analytics of metrics
          - team_table
- Outputs the schema for each resource

## Setup

Building follows the conventional Singer setup:

python3 ./setup.py clean
python3 ./setup.py build
python3 ./setup.py install

## Configuration

This tap requires a `config.json` which specifies details regarding [API authentication](https://dev.frontapp.com/#authentication) by using a token, a cutoff date for syncing historical data (date format of YYYY-MM-DDTHH:MI:SSZ), and a time period range [daily,hourly] to control what incremental extract date ranges are. See [example.config.json](example.config.json) for an example.

Create the catalog:

```bash
› tap-frontapp --config config.json --discover > catalog.json
```

Then to run the extract:

```bash
› tap-frontapp --config config.json --catalog catalog.json --state state.json 
```

Note that a typical state file looks like this:

```json
{"bookmarks": {"team_table": {"date_to_resume": "2018-08-01 00:00:00"}}}
```

---

Copyright &copy; 2018 Stitch
