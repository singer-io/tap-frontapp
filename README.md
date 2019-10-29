# tap-frontapp

This is a [Singer](https://singer.io) tap that produces JSON-formatted data following the [Singer spec](https://github.com/singer-io/getting-started/blob/master/docs/SPEC.md).

This tap:

- Pulls raw data from FrontApp's [API](https://dev.frontapp.com/)
- Extracts the following resources from FrontApp
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
  - [Analytics](https://dev.frontapp.com/#analytics)
=======
  - [Analytics](https://dev.frontapp.com#analytics)
>>>>>>> cleaned up http.py and added handling for when no data returned.
=======
  - [Analytics](https://dev.frontapp.com#analytics)
>>>>>>> cleaned up http.py and added handling for when no data returned.
=======
  - [Analytics](https://dev.frontapp.com#analytics)
>>>>>>> 1f95e607f623bdeea55b53cc16f1f3f007dda690
      - Hourly/Daily analytics of metrics
          - team_table
  - [Conversations](https://dev.frontapp.com/#list-conversations)
      - List of all conversations by day
          - conversations (enriches the data with recipient contact email)
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
{"bookmarks": {"conversations": {"date_to_resume": "2018-08-01 00:00:00"}}}
```

---
## Replication

With each run of the integration, the following data set is extracted and replicated to the data warehouse:

- **Team Table**: Daily or hourly aggregated team member statistics since the last_update (last completed run of the integration) through the most recent day or hour respectively. On the first run, ALL increments since the **Start Date** will be replicated.

---

## Troubleshooting / Other Important Info

- **Team_table Data**: The first record is for the teammate = "ALL" and so is an aggregated record across all team members.  Also, the API supports pulling specific teams by using a slightly different endpoint, but we have set it up to pull members from all teams.

- **Timestamps**: All timestamp columns and resume_date state parameter are Unix timestamps.


Copyright &copy; 2018 Stitch
