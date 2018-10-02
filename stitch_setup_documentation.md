# FrontApp

This tap is for pulling [Analytics](https://dev.frontapp.com/#analytics) data from the FrontApp API. Its current developed scope is limited to the teams table, but it is easily expandable to the other Analytics data sets.

## Connecting FrontApp

### FrontApp Setup Requirements

To set up FrontApp in Stitch, you need to get your JSON web token directly from Front (go to > Plugins & API > API).

### Setup FrontApp as a Stitch source

1. [Sign into your Stitch account](https://app.stitchdata.com/)

2. On the Stitch Dashboard page, click the **Add Integration** button.

3. Click the **FrontApp** icon.

4. Enter a name for the integration. This is the name that will display on the Stitch Dashboard for the integration; it’ll also be used to create the schema in your destination. For example, the name "Stitch FrontApp" would create a schema called `stitch_frontapp` in the destination. **Note**: Schema names cannot be changed after you save the integration.

5. In the **Token** field, enter your FrontApp web token.

6. In the **Metric** field, enter the Analytics metric needed.  The only schema supported in this tap right now is the team_table metric.

7. In the **Incremental Range** field, enter the desired aggregation frame (daily or hourly).

8. In the **Start Date** field, enter the minimum, beginning start date for FrontApp Analytics (e.g. 2017-01-1).

---

## FrontApp Replication

With each run of the integration, the following data set is extracted and replicated to the data warehouse:

- **Team Table**: Daily or hourly aggregated team member statistics since the last_update (last completed run of the integration) through the most recent day or hour respectively. On the first run, ALL increments since the **Start Date** will be replicated.

---

## FrontApp Table Schemas

### team_table

- Table name: team_table 
- Description: A list of team members and their event statistics during the course of the day/hour starting from the analytics_date.
- Primary key: analytics_date, analytics_range, teammate_id
- Replicated incrementally
- Bookmark column: analytics_date (written as resume_date in the state records)
- API endpoint documentation: [Analytics](https://dev.frontapp.com/#analytics)

---

## Troubleshooting / Other Important Info

- **Team_table Data**: The first record is for the teammate = "ALL" and so is an aggregated record across all team members.  Also, the API supports pulling specific teams by using a slightly different endpoint, but we have set it up to pull members from all teams.

- **Timestamps**: All timestamp columns and resume_date state parameter are Unix timestamps.

