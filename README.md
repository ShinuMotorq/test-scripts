# TEST_SCRIPTS

## Services added

* Snowflake setup : Sets up basic tables to sync feed from release testing replays.
* Snowflake sync : Syncs feed data from Blob storage to Snowflakes.
* Regression prep : Prepares regression and prod tables for AB regression.
* Post deployment validation : Runs primary validations for pilot/prod environment post deployment

## Services to be added

* Resource creation for immediate setup of resource for replays
* Create templates for replays. Each replay should take less than 15 mins to setup
* Monitor replays using replay-monitor to determine when to stop a replay