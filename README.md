# Postgres Connector App

## Objective
- To receive API requests from a dashboard, connect to a PostgreSQL server to get summary data, and finally return the data to the dashboard

## Requirements
- High concurrency
- Low latency

## Infrastructure Stack
- A VM to run Postgres, instead of an managed SQL instance. This will ensure lower vendor lock-in and allow more customizability (though at the cost of perhaps more maintenance overhead)
- Serverless functions to connect to postgres – ensures high concurrency and relatively low latency without putting additional load on the Postgres server

## Stack
- Python (though honestly can use practically anything). High speed of development. Difficulty in concurrency won't matter since this is a serverless function. Slightly longer processing times won't matter, since most of the actual compute is done on the Postgres server

## Admin
- [done] Enable access to port 5432 in firewall
- [done] On your postgres server, `/etc/postgresql/12/main/postgresql.conf`, change `listen_addresses` from `localhost` to your IP
