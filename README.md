# Missing folders dashboard

service to get all mri scan in the storage server for display in a grafana dashboard

# how to run

## docker

**Start up**
```sh
docker compose up --build -d
```

**Shut Down**
```sh
docker compose down
```

**get logs**

```sh
docker logs dashboard_project-api-1
```

then navigate to http://127.0.0.1:3000 in the browser.
the admin password is `admin`.
Go to `Dashboards` > `missing folders`


## Tests

```bash
pip install -r requirements-dev.txt
pytest tests/
```

The SMB share and the Google Sheet are always faked. Postgres is real: a temporary `postgres:17`
container is started with docker, or set `TEST_DATABASE_URL` to use an existing **throwaway**
database (tables are dropped before every test).
