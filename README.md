# dlt-source-google-workspace

[![PyPI version](https://img.shields.io/pypi/v/dlt-source-google-workspace)](https://pypi.org/project/dlt-source-google-workspace/)

[DLT](https://dlthub.com/) source for Google Workspace.

Currently loads the following data:

| Table | Contains |
| -- | -- |
| `users` | All users in the organization |

## Usage

Create a `.dlt/secrets.toml` with your API credentials (`service_account`):

```toml
google_workspace_service_account_info = "{ ... }"
```

and a `.dlt/config.toml` with your admin email address:

```toml
admin_user_email = "some-admin@your-domain.com"
```

and then run the default source with optional list references:

```py
from dlt_source_google_workspace import source as google_workspace_source

pipeline = dlt.pipeline(
   pipeline_name="google_workspace_pipeline",
   destination="duckdb",
   dev_mode=True,
)
google_workspace_data = google_workspace_source()
pipeline.run(google_workspace_data)
```

## Development

This project is using [devenv](https://devenv.sh/).

Commands:

| Command | What does it do? |
| -- | -- |
| `format` | Formats & lints all code |
| `sample-pipeline-run` | Runs the sample pipeline. |
| `sample-pipeline-show` | Starts the streamlit-based dlt hub |
