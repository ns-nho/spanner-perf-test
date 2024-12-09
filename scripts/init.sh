#!/bin/bash

set +e

gcloud config configurations create emulator
gcloud config set auth/disable_credentials true
gcloud config set project perf-project
gcloud config set api_endpoint_overrides/spanner http://google-spanner:9020/
gcloud config configurations activate emulator

gcloud spanner instances create perf-instance --config=emulator-config --description="Performance Testing Instance" --nodes=1
gcloud spanner databases create perf-database --instance=perf-instance --database-dialect=GOOGLE_STANDARD_SQL --ddl-file=scripts/test.ddl
