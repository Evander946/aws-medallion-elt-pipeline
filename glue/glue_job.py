"""AWS Glue entry point. Same transformation modules as the local run; only paths differ.

Job parameters:  --RAW_PATH s3://<bucket>/raw  --LAKE_PATH s3://<bucket>/lake  --INGEST_DATE 2026-11-26
Package `src/pipeline` as a zip and pass it with --extra-py-files. This file has not been run on a
live AWS account; the transformation logic it calls is covered by the local tests.
"""
import sys

from awsglue.utils import getResolvedOptions  # available only inside the Glue runtime

from src.pipeline.run import main

args = getResolvedOptions(sys.argv, ["RAW_PATH", "LAKE_PATH", "INGEST_DATE"])
main(args["RAW_PATH"], args["LAKE_PATH"], args["INGEST_DATE"])
