# get_spark_session(app_name, config_overrides=None)  # handles Serverless vs cluster
# add_audit_columns(df, run_id)                       # _ingestion_run_id, _ingested_at, _source_file
# broadcast_if_small(df, threshold_mb=10)
# repartition_by_size(df, target_size_mb=128)
# The add_audit_columns is non-negotiable — every Bronze row gets stamped. Centralize it so you never forget.