CREATE TABLE feature_flags (
  tenant_id INT64 NOT NULL,
  hash_id INT64 NOT NULL,
  feature_flags_cfg STRING(MAX) NOT NULL,
) PRIMARY KEY(tenant_id, hash_id)