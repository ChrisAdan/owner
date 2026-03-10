{% docs surrogate_key %}
Surrogate primary key generated via `dbt_utils.generate_surrogate_key()`. Deterministic
md5 hash of the natural key column(s). Used as the stable join key for downstream models.
{% enddocs %}
