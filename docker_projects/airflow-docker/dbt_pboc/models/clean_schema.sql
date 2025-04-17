{{ config(materialized='ephemeral') }}

{% if execute %}
  {{ process_schema('land') }}
{% endif %}

-- This empty select statement is just to make it a valid model
SELECT 1 LIMIT 0