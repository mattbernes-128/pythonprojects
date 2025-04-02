{% macro trim_whitespace(relation) %}
  {% set relation_name = relation.identifier %}
  {% set new_relation_name = relation_name | trim %}

  {% if relation_name != new_relation_name %}
    {% set rename_relation_sql %}
      alter table {{ relation }} rename to {{ new_relation_name }};
    {% endset %}
    {{ log("Renaming table " ~ relation_name ~ " to " ~ new_relation_name, info=True) }}
    {% do run_query(rename_relation_sql) %}
  {% endif %}

  {% set columns = adapter.get_columns_in_relation(relation) %}
  {% for column in columns %}
    {% set column_name = column.name %}
    {% set new_column_name = column_name | trim %}
    {% if column_name != new_column_name %}
      {% set rename_column_sql %}
        alter table {{ relation }} rename column {{ column_name }} to {{ new_column_name }};
      {% endset %}
      {{ log("Renaming column " ~ column_name ~ " in table " ~ relation.identifier ~ " to " ~ new_column_name, info=True) }}
      {% do run_query(rename_column_sql) %}
    {% endif %}
  {% endfor %}
{% endmacro %}

{% macro process_schema(schema_name) %}
  {% set relations = adapter.list_relations(database=target.database, schema=schema_name) %}
  {% for relation in relations %}
    {% do trim_whitespace(relation) %}
  {% endfor %}
{% endmacro %}

{% set schema_to_process = 'land' %}

{% if execute %}
  {{ process_schema(schema_to_process) }}
{% endif %}