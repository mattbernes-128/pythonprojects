{% macro clean_all_tables(schema_name) %}
    {% set relations = adapter.list_relations(database=target.database, schema=schema_name) %}
    
    {% for relation in relations %}
        {% if relation.is_table or relation.is_view %}
            {{ log("Generating cleanup model for: " ~ relation.identifier, info=True) }}

            -- Create a model for each table dynamically
            {% set model_name = "cleaned_" ~ relation.identifier %}
            
            {% do return("SELECT " ~ clean_columns(relation) ~ " FROM " ~ relation) %}
        {% endif %}
    {% endfor %}
{% endmacro %}