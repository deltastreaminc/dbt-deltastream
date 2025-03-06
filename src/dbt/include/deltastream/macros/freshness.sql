{% macro deltastream__collect_freshness(source, loaded_at_field, filter) %}
    {{ exceptions.raise_compiler_error(
        """
        dbt-deltastream does not implement source freshness checks.

        If this feature is important to you, please reach out!
        """
    )}}
{% endmacro %}
