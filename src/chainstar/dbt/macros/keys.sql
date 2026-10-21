{#
  Readable string keys such as 'ethereum:0xabc...'. Prefixing the chain lets
  Ethereum and Base rows share one table without key collisions, and keeps each
  key a single column so dbt relationship tests can check it.
#}
{% macro chain_key(chain_column) -%}
    {{ chain_column }}{% for part in varargs %} || ':' || cast({{ part }} as varchar){% endfor %}
{%- endmacro %}
