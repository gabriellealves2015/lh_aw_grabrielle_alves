{% macro generate_not_informed_case(column_list) -%}
    {% for column in column_list %}
            /* Define not informed if null */
            , case
                when {{ column }} is null then '9999999'
                else {{ column }}
            end as {{ column }}
    {% endfor %}
{%- endmacro %}