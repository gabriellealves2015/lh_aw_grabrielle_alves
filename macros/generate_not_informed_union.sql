{%- macro generate_not_informed_union(all_column_list, only_ids_list, only_null_list) -%}
    union all
        select
    {%- for column in all_column_list -%}
        {%- set prefix = '' -%}
        {%- if not loop.first -%}
            {%- set prefix = ',' -%}
        {%- endif -%}

        {%- set value = "'Not informed'" -%}
        {%- if column in only_ids_list -%}
            {%- set value = "'9999999'" -%}
        {%- endif -%}
        
        {%- if column in only_null_list -%}
            {%- set value = 'null' -%}
        {%- endif -%}
            
        {{ prefix }} {{ value }} as {{ column }}
            
    {%- endfor -%}
{%- endmacro -%}