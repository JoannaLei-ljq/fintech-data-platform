{#- Fails for rows where the column is not NULL and not > 0. -#}
{% test positive_value(model, column_name) %}
select * from {{ model }} where {{ column_name }} is not null and {{ column_name }} <= 0
{% endtest %}
