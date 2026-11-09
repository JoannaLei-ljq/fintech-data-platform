{#- Fails for every combination of the given columns that appears more than once. -#}
{% test unique_combination(model, combination) %}
select {{ combination | join(', ') }}, count(*) as occurrences
from {{ model }}
group by {{ combination | join(', ') }}
having count(*) > 1
{% endtest %}
