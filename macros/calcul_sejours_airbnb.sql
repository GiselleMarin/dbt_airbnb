{% macro sejours_estimes(col_reviews, taux_review) %}
    round({{ col_reviews }} / {{ taux_review }}, 0)
{% endmacro %}

{% macro pct_airbnb(col_sejours, col_touristes) %}
    round({{ col_sejours }} * 100.0 / nullif({{ col_touristes }}, 0), 2)
{% endmacro %}
