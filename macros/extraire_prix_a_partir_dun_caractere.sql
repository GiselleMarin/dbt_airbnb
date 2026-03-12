{% macro extraire_prix_a_partir_dun_caractere(price, symbol) -%}
--{% set symbole_devise = symbol %}
  --  {{ log('execution macro extraire_prix avec symbole devise ' ~ symbole_devise, info=True)}}
    try_cast(
        CASE 
            WHEN STARTSWITH({{ price }}, '{{ symbol }}') THEN SPLIT_PART({{ price }}, '{{ symbol }}', 2)
            WHEN ENDSWITH({{ price }}, '{{ symbol }}') THEN SPLIT_PART({{ price }}, '{{ symbol }}', 1)
            ELSE NULL
        END
    AS FLOAT)
{% endmacro %}