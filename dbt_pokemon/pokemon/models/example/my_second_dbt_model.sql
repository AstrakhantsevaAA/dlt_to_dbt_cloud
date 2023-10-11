-- Use the `ref` function to select from other models
{{ config(materialized="table") }}

with two_pokemon as (select name from {{ ref('my_first_dbt_model') }} limit 2)
select *
from two_pokemon
