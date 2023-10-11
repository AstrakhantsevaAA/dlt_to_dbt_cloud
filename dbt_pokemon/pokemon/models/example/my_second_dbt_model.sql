-- Use the `ref` function to select from other models
{{ config(materialized="table") }}

with two_pokemon as (select name from `dlt-dev-external.pokemon_data.pokemon` limit 2)
select *
from two_pokemon
