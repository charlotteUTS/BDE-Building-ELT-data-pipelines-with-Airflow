
      update "postgres"."at3"."suburb_snapshot"
    set dbt_valid_to = DBT_INTERNAL_SOURCE.dbt_valid_to
    from "suburb_snapshot__dbt_tmp164908479894" as DBT_INTERNAL_SOURCE
    where DBT_INTERNAL_SOURCE.dbt_scd_id::text = "postgres"."at3"."suburb_snapshot".dbt_scd_id::text
      and DBT_INTERNAL_SOURCE.dbt_change_type::text in ('update'::text, 'delete'::text)
      and "postgres"."at3"."suburb_snapshot".dbt_valid_to is null;

    insert into "postgres"."at3"."suburb_snapshot" ("lga_name", "suburb_name", "id", "dbt_updated_at", "dbt_valid_from", "dbt_valid_to", "dbt_scd_id")
    select DBT_INTERNAL_SOURCE."lga_name",DBT_INTERNAL_SOURCE."suburb_name",DBT_INTERNAL_SOURCE."id",DBT_INTERNAL_SOURCE."dbt_updated_at",DBT_INTERNAL_SOURCE."dbt_valid_from",DBT_INTERNAL_SOURCE."dbt_valid_to",DBT_INTERNAL_SOURCE."dbt_scd_id"
    from "suburb_snapshot__dbt_tmp164908479894" as DBT_INTERNAL_SOURCE
    where DBT_INTERNAL_SOURCE.dbt_change_type::text = 'insert'::text;

  