CREATE TABLE `group_4_dataset.enriched_synthetic_deliveroo_plus_dataset2` AS
WITH CustomerOrderHistory AS (
  SELECT
    id_customer_synth,
    order_datetime_synth,
    is_free_delivery,
    ROW_NUMBER() OVER (PARTITION BY id_customer_synth ORDER BY order_datetime_synth) AS order_rank,
    MAX(order_datetime_synth) OVER (PARTITION BY id_customer_synth) AS current_subscription_end_datetime
  FROM assignment_data.synthetic_deliveroo_plus_dataset
)
SELECT
  sod.*,
  CASE
    WHEN sod.order_rank = 1 THEN 1
    ELSE 0
  END AS is_order_made_during_subscription,
  CASE
    WHEN sod.order_rank = 1 THEN sod.order_datetime_synth
    ELSE NULL
  END AS current_subscription_start_datetime
FROM CustomerOrderHistory sod;