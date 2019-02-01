-- This makes a wide table of workday and airtable data
-- When we join this to dpr if you mult the floor_usf by the per_usf #'s you should get to the spend for that line item, in that cpi, on that floor
-- You should use the ifnull(floor_usf, 0.00001) for you usf multiplier in case there is an active project that has no usf then all the cost will be allocated evenly across the floors (i think) 
-- spot check that when you total everything up it matches the airtable and workday totals 
-- Find the latest revision of the airtable budget
WITH at_revision
AS (SELECT
  id,
  ROW_NUMBER() OVER (PARTITION BY project_code ORDER BY submitted_date DESC) AS rn
FROM playground.airtable_budgets),
-- Find Project total USF
project_total
AS (SELECT
  SUM(COALESCE(floor_usf, 0.00001)) AS project_usf, -- this is basically ifnull()
  project_uuid
FROM stargate_bi_stargate.mv_desk_projections_v2
GROUP BY project_uuid
HAVING SUM(COALESCE(floor_usf, 0.00001)) > 0 -- Must be more than zero bc we can't divide by zero, this would only be if the project wasn't in the dpr (dead)
),

-- pull in airtable budget data
airtable
AS (SELECT
  p.uuid AS project_uuid,
  pt.project_usf AS project_total_usf,
  signed_off_date AS at_sign_date,
  COALESCE(REPLACE(REPLACE(UPPER(REPLACE(b.category, '_', ' ')), 'COSTS', 'COST'), ' ', ' '), 'OTHER') AS at_cpi, -- ADDED CPI TRANSLATOR STUFF HERE 
  LOWER(b.description) AS at_line_item,
  initial_cost / pt.project_usf AS at_budget_line_per_usf, -- get per usf # so when we join to dpr we mult by square feet on that floor/ area 
  currency AS at_currency
FROM playground.airtable_budgets b
LEFT JOIN at_revision
  ON at_revision.id = b.id
LEFT JOIN stargate_bi_stargate.bi_project p -- need this to turn 4 digit code into project_uuid
  ON p.id = b.project_code
LEFT JOIN project_total pt
  ON p.uuid = pt.project_uuid
WHERE at_revision.rn = 1 -- Only keep latest revision
AND pt.project_uuid IS NOT NULL -- only if there is a project with more than 0 square feet
),

-- pull in workday financial data 
workday
AS (SELECT
  p.uuid AS project_uuid,
  pt.project_usf AS project_total_usf,
  LOWER(purchase_item) AS jcr_line_item,
  cpi AS jcr_cpi,
  -- Actuals and projected jcr columns added
  total_paid_amount_paid_expense_report_credit_card_intercompany_ / pt.project_usf AS jcr_actuals_per_usf,
  total_projected_spend_po_supplier_contract_ / pt.project_usf AS jcr_projected_per_usf,
  invoiced_amount / pt.project_usf AS jcr_invoiced_amount_per_usf,
  expense_report / pt.project_usf AS jcr_expense_report_per_usf,
  current_budget / pt.project_usf AS jcr_budget_line_per_usf
FROM workday_development.mv_workday_job_cost_report jcr
LEFT JOIN stargate_bi_stargate.bi_project p
  ON p.id = jcr.project_id
LEFT JOIN project_total pt
  ON p.uuid = pt.project_uuid
WHERE pt.project_uuid IS NOT NULL
),

-- Find latest revision of stargate data
sg_revision
AS (SELECT
  *,
  ROW_NUMBER() OVER (PARTITION BY b.budget_uuid ORDER BY b.created_date DESC) AS rn
FROM stargate_modules_budget_production.revision b
),

-- Get stargate revision detail data
sg_revision_detail
AS (SELECT
  rd.revision_uuid,
  MAX(rd.currency) AS currency,
  SUM(rd.final_cost) AS budget,
  COALESCE(REPLACE(REPLACE(UPPER(REPLACE(rd.category, '_', ' ')), 'COSTS', 'COST'), '  ', ' '), 'OTHER') AS cpi_categories,
  LOWER(rd.cost_description) AS cost_description,
  CASE
    WHEN SUM(rd.final_cost) != SUM(rd.initial_cost) THEN SUM(rd.initial_cost ::NUMERIC(30, 20)) / SUM(rd.final_cost)
  END AS rate
FROM stargate_modules_budget_production.revision_detail rd
WHERE rd._fivetran_deleted = FALSE AND NOT cpi_categories ~ '^[0-9\.]+$' AND NOT cpi_categories ~ '#sV/0!'
GROUP BY rd.revision_uuid,
         rd.cost_description,
         rd.category
HAVING SUM(rd.final_cost) > 0
),

-- pull in stargate budget data
stargate
AS (SELECT
  b.project_uuid,
  pt.project_usf AS project_total_usf,
  sg_revision_detail.cpi_categories AS sg_cpi,
  sg_revision_detail.cost_description AS sg_line_item,
  (CASE
    WHEN sg_revision_detail.rate IS NULL AND
      r.rate IS NULL THEN 1
    WHEN sg_revision_detail.rate IS NULL THEN r.rate
    ELSE sg_revision_detail.rate
  END) AS sg_exchange_rate,
  date_trunc('day', sg_revision.created_date) AS sg_budget_date,
  /*Apply rate to budget*/
  sg_revision_detail.budget * (CASE
    WHEN sg_revision_detail.rate IS NULL AND
      r.rate IS NULL THEN 1
    WHEN sg_revision_detail.rate IS NULL THEN r.rate
    ELSE sg_revision_detail.rate
  END) / pt.project_usf AS sg_cpi_budget_per_usf,
  sg_revision_detail.currency AS sg_currency
FROM stargate_modules_budget_production.budget b
LEFT JOIN sg_revision
  ON b.uuid = sg_revision.budget_uuid
LEFT JOIN stargate_modules_budget_production.budget_iteration i
  ON sg_revision.budget_iteration_uuid = i.uuid
LEFT JOIN sg_revision_detail
  ON sg_revision.uuid = sg_revision_detail.revision_uuid
LEFT JOIN dw.mv_exchange_rates r
  ON date_trunc('day', sg_revision.created_date) = r.date
  AND r.from_currency = RIGHT(sg_revision_detail.currency, 3)
LEFT JOIN project_total pt
  ON b.project_uuid = pt.project_uuid
WHERE sg_revision.rn = 1
AND sg_cpi_budget_per_usf IS NOT NULL
AND sg_cpi_budget_per_usf != 0
AND sg_revision._fivetran_deleted = FALSE
AND pt.project_uuid IS NOT NULL -- only if there is a project with more than 0 square feet
),

-- Final query as a detail to pull project roll up from
detail AS (SELECT
  COALESCE(airtable.project_uuid, COALESCE(workday.project_uuid, stargate.project_uuid)) AS project_uuid, -- We won't get 100% matches so this makes sure this columns is populated all the way down
  COALESCE(airtable.project_total_usf, COALESCE(workday.project_total_usf, stargate.project_total_usf)) AS project_total_usf,
  LOWER(COALESCE(airtable.at_cpi, COALESCE(workday.jcr_cpi, stargate.sg_cpi))) AS cpi, -- same here 
  COALESCE(airtable.at_line_item, COALESCE(workday.jcr_line_item, stargate.sg_line_item)) AS line_item, -- and here
  airtable.at_sign_date,
  MAX(airtable.at_budget_line_per_usf) AS at_budget_line_per_usf,
  airtable.at_currency,
  stargate.sg_budget_date,
  MAX(stargate.sg_cpi_budget_per_usf) AS sg_cpi_budget_per_usf,
  stargate.sg_currency,
  MAX(workday.jcr_invoiced_amount_per_usf) AS jcr_invoiced_amount_per_usf,
  MAX(workday.jcr_expense_report_per_usf) AS jcr_expense_report_per_usf,
  MAX(workday.jcr_budget_line_per_usf) AS jcr_budget_line_per_usf,
  MAX(workday.jcr_actuals_per_usf) AS jcr_actuals_per_usf,
  MAX(workday.jcr_projected_per_usf) AS jcr_projected_per_usf
FROM airtable
FULL JOIN workday
  ON airtable.project_uuid = workday.project_uuid
  AND airtable.at_cpi = workday.jcr_cpi
  AND airtable.at_line_item = workday.jcr_line_item
FULL JOIN stargate
  ON airtable.project_uuid = stargate.project_uuid
  AND airtable.at_cpi = stargate.sg_cpi
  AND airtable.at_line_item = stargate.sg_line_item
GROUP BY 1,
         2,
         3,
         4,
         5,
         7,
         8,
         10
ORDER BY 1, 2, 3
)

SELECT
  project_uuid,
  
  MAX(at_sign_date) AS at_sign_date,
  SUM(at_budget_line_per_usf) AS at_budget_line_per_usf_total,
  MAX(at_currency) AS at_currency,

  MAX(sg_budget_date) AS sg_budget_date,
  SUM(sg_cpi_budget_per_usf) AS sg_cpi_budget_per_usf_total,
  MAX(sg_currency) AS sg_currency,

  SUM(jcr_invoiced_amount_per_usf) AS jcr_invoiced_amount_per_usf_total,
  SUM(jcr_expense_report_per_usf) AS jcr_expense_report_per_usf_total,
  SUM(jcr_budget_line_per_usf) AS jcr_budget_line_per_usf_total,
  SUM(jcr_actuals_per_usf) AS jcr_actuals_per_usf_total,
  SUM(jcr_projected_per_usf) AS jcr_projected_per_usf_total
FROM detail
GROUP BY project_uuid