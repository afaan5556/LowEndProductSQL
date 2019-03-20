-- ######## PROJECT LEVEL ######## --
-- This makes a wide table of workday and airtable data
-- When we join this to dpr if you mult the floor_usf by the per_usf #'s you should get to the spend for that line item, in that cpi, on that floor
-- You should use the ifnull(floor_usf, 0.00001) for you usf multiplier in case there is an active project that has no usf then all the cost will be allocated evenly across the floors (i think) 
-- spot check that when you total everything up it matches the airtable and workday totals

-- Find the latest revision of the airtable budget
WITH at_revision
AS (SELECT
  id,
  ROW_NUMBER() OVER (PARTITION BY project_code ORDER BY submitted_date DESC) AS rn
FROM workday_development.airtable_budgets),

-- Find Project total USF
project_total
AS (SELECT
  SUM(COALESCE(floor_usf, 0.00001)) AS project_usf, -- this is basically ifnull()
  SUM(COALESCE(floor_desks, 0.01)) AS project_desks, -- this is basically ifnull()
  project_uuid
FROM redtech_dw.mv_desk_projections_v2
GROUP BY project_uuid
HAVING SUM(COALESCE(floor_usf, 0.00001)) > 0 -- Must be more than zero bc we can't divide by zero, this would only be if the project wasn't in the dpr (dead)
),

-- pull in airtable budget data
airtable
AS (SELECT
  p.uuid AS project_uuid,
  pt.project_usf AS project_total_usf,
  pt.project_desks AS project_total_desks,
  signed_off_date AS at_sign_date,
  CAST(initial_cost AS double precision) / pt.project_usf AS at_budget_line_per_usf, -- get per usf # so when we join to dpr we mult by square feet on that floor/ area
  CAST(initial_cost AS double precision) / CASE
    WHEN pt.project_desks = 0 THEN 1
    ELSE pt.project_desks
  END AS at_budget_line_per_desk, -- get per desk # so when we join to dpr we mult by desks on that floor/ area
  currency AS at_currency
FROM workday_development.airtable_budgets b
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
  pt.project_desks AS project_total_desks,
  -- Actuals and projected jcr columns added
  CAST(total_paid_amount_paid_expense_report_credit_card_intercompany_ AS double precision) / pt.project_usf AS jcr_actuals_per_usf,
  CAST(total_projected_spend_po_supplier_contract_ AS double precision) / pt.project_usf AS jcr_projected_per_usf,
  CAST(invoiced_amount AS double precision) / pt.project_usf AS jcr_invoiced_amount_per_usf,
  CAST(expense_report AS double precision) / pt.project_usf AS jcr_expense_report_per_usf,
  CAST(current_budget AS double precision) / pt.project_usf AS jcr_budget_line_per_usf,
  CAST(total_paid_amount_paid_expense_report_credit_card_intercompany_ AS double precision) / CASE
    WHEN pt.project_desks = 0 THEN 1
    ELSE pt.project_desks
  END AS jcr_actuals_per_desk,
  CAST(total_projected_spend_po_supplier_contract_ AS double precision) / CASE
    WHEN pt.project_desks = 0 THEN 1
    ELSE pt.project_desks
  END AS jcr_projected_per_desk,
  CAST(invoiced_amount AS double precision) / CASE
    WHEN pt.project_desks = 0 THEN 1
    ELSE pt.project_desks
  END AS jcr_invoiced_amount_per_desk,
  CAST(expense_report AS double precision) / CASE
    WHEN pt.project_desks = 0 THEN 1
    ELSE pt.project_desks
  END AS jcr_expense_report_per_desk,
  CAST(current_budget AS double precision) / CASE
    WHEN pt.project_desks = 0 THEN 1
    ELSE pt.project_desks
  END AS jcr_budget_line_per_desk

FROM workday_development.mv_workday_job_cost_report jcr
LEFT JOIN stargate_bi_stargate.bi_project p
  ON p.id = jcr.project_id
LEFT JOIN project_total pt
  ON p.uuid = pt.project_uuid
WHERE pt.project_uuid IS NOT NULL),

-- Find latest revision of stargate data
sg_revision
AS (SELECT
  *,
  ROW_NUMBER() OVER (PARTITION BY b.budget_uuid ORDER BY b.created_date DESC) AS rn
FROM stargate_modules_budget_production.revision b),

-- Get stargate revision detail data
sg_revision_detail
AS (SELECT
  rd.revision_uuid,
  MAX(rd.currency) AS currency,
  SUM(CAST(rd.final_cost AS double precision)) AS budget,
  CASE
    WHEN SUM(CAST(rd.final_cost AS double precision)) != SUM(CAST(rd.initial_cost AS double precision)) THEN CAST(SUM(CAST(rd.initial_cost AS double precision)) / SUM(CAST(rd.final_cost AS double precision)) AS double precision)
  END AS rate
FROM stargate_modules_budget_production.revision_detail rd
WHERE rd._fivetran_deleted = FALSE
GROUP BY rd.revision_uuid
HAVING SUM(rd.final_cost) > 0),

-- pull in stargate budget data
stargate
AS (SELECT
  b.project_uuid,
  pt.project_usf AS project_total_usf,
  pt.project_desks AS project_total_desks,
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
  END) / pt.project_usf AS sg_budget_per_usf,
  sg_revision_detail.budget * (CASE
    WHEN sg_revision_detail.rate IS NULL AND
      r.rate IS NULL THEN 1
    WHEN sg_revision_detail.rate IS NULL THEN r.rate
    ELSE sg_revision_detail.rate
  END) / CASE
    WHEN pt.project_desks = 0 THEN 1
    ELSE pt.project_desks
  END AS sg_budget_per_desk,

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
AND sg_budget_per_usf IS NOT NULL
AND sg_budget_per_usf != 0
AND sg_revision._fivetran_deleted = FALSE
AND pt.project_uuid IS NOT NULL -- only if there is a project with more than 0 square feet
),

-- Final query as a detail to pull project roll up from
detail
AS (SELECT
  COALESCE(airtable.project_uuid, COALESCE(workday.project_uuid, stargate.project_uuid)) AS project_uuid, -- We won't get 100% matches so this makes sure this columns is populated all the way down
  COALESCE(airtable.project_total_usf, COALESCE(workday.project_total_usf, stargate.project_total_usf)) AS project_total_usf,
  COALESCE(airtable.project_total_desks, COALESCE(workday.project_total_desks, stargate.project_total_desks)) AS project_total_desks,
  airtable.at_sign_date,
  MAX(airtable.at_budget_line_per_usf) AS at_budget_line_per_usf,
  MAX(airtable.at_budget_line_per_desk) AS at_budget_line_per_desk,
  airtable.at_currency,
  stargate.sg_budget_date,
  MAX(stargate.sg_budget_per_usf) AS sg_budget_per_usf,
  MAX(stargate.sg_budget_per_desk) AS sg_budget_per_desk,
  stargate.sg_currency,
  MAX(workday.jcr_invoiced_amount_per_usf) AS jcr_invoiced_amount_per_usf,
  MAX(workday.jcr_expense_report_per_usf) AS jcr_expense_report_per_usf,
  MAX(workday.jcr_budget_line_per_usf) AS jcr_budget_line_per_usf,
  MAX(workday.jcr_actuals_per_usf) AS jcr_actuals_per_usf,
  MAX(workday.jcr_projected_per_usf) AS jcr_projected_per_usf,
  MAX(workday.jcr_invoiced_amount_per_desk) AS jcr_invoiced_amount_per_desk,
  MAX(workday.jcr_expense_report_per_desk) AS jcr_expense_report_per_desk,
  MAX(workday.jcr_budget_line_per_desk) AS jcr_budget_line_per_desk,
  MAX(workday.jcr_actuals_per_desk) AS jcr_actuals_per_desk,
  MAX(workday.jcr_projected_per_desk) AS jcr_projected_per_desk
FROM airtable
FULL JOIN workday
  ON airtable.project_uuid = workday.project_uuid
FULL JOIN stargate
  ON airtable.project_uuid = stargate.project_uuid
GROUP BY 1,
         2,
         3,
         4,
         7,
         8,
         11
ORDER BY 1, 2, 3)

SELECT
  project_uuid,

  MAX(at_sign_date) AS at_sign_date,
  SUM(at_budget_line_per_usf) AS at_budget_line_per_usf_total,
  SUM(at_budget_line_per_desk) AS at_budget_line_per_desk_total,
  MAX(at_currency) AS at_currency,

  MAX(project_total_usf) AS project_total_usf,
  MAX(project_total_desks) AS project_total_desks,

  MAX(sg_budget_date) AS sg_budget_date,
  SUM(sg_budget_per_usf) AS sg_budget_per_usf_total,
  SUM(sg_budget_per_desk) AS sg_budget_per_desk_total,
  MAX(sg_currency) AS sg_currency,

  SUM(jcr_invoiced_amount_per_usf) AS jcr_invoiced_amount_per_usf_total,
  SUM(jcr_expense_report_per_usf) AS jcr_expense_report_per_usf_total,
  SUM(jcr_budget_line_per_usf) AS jcr_budget_line_per_usf_total,
  SUM(jcr_actuals_per_usf) AS jcr_actuals_per_usf_total,
  SUM(jcr_projected_per_usf) AS jcr_projected_per_usf_total,
  SUM(jcr_invoiced_amount_per_desk) AS jcr_invoiced_amount_per_desk_total,
  SUM(jcr_expense_report_per_desk) AS jcr_expense_report_per_desk_total,
  SUM(jcr_budget_line_per_desk) AS jcr_budget_line_per_desk_total,
  SUM(jcr_actuals_per_desk) AS jcr_actuals_per_desk_total,
  SUM(jcr_projected_per_desk) AS jcr_projected_per_desk_total
FROM detail

GROUP BY project_uuid