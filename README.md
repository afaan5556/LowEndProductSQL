# LowEndProductSQL
Working SQL files for increasingly complicated queries - 2018/19

### List of queries
1. Line level query
2. CPI level query
3. Project level query

### Notes
* Casting `AS DOUBLE PRECISION` on various fields to limit rounding errors
* Using `COALESCE` to get single column of data across workday, stargate, and airtable
* Gets line_level, cpi_level, and project_level costs_per_usf along with project_total_usf to make Tableau calcs easier
