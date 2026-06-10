# SQL Practice — MySQL 8

A self-contained SQL interview-practice kit: one script builds a realistic
payments / e-commerce database, and an Excel tracker holds **54 graded practice
queries** (with hints, status tracking, and validated reference solutions).

## What's here

| File | Purpose |
|---|---|
| `schema_and_seed_mysql.sql` | Creates the `sql_practice` database: 6 tables + dummy data (MySQL 8 dialect) |
| `SQL_Practice_Tracker.xlsx` | 4 sheets: 📊 Dashboard (live progress), 🗄 Schema reference, 🧩 54 Practice Queries (hints on hover, status dropdown), 🔑 Solutions |

## Setup (DBeaver)

1. Connect DBeaver to your local MySQL.
2. Open `schema_and_seed_mysql.sql` in DBeaver.
3. Execute the whole script: **Alt+X** (Execute SQL Script).
4. Refresh — you'll see the `sql_practice` database with 6 tables.

Or from the command line:

```bash
mysql -u root -p < schema_and_seed_mysql.sql
```

## Schema (one line each)

- **employees** — org chart with `manager_id` self-reference (self-joins, recursive CTEs)
- **customers** — 20 customers across cities/segments
- **orders** — 50 orders, 5 lifecycle statuses, 3 channels
- **order_items** — line items (quantity × unit_price)
- **products** — 15 products in 5 categories
- **payments** — SUCCESS / FAILED / REFUNDED, linked to orders

## Planted edge cases (so anti-joins, NULLs and gaps have real answers)

- Customers **19 & 20** have never ordered
- Product **15** (Cable Organizer) has never been sold
- Orders **5 & 23** are CANCELLED and have **no payment row**
- Product **13** has a **NULL cost**
- Customer **13** ordered in 2023 but never in 2024
- Orders **8 & 17** have a FAILED payment followed by a SUCCESS
- Two RETURNED orders carry a REFUNDED payment

## How to practice

1. Pick a question on **🧩 Practice Queries** (filter by topic/difficulty).
2. Write the query in DBeaver against `sql_practice`.
3. Stuck? Hover the 💡 hint cell.
4. Compare with **🔑 Solutions** (every solution was executed and verified
   against MySQL 8.0).
5. Set Status ✅ — the Dashboard updates itself.

## Topics covered (6 each, 54 total)

SELECT basics · Filtering · Aggregation (GROUP BY/HAVING) · Joins (incl. self-
and anti-joins) · Subqueries (scalar, correlated, EXISTS) · CASE & conditional
aggregation · Window functions (RANK, ROW_NUMBER, LAG, NTILE, running totals)
· CTEs & recursive CTEs · Dates & advanced (top-N per group, % of total, MoM)
