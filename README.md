# Pharma Compliance Spend Analytics

> End-to-end compliance spend analytics on real CMS Open Payments data: anomaly detection, tiered risk classifier, and an interactive Tableau dashboard surfacing the relationships that compliance teams actually need to investigate.

[![Python](https://img.shields.io/badge/Python-3.10%2B-3776AB.svg?logo=python&logoColor=white)](https://www.python.org)
[![Pandas](https://img.shields.io/badge/Pandas-2.0%2B-150458.svg?logo=pandas&logoColor=white)](https://pandas.pydata.org)
[![Scikit-learn](https://img.shields.io/badge/Scikit--learn-1.4%2B-F7931E.svg?logo=scikit-learn&logoColor=white)](https://scikit-learn.org)
[![PySpark](https://img.shields.io/badge/PySpark-4.1%2B-E25A1C.svg?logo=apachespark&logoColor=white)](https://spark.apache.org/docs/latest/api/python/)
[![Snowflake](https://img.shields.io/badge/Snowflake-Cloud-29B5E8.svg?logo=snowflake&logoColor=white)](https://www.snowflake.com)
![Tableau](https://img.shields.io/badge/Tableau-Packaged_Workbook-E97627.svg?logo=tableau&logoColor=white)
[![License](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)


<p align="center">
  <strong>CMS Open Payments → HCP feature store → Rules + ML → Explainable triage → Tableau</strong>
</p>

<p align="center">
  <a href="#why-this-project">Context</a> ·
  <a href="#architecture">Architecture</a> ·
  <a href="#tableau-dashboard">Dashboard</a> ·
  <a href="#results">Results</a> ·
  <a href="#how-to-run">Run locally</a> ·
  <a href="#snowflake-implementation">Snowflake</a> ·
  <a href="#pyspark-implementation">PySpark</a>
</p>

## Project at a Glance

This repository is a compact, end-to-end reference implementation for compliance analytics on CMS Open Payments data. It is intentionally designed as a portfolio-quality system rather than a single notebook: the same analytical intent is expressed through an exploratory Pandas workflow, a Snowflake SQL workload, and a distributed PySpark implementation.

| Capability | Implementation | Output |
|---|---|---|
| Transaction ingestion | Chunked CSV read, physician filtering, deterministic 10% sample | Cleaned payment-level records |
| HCP feature engineering | NPI-level aggregation across value, frequency, geography, and concentration | One analytical row per HCP |
| Explainable detection | Specialty-relative z-scores, global IQR fences, and a domain concentration rule | `HIGH`, `MEDIUM`, `LOW`, `NONE` tiers |
| Multivariate detection | Isolation Forest over the same five features | ML outlier flag and method agreement |
| Production migration paths | Snowflake SQL and PySpark equivalents | Scalable, cross-platform analytical recipes |
| Decision support | Packaged Tableau workbook and dashboard screenshots | Investigation-oriented views |

### Contents

- [Architecture](#architecture)
- [Analytical data contract](#analytical-data-contract)
- [Tableau Dashboard](#tableau-dashboard)
- [Why This Project](#why-this-project)
- [Pipeline](#pipeline)
- [Key Design Decisions](#key-design-decisions)
- [Results](#results)
- [ML Anomaly Detection Layer — Isolation Forest](#ml-anomaly-detection-layer--isolation-forest)
- [Tech Stack](#tech-stack)
- [Repository Structure](#repository-structure)
- [How to Run](#how-to-run)
- [Snowflake Implementation](#snowflake-implementation)
- [PySpark Implementation](#pyspark-implementation)
- [What I Learned](#what-i-learned)
- [Author](#author)
- [License](#license)

## Architecture

The system separates the analytical grain from the investigation surface: raw transactions are transformed into an HCP-level feature table, detectors add independent signals, and the dashboard exposes the resulting queue with enough context to explain why an HCP was surfaced.

### High-level architecture

```mermaid
flowchart LR
    A[(CMS Open Payments<br/>2024 General Payments)] --> B[Ingestion & quality filters<br/>chunked CSV / deterministic sample]
    B --> C[(Payment-level sample<br/>~989K rows)]

    C --> D1[Pandas feature engineering<br/>notebook 01]
    C --> D2[Snowflake analytical SQL<br/>sql/snowflake_queries.sql]
    C --> D3[PySpark feature engineering<br/>notebook 03]

    D1 --> E[(HCP feature table<br/>~289K HCPs × 5 features)]
    E --> F1[Rule engine<br/>z-score + IQR + concentration]
    E --> F2[Isolation Forest<br/>multivariate anomaly score]
    F1 --> G[Explainable risk tiers]
    F2 --> H[ML flag & archetype comparison]
    G --> I[(Curated Tableau input)]
    H --> I
    I --> J[Tableau workbook<br/>geography · specialty · company · trend · detail]

    D2 -.reproduces findings.-> K[Warehouse findings]
    D3 -.validates parity.-> L[Distributed execution evidence]

    classDef source fill:#e8f3ff,stroke:#2b6cb0,color:#102a43;
    classDef store fill:#eefbf3,stroke:#2f855a,color:#1c4532;
    classDef model fill:#fff5eb,stroke:#c05621,color:#431407;
    class A source;
    class C,E,I store;
    class F1,F2,G,H,J model;
```

### Low-level analytical flow

```mermaid
flowchart TD
    A[Read CMS CSV in chunks of 500K] --> B[Keep physician records]
    B --> C[Drop null NPI and amount]
    C --> D[Sample 10% with random_state=42]
    D --> E[Group by Covered_Recipient_NPI]

    E --> F1[sum amount<br/>total_payment_value]
    E --> F2[count rows<br/>payment_frequency]
    E --> F3[mean amount<br/>avg_payment_size]
    E --> F4[count distinct state<br/>n_unique_states]
    E --> F5[manufacturer max share<br/>top_company_share]

    F1 --> G[HCP feature matrix]
    F2 --> G
    F3 --> G
    F4 --> G
    F5 --> G

    G --> H1[Within-specialty z-score<br/>three value/frequency features]
    G --> H2[Global IQR outlier flags<br/>three features]
    G --> H3[Concentration rule<br/>share > 0.70 and total > $500]
    H1 --> I[Signal count]
    H2 --> I
    H3 --> I
    I --> J{Tier mapping}
    J --> J1[HIGH]
    J --> J2[MEDIUM]
    J --> J3[LOW]
    J --> J4[NONE]

    G --> K[Isolation Forest<br/>200 trees · contamination 0.02]
    K --> L[ML flag]
    L --> M[Agreement matrix + archetypes]
```

### Execution backends and responsibilities

| Layer | Pandas / notebooks | Snowflake | PySpark |
|---|---|---|---|
| Exploration | Primary implementation and visual analysis | SQL reproduction of headline findings | Distributed equivalent |
| Feature grain | One row per HCP/NPI | Aggregations over `PAYMENTS_SAMPLE` | One row per HCP/NPI |
| Detector logic | Rules and Isolation Forest | Findings and warehouse-ready queries | Feature computation parity |
| Validation role | Source of analytical results | Scalable SQL migration path | Distributed execution migration path |

## Analytical Data Contract

The central interface between ingestion, detection, and presentation is the HCP-level feature table. Keeping this contract explicit makes the three execution backends comparable and makes the dashboard output understandable.

| Field | Grain | Meaning | Used by |
|---|---|---|---|
| `covered_recipient_npi` | HCP | Stable physician identifier | All joins and drill-through |
| `total_payment_value` | HCP | Sum of sampled payments in USD | Rules, ML, ranking |
| `payment_frequency` | HCP | Number of payment records | Rules, ML, dashboard |
| `avg_payment_size` | HCP | Mean payment amount in USD | Rules, ML, archetypes |
| `n_unique_states` | HCP | Distinct recipient states | ML and geographic context |
| `top_company_share` | HCP | Largest manufacturer share of HCP payment value | Concentration rule, ML |
| `risk_tier` | HCP | Rule-based `HIGH` / `MEDIUM` / `LOW` / `NONE` | Triage and Tableau |
| `ml_flag` | HCP | Isolation Forest outlier indicator | Method comparison |

The transaction-level source remains the system of record for payment detail. The HCP table is an analytical projection: it is optimized for triage, ranking, cohort comparison, and dashboard interaction—not for replacing the underlying CMS disclosure.

![Feature distributions](output/figures/feature_distributions.png)

## Tableau Dashboard

The Tableau deliverable is included locally so the analysis can be inspected and reproduced without relying on a hosted preview. The packaged workbook contains the dashboard layout, calculated fields, filters, formatting, and data-source configuration used for the screenshots below.

### Workbook and dashboard assets

- **Packaged workbook:** [`output/tableau/Pharma Compliance Spend Analytics — 2024 CMS Open Payments.twbx`](<output/tableau/Pharma Compliance Spend Analytics — 2024 CMS Open Payments.twbx>)
- **Dashboard screenshots:** [`output/tableau/`](output/tableau/)
- **Primary HCP input:** [`output/hcp_features_with_flags.csv`](output/hcp_features_with_flags.csv)
- **ML-enriched HCP input:** [`output/hcp_features_with_ml_flags.csv`](output/hcp_features_with_ml_flags.csv)

### Geographic & Specialty Overview

The overview dashboard is designed for cohort discovery. It combines a state-level payment map with risk-tier coloring, specialty-level spend stacked by risk, and a manufacturer ranking view. Together, these views expose where spend is concentrated, which specialties carry the largest payment totals, and which manufacturers dominate the payment landscape.

![Geographic and specialty overview dashboard](<output/tableau/Geographic & Specialty Overview.png>)

### Flagged Physician Detail

The detail dashboard is the investigation surface. It presents one row per HCP with NPI, specialty, state, top manufacturer, total payment value, payment frequency, and top-company share. Interactive filters support triage by risk tier, specialty, state, and manufacturer; the monthly trend provides temporal context for the filtered population.

![Flagged physician compliance dashboard](<output/tableau/Flagged Physician Compliance Dashboard.png>)

### Dashboard implementation notes

| Concern | Implementation |
|---|---|
| Analytical grain | One row per covered-recipient NPI in the HCP feature extract |
| Risk encoding | Consistent `HIGH`, `MEDIUM`, and `LOW` categorical colors across views; `NONE` remains available in the source for population analysis |
| Geographic view | State-level aggregation of payment value with risk-tier context |
| Specialty view | Total payment value segmented by risk tier to show both spend and triage concentration |
| Manufacturer view | Ranked total physician spend by manufacturer |
| Detail view | Sortable HCP-level table with compliance-relevant concentration and frequency measures |
| Time view | Monthly payment aggregation for the selected dashboard population |

To reproduce the dashboard locally, open the `.twbx` file in Tableau Desktop. If Tableau prompts for a missing data location, point the packaged data source to the generated CSV outputs in `output/`—especially `hcp_features_with_flags.csv`—or regenerate them by following [How to Run](#how-to-run). The workbook is intended as a transparent presentation layer over the documented analytical outputs; it does not introduce a separate scoring system.

---

## Why This Project

The Sunshine Act requires pharmaceutical and medical-device manufacturers to publicly report every payment to U.S. physicians. The 2024 CMS Open Payments dataset contains **16.16 million records totaling $13.18 billion**. Far more than any compliance team can manually review.

This project answers a question compliance teams ask every quarter: *"Of all the physicians who received payments this year, which ones should we investigate first?"*

The system combines two complementary statistical methods with a compliance-specific domain rule to produce a **tiered triage queue** that focuses human review on the highest-risk relationships first. This is the same triage logic used in production compliance monitoring at major pharma companies, applied here to publicly available federal data.

---

## Pipeline

```
┌─────────────────────────────┐
│ CMS Open Payments 2024 CSV  │
│ 16.16M records, ~6 GB       │
└──────────────┬──────────────┘
               │ chunked read (500K rows × 32)
               │ filter: physicians only
               │ filter: drop null NPI / amount
               │ random 10% sample (seed=42)
               ▼
┌─────────────────────────────┐
│ Sampled transactions        │
│ ~989K rows × 10 columns     │
└──────────────┬──────────────┘
               │ groupby NPI + derived features
               ▼
┌─────────────────────────────┐
│ HCP-level table             │
│ ~288K HCPs × 5 features     │
│ - total_payment_value       │
│ - payment_frequency         │
│ - avg_payment_size          │
│ - n_unique_states           │
│ - top_company_share         │
└──────────────┬──────────────┘
               │ within-specialty z-score (3 features)
               │ global IQR outliers (3 features)
               │ compliance concentration rule
               ▼
┌─────────────────────────────┐
│ Tiered risk classifier      │
│ HIGH / MEDIUM / LOW / NONE  │
└──────────────┬──────────────┘
               │
               ▼
┌─────────────────────────────┐
│ Tableau workbook + assets   │
│ Geographic + Specialty +    │
│ Company + Trend + Detail    │
└─────────────────────────────┘
```

---

## Key Design Decisions

| Decision | Choice | Why |
|---|---|---|
| **Filter to physicians only** | Drop teaching hospitals & non-physician practitioners | Cleanest analytical unit: every physician has a unique NPI, specialty, and state. Mixing in teaching hospitals (no specialty) would corrupt specialty-level z-scores. |
| **10% random sample** | `frac=0.1`, `random_state=42` | The full 16M-row file doesn't fit comfortably in 16 GB RAM. A random sample preserves distributional shape, which is what z-score and IQR rely on. The same code runs on the full file with one parameter change. |
| **Z-score within specialty** | Group by `Covered_Recipient_Specialty_1`, min 30 HCPs/specialty | Different specialties have wildly different payment baselines. Orthopedic surgeons routinely receive larger device payments than pediatricians; a single global z-score would flag entire specialties rather than unusual individuals. |
| **Both z-score AND IQR** | Compute both, combine in tier logic | The CMS payment distribution is heavily right-skewed (median $21, max $3.2M). Right-skew inflates the standard deviation enough that even genuine outliers fall below z=2. IQR catches what z-score misses. **Receipt: on `total_payment_value`, IQR flagged 30,223 HCPs that z-score missed.** |
| **Concentration with monetary floor** | `top_company_share > 0.7 AND total_payment_value > $500` | The median `top_company_share` is 1.0 (half of all HCPs received a single payment from a single rep). A naïve concentration flag would alert on 50%+ of physicians. The $500 floor strips out the casual food-and-beverage population and leaves the relationships that actually involve compensation. |
| **Tiered classifier (not binary)** | HIGH / MEDIUM / LOW / NONE based on signal count | A binary "flagged / not flagged" output is unusable when 17% of the population qualifies. Tiering gives the compliance team a triage queue: HIGH first (1.7%, manageable manual review), automated rules for MEDIUM/LOW. |

---

## Results

### Tier breakdown (288,942 HCPs total)

| Tier | Count | % of total | What it represents |
|---|---|---|---|
| **HIGH** | 4,819 | 1.67% | All three signals fire: the actionable manual-review queue |
| **MEDIUM** | 23,423 | 8.11% | High value with concentration, or high value with high frequency |
| **LOW** | 22,772 | 7.88% | Single statistical signal, no concentration |
| **NONE** | 237,928 | 82.34% | Below all thresholds |

---

## ML Anomaly Detection Layer — Isolation Forest

After shipping the rule-based detection system, I added an unsupervised ML layer to answer a question every compliance team eventually asks: *what does each method catch that the other misses?*

### Method

- **Model:** Isolation Forest (`n_estimators=200`, `contamination=0.02`, `random_state=42`) on the same 5 engineered features used by the rules.
- **Why this model:** unsupervised (CMS provides no labels), multivariate (catches feature *combinations* rules can't express), tree-based (no feature scaling required given the wildly different feature scales).
- **Why `contamination=0.02`:** matched to the rule-based HIGH rate (1.67%) so the flag volumes are comparable. Identical contamination would over-anchor; 0.02 gives ML modest room to disagree while keeping the comparison interpretable.

### Results — agreement between methods

| | ML flagged | ML inlier | Total |
|---|---|---|---|
| **Rule HIGH** | 1,600 | 3,219 | 4,819 |
| **Not rule HIGH** | 4,179 | 279,944 | 284,123 |
| **Total** | 5,779 | 283,163 | 288,942 |

| Metric | Value |
|---|---|
| Jaccard similarity | 0.178 |
| % of rule-HIGH also flagged by ML | 33.2% |
| % of ML flags also rule-HIGH | 27.7% |
| Cohen's kappa (chance-corrected agreement) | 0.289 |

### Finding — Three distinct compliance archetypes

The disagreement is the finding. Methods catch structurally different patterns:

| Archetype | Group | n | Median total | Median avg pmt | Top company share | Typical specialties |
|---|---|---|---|---|---|---|
| **Captured specialist** | BOTH | 1,600 | $13,742 | $828 | 0.93 | Orthopedic surgery, dental |
| **Captured generalist** | RULE_ONLY | 3,219 | $3,070 | $254 | 0.91 | General practice, cardiology, oncology, pediatrics |
| **Industry consultant** | ML_ONLY | 4,179 | $13,111 | **$1,529** | 0.63 | Dermatology, oncology, rheumatology, advanced cardiology |

**1. Captured specialists (BOTH).** Both methods agree on the unambiguous compliance leads — high-volume device-manufacturer relationships in orthopedic surgery and dental. Top 5 most-anomalous-by-ML in this group are dominated by Stryker, Zimmer Biomet, Skeletal Dynamics, and Align Technology. This is the consensus triage queue.

**2. Captured generalists (RULE_ONLY).** Rules surface 3,219 physicians outside high-spend specialties who receive modest absolute totals (~$3K median) but heavily concentrated payments from a single manufacturer (≥91% top share). ML doesn't isolate them because in absolute multivariate space they look ordinary. **This is precisely why pure-ML is risky for compliance** — small-dollar captured-prescriber patterns are exactly the cases where the *pattern* matters more than the magnitude. Rules win this category.

**3. Industry consultants (ML_ONLY).** ML surfaces 4,179 physicians who earn high totals across *many* manufacturers — diversified industry influence rather than single-firm capture. Median average payment is $1,529, the highest of any group, and ~63% top-company-share means most payments come from secondary sources. Rules cannot catch them because the concentration signal explicitly excludes diversified physicians. ML catches them via the combination: high magnitude + high per-payment + multi-state + diversified manufacturers is an isolated combination in feature space. This is the multivariate signal pure-rules cannot express.

### What the visualization shows

![Three compliance archetypes](output/figures/rules_vs_ml_archetypes.png)

X-axis is concentration (0 = diversified, 1 = single manufacturer). Y-axis is total payment value, log scale. BOTH (red) and RULE_ONLY (blue) cluster on the right — concentrated relationships. **ML_ONLY (orange) sits on the upper LEFT** — physicians with large totals and diversified manufacturer mix. That left-side cluster is the regime no rule-based system can reach.

### Production recommendation

A real compliance system should run both detectors and triage each archetype separately:

- **BOTH** → highest priority, both signals fire.
- **RULE_ONLY** → review for captured-prescriber patterns in low-spend specialties.
- **ML_ONLY** → review for industry-consultant patterns; candidates for *new rule definitions* (e.g., "diversified high-magnitude relationships").

Either method alone misses ~67–72% of what the other catches. The pair is strictly stronger.

### Top HIGH-risk HCPs — the headline finding

The top 10 HIGH-risk physicians by total payment value are **all orthopedic-related specialists**, with **9 of 10 receiving over 99% of their payments from a single device manufacturer**:

| Rank | Specialty | State | Total Paid | Frequency | Top Company | Share |
|---|---|---|---|---|---|---|
| 1 | Adult Reconstructive Orthopedic Surgery | ID | $3,200,055 | 14 | Zimmer Biomet | 100% |
| 2 | Vascular & Interventional Radiology | AZ | $1,813,137 | 13 | Boston Scientific | 100% |
| 3 | Orthopaedic Surgery | NY | $950,342 | 19 | Arthrex | 100% |
| 4 | Orthopaedic Sports Medicine | IL | $808,444 | 10 | Arthrex | 94% |
| 5 | Orthopaedic Surgery | IL | $543,748 | 29 | Stryker | 100% |
| 6 | Orthopaedic Surgery | MN | $442,356 | 11 | Stryker | 100% |
| 7 | Orthopaedic Surgery | GA | $435,248 | 9 | Stryker | 100% |
| 8 | Adult Reconstructive Orthopedic Surgery | NY | $312,872 | 19 | Stryker | 100% |
| 9 | Orthopaedic Surgery | WI | $302,522 | 22 | Stryker | 100% |
| 10 | Orthopaedic Surgery | NY | $265,922 | 18 | Smith+Nephew | 86% |

**Interpretation:** Orthopedic surgeons are the highest-leverage prescribers for joint-replacement and sports-medicine devices. The top 10 list reveals a structural pattern. Four manufacturers (Stryker, Arthrex, Zimmer Biomet, Boston Scientific) collectively dominate orthopedic-physician compensation. This is exactly the kind of concentration the Sunshine Act was designed to make visible.

---

## Tech Stack

- **Python 3.10+** : `pandas`, `numpy`, `scipy`, `matplotlib`, `seaborn`
- **Tableau Desktop** : packaged workbook and reproducible dashboards
- **Jupyter** : analysis notebook
- **Git** : version control

---

## Repository Structure

```
pharma-compliance-spend-analytics/
├── notebooks/
│   └── 01_data_features.ipynb     # End-to-end pipeline
├── output/
│   ├── hcp_features.csv           # HCP-level features (no flags)
│   ├── hcp_features_with_flags.csv # HCP features + risk tiers (Tableau input)
│   └── figures/                    # Analysis and dashboard evidence figures
├── data/                          # gitignored, see "How to Run"
├── requirements.txt
├── .gitignore
└── README.md
```

### Artifact map

| Path | Purpose | Reproducibility role |
|---|---|---|
| [`notebooks/01_data_features.ipynb`](notebooks/01_data_features.ipynb) | Ingests the CMS file, engineers HCP features, applies rule-based tiers, and writes analytical outputs | Primary end-to-end workflow |
| [`notebooks/02_isolation_forest.ipynb`](notebooks/02_isolation_forest.ipynb) | Trains the unsupervised detector and compares ML flags with rule HIGH | ML layer and archetype analysis |
| [`notebooks/03_pyspark_pipeline.ipynb`](notebooks/03_pyspark_pipeline.ipynb) | Rebuilds the feature table with DataFrame API and Spark SQL | Distributed execution and parity evidence |
| [`sql/snowflake_queries.sql`](sql/snowflake_queries.sql) | Warehouse setup plus five analytical queries | Cloud SQL reproduction path |
| `output/hcp_features*.csv` | HCP-level analytical extracts with and without flags | Tableau and cross-platform comparison inputs |
| `output/figures/` | Static charts for method comparison and Snowflake findings | README evidence and presentation assets |
| [`requirements.txt`](requirements.txt) | Captured Python environment dependencies | Local environment bootstrap |

### Reproducibility and validation

The analytical run is deterministic at the sampling boundary (`random_state=42`). Validation is performed at three levels:

1. **Distributional:** feature distributions and long-tail behavior are inspected before thresholds are selected.
2. **Methodological:** rule-based tiers are compared with Isolation Forest flags using a confusion matrix, Jaccard similarity, and Cohen's kappa.
3. **Cross-platform:** the PySpark feature table is joined back to the Pandas output HCP-by-HCP; cents-level rounding resolves floating-point accumulation order differences.

The published counts and figures describe the checked 2024 10% physician sample. Re-running against a changed CMS release, a different sample fraction, or altered filters should be treated as a new analytical run and should produce a new result snapshot.

### Scope and interpretation

This is a decision-support and investigation-prioritization system, not a finding of misconduct. A flag identifies an unusual payment pattern under the documented thresholds; it does not establish intent, non-compliance, or a violation. Any operational deployment would need source-version tracking, access controls, audit logging, investigator disposition labels, threshold governance, and review by compliance and legal stakeholders.

---

## How to Run

### 1. Clone and set up environment

```bash
git clone https://github.com/0xs1d/Pharma-Compliance-Spend-Analytics.git
cd pharma-compliance-spend-analytics
python -m venv .venv
.venv\Scripts\activate   # Windows
# source .venv/bin/activate  # macOS/Linux
pip install -r requirements.txt
```

### 2. Download the CMS data

The 2024 General Payments file is too large to host on GitHub (~6 GB). Download from CMS:

```
https://download.cms.gov/openpayments/PGYR2024_P06302025_06162025/OP_DTL_GNRL_PGYR2024_P06302025_06162025.csv
```

Save as `data/general_payments_2024.csv`.

### 3. Run the notebook

```bash
jupyter notebook notebooks/01_data_features.ipynb
```

Run all cells. Total runtime: ~3 minutes on a modern laptop.

### 4. Open the Tableau workbook (optional)

Open the packaged [`Tableau workbook`](<output/tableau/Pharma Compliance Spend Analytics — 2024 CMS Open Payments.twbx>) in Tableau Desktop. To rebuild or modify it locally, use `output/hcp_features_with_flags.csv` and `output/payments_sampled.csv` (regenerated by the notebook) as data sources. Reference screenshots and dashboard notes are in the [Tableau Dashboard](#tableau-dashboard) section.

---

## Snowflake Implementation

After shipping the rule-based and ML detection systems in Pandas, I ported the 989K-row CMS sample to Snowflake to demonstrate the production analytics path. Five SQL queries — captured in [`sql/snowflake_queries.sql`](sql/snowflake_queries.sql) — reproduce the key findings from the Pandas EDA in a fully scalable cloud data warehouse.

### Setup

```sql
CREATE DATABASE PHARMA_COMPLIANCE;
CREATE SCHEMA   PHARMA_COMPLIANCE.CMS;

-- payments_sampled.csv loaded via Snowsight wizard;
-- column names normalized via ALTER TABLE RENAME COLUMN.
-- Final table: PHARMA_COMPLIANCE.CMS.PAYMENTS_SAMPLE (988,821 rows × 10 cols)
```

Compute: default `COMPUTE_WH` (X-SMALL, auto-suspends after 10 min). The full query suite runs in under 2 seconds on the 989K-row table.

### Headline finding — orthopedic surgery's manufacturer capture

Top manufacturers by total payments within Orthopaedic Surgery, with each one's share of the specialty's total spend:

| Manufacturer | Physicians paid | Total paid (USD) | Share of orthopedic spend |
|---|---:|---:|---:|
| Arthrex, Inc. | 1,629 | $10,531,091 | **21.4%** |
| Stryker Corporation | 4,036 | $8,943,117 | **18.1%** |
| Zimmer Biomet Holdings | 1,257 | $5,778,445 | **11.7%** |
| Smith+Nephew, Inc. | 1,520 | $4,213,432 | **8.5%** |
| DePuy Synthes Products | 117 | $3,951,145 | 8.0% |
| Globus Medical | 691 | $1,495,449 | 3.0% |
| Medtronic, Inc. | 636 | $1,431,317 | 2.9% |

**Top 4 manufacturers capture 59.7% of all orthopedic surgery payments. Top 5 capture 67.7%.** No other specialty in the dataset shows structural concentration at this magnitude.

Notable outlier — **DePuy Synthes pays only 117 physicians but moves $3.95M** ($33,770 per physician, the highest concentration-per-physician of any major orthopedic manufacturer). Arthrex spreads $10.5M across 1,629 physicians ($6,464 per physician). Same specialty, fundamentally different go-to-market patterns.

![Snowflake query: orthopaedic capture](output/figures/snowflake_orthopaedic_capture.png)

### Other findings reproduced in SQL

**Cross-industry manufacturer dispersion:** Stryker leads total payments with only 14,812 transactions; AbbVie has 93,880 transactions (6× more) but lower total. Classic device-royalty vs pharma-marketing structural difference.

**Specialty disparity:** Orthopaedic Surgery is #1 in total spend with 8,901 physicians; Internal Medicine has 41,349 physicians (4.6× more) but ranks below in total dollars. Average per physician differs by an order of magnitude.

**Payment distribution shape (988,821 transactions):**

| Statistic | Value |
|---|---:|
| Mean payment | $220.87 |
| Median (P50) | $21.26 |
| P75 | $33.11 |
| P90 | $137.73 |
| P95 | $458.30 |
| P99 | $3,955.00 |
| Max single payment | $3,199,444.94 |

Mean is roughly 10× median — extreme right skew. This data shape justifies the rule-based system's use of IQR (Q3 + 1.5·IQR) alongside z-score: the long tail inflates standard deviation enough to hide moderate outliers from z-score detection. SQL percentiles confirm what Pandas distribution analysis showed.

### Why Snowflake?

Pandas handles the 989K-row sample comfortably in memory. The full 16M-row dataset doesn't — and a production compliance system would run on the full data across multiple program years, joined to investigation outcomes, refreshed weekly. Snowflake is the canonical platform for that workload in regulated industries: decoupled compute and storage, standard SQL (queries portable to Postgres / BigQuery / Databricks SQL with minor changes), and auto-suspending warehouses that bill only for active compute. The implementation here proves the migration path: same logic, scaled platform.

![Snowflake Database Explorer](output/figures/snowflake_database_explorer.png)

---

## PySpark Implementation

Third execution backend for the same HCP feature-engineering pipeline. Together with Pandas (notebook 01) and Snowflake SQL ([`sql/snowflake_queries.sql`](sql/snowflake_queries.sql)), this completes a deliberate demonstration of platform-fit reasoning for compliance analytics at three different scales.

| Platform | Best for | This project's role |
|---|---|---|
| **Pandas** | In-memory datasets, fast iteration | Built the rule-based and ML detection layers |
| **Snowflake** | Cloud SQL, multi-user analytics | Production analytics path; recruiters can verify with SQL |
| **PySpark** | Distributed ETL, multi-TB pipelines | Demonstrates the migration path beyond single-machine limits |

Notebook: [`notebooks/03_pyspark_pipeline.ipynb`](notebooks/03_pyspark_pipeline.ipynb)

### What the notebook demonstrates

**1. DataFrame API and Spark SQL parity.** The 5 HCP-level features are computed two ways — once with `groupBy().agg()` + `F.max_by()` for top-company-per-NPI, once with a single Spark SQL query using `ROW_NUMBER() OVER (PARTITION BY ...)` window functions. Both produce 288,942 rows with identical values. Same query plan under the hood; the choice between APIs is team preference, not capability.

**2. Lazy evaluation visualized.** `.explain(mode="formatted")` prints the full physical query plan before execution — HashAggregate stages, hashpartitioning shuffles, BroadcastHashJoins, and AQE optimizations the engine selected at runtime. Spark optimizes the entire pipeline before running anything, which Pandas can't do because it executes line-by-line.

**3. Caching speedup.** Materializing the HCP DataFrame and re-counting yields a ~7× speedup on the cached version (1.6s → 0.225s) for this dataset. On multi-GB tables the speedup is typically 50–500× — the reason production Spark pipelines cache reusable intermediates aggressively.

**4. Cross-platform equivalence verified.** The PySpark output is row-level joined to the Pandas pipeline's output and compared HCP-by-HCP across all five features:

| Feature | Match |
|---|---|
| `payment_frequency` | 100.00% exact |
| `n_unique_states` | 100.00% exact |
| `avg_payment_size` | 100.00% at 1e-4 tolerance |
| `top_company_share` | 100.00% at 1e-4 tolerance |
| `total_payment_value` | 86.25% exact / 100% within floating-point precision |

The 13.75% of HCPs that don't match exactly on `total_payment_value` differ by sub-penny amounts — IEEE 754 floating-point addition isn't strictly associative, and PySpark sums the per-HCP payments in a different partition order than Pandas. Rounding to cents (the natural representation for dollar values) collapses all 288,942 rows to exact match. **The top 5 highest-paid HCPs are bit-for-bit identical** between platforms, confirming practical equivalence.

### Why a third implementation matters

The interview question this answers: *"What do you do when Pandas isn't enough?"* This notebook is the demonstrated answer — same logic, distributed execution, with the migration receipts. Each engineering decision (lazy plan inspection, partition sizing, caching, DataFrame-vs-SQL API choice) is exercised on real data with measurable outcomes.

---

## What I Learned

- **Right-skew breaks z-score.** On `total_payment_value`, z-score within specialty flagged 1.5% of HCPs, *fewer* than the 2.3% expected from a normal distribution. The standard deviation was inflated badly enough by the long tail that even genuine outliers fell below z=2. Switching to IQR caught **30,223 additional HCPs** that z-score missed.
- **Domain rules outperform pure statistics.** A naïve "flag everyone with `top_company_share > 0.7`" rule would have flagged 50%+ of the dataset because the median share is 1.0 (most physicians who receive payments only get them from one rep). Adding a $500 monetary floor to the concentration rule cut the flag rate from 73.7% to 17.7% without losing a single HIGH-tier HCP.
- **Tiered triage matters.** Compliance teams can't review 50,000 flags. They can review ~5,000 HIGH-tier flags. The tier system isn't theoretical polish. It's the difference between a system that gets adopted and one that gets ignored.
- **Specialty-level structure is a finding, not a parameter.** Going in, I expected anomalies to be distributed across specialties. The data revealed that orthopedic surgery is structurally captured by four device manufacturers, a finding that wouldn't have surfaced without specialty-level grouping in the analysis.
