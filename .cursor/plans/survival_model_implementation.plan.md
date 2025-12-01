Based on the review of the codebase and the upgrade plan, here is a detailed breakdown of the current functionality and the implementation plan for the survival model upgrades.

### **1. Current System Functionality**

The current system (`src/core/numeric_analyzer.py`) uses a **deterministic, rules-based engine** to calculate baselines.

-   **Gestation:** Calculates the **median** duration of past projects. It uses simple IQR (Interquartile Range) for confidence but doesn't truly model the "shape" of time.
-   **Conversion:** Calculates a simple **win rate** (Won / Total). It uses "Beta Smoothing" (adding dummy wins/losses) to prevent wild swings on small sample sizes, but it treats "Open" projects somewhat statically (either ignoring them or treating them as non-wins).
-   **Segmentation:** It tries to find specific matches (e.g., `Account="A" AND Product="B"`). If it finds fewer than 15 examples, it "backs off" to broader categories (e.g., just `Product="B"`), dropping the specific account details entirely.
-   **Rating Score:** A hard-coded weighted formula (0-100) combining conversion, gestation, and value.

---

### **2. Upgrade Implementation Plan (Model Logic)**

The goal is to move from **static averages** to **probabilistic models** that understand *time* and *uncertainty*.

#### **Phase 1: Dependencies & Data Preparation**

You will need to add the following libraries to `requirements.txt`:

-   `lifelines` (for survival analysis/Kaplan-Meier/CoxPH)
-   `lightgbm` (for gradient-boosted quantile regression)
-   `scikit-learn` (for general ML utilities)

**Inputs Required:**

The new models require a flattened dataset where every row is a historical project with:

1.  **Features:** `account`, `project_type`, `category`, `product_type`, `value_band`, `new_enquiry_value`.
2.  **Time-to-Event Targets:**

    -   `duration`: Days from creation to *now* (if open) or to *outcome date* (if closed).
    -   `event_occurred`: `1` if Won, `0` if Lost or Open (for simple survival) OR `0`=Open, `1`=Won, `2`=Lost (for competing risks).

#### **Phase 2: Hierarchical Partial Pooling (The "Smart" Average)**

*Problem:* The current system drops `Account="A"` data if there are only 5 examples.

*Solution:* **Partial Pooling** blends the specific data with the global average mathematically.

**Implementation Logic:**

Instead of `if count < 15: use_global`, we use a **shrinkage estimator**:

$$ \text{Estimate} = w \cdot (\text{Account Rate}) + (1-w) \cdot (\text{Global Rate}) $$

Where $w$ gets closer to 1 as the Account's sample size increases.

-   **Action:** Create a `HierarchicalBayes` class in a new `src/core/ml/` module.
-   **Method:** Implement a Beta-Binomial model.
    -   Calculate global `alpha`, `beta` (prior).
    -   Update with segment specific `wins`, `losses`.
    -   Output: A "posterior mean" that automatically trusts small segments *just enough*.

#### **Phase 3: Survival Analysis (Predicting "When")**

*Problem:* Existing open projects are ignored or treated as losses, biasing predictions downward.

*Solution:* **Censoring-aware models**. A project open for 300 days tells us something different than one open for 3 days.

**Implementation Logic:**

1.  **Conversion Model (Cox PH or Weibull AFT):**

    -   Train on all data.
    -   **Input:** Features + `duration` + `status` (Won=1, Other=0).
    -   **Output:** A survival curve $S(t)$. The probability of winning by day $t$ is $1 - S(t)$.
    -   *Key Feature:* It learns that "Project Type X" usually wins late, while "Type Y" wins early.

2.  **Prediction:**

    -   For a live project open for 20 days, calculate $P(\text{Win} | T > 20)$.
    -   Output: "Win probability within next 30 days."

#### **Phase 4: Quantile Regression (Predicting Ranges)**

*Problem:* "Average gestation is 45 days" hides the fact that 20% of jobs take 200 days.

*Solution:* Predict the **p25 (optimistic)**, **p50 (median)**, and **p75 (conservative)** timelines.

**Implementation Logic:**

-   **Model:** LightGBM Regressor.
-   **Objective:** `quantile` (train three separate models or one multi-output model with `alpha=[0.25, 0.5, 0.75]`).
-   **Features:** `value`, `project_type`, `category`, `product`.
-   **Output:** "We expect this to take 30-60 days (Median: 42)."

---

### **3. Architecture: Shadow Mode Deployment**

We will run the new ML-based system in parallel with the existing calculator-based system using a new service and database table. This is the **safest and most robust strategy** for this upgrade, often called "Shadow Mode" or "Side-by-Side" deployment.

It minimizes risk to your production environment while allowing you to validate the complex new models (Survival Analysis, Quantile Regression) against the current "official" logic.

#### **Analysis of Approach**

-   **✅ Risk Mitigation:** The current `AnalysisService` remains the "source of truth" for Monday.com updates. If the new ML model hallucinates or fails, operations are unaffected.
-   **✅ Comparison:** Storing results in a separate table allows you to query `analysis_results` (old) vs `ml_analysis_results` (new) to calculate the exact accuracy improvement (C-index, MAE) promised in your plan.
-   **✅ Separation of Concerns:** The new ML logic will require heavy libraries (`lifelines`, `lightgbm`) and complex data processing. Keeping this out of the legacy file keeps the codebase clean.

---

### **4. Recommended Implementation Plan**

Here is the step-by-step breakdown of how we should structure this "Shadow Mode" implementation:

#### **Step 1: Database Changes (New Table)**

We will create a new table `ml_analysis_results` that mirrors the existing one but adds fields for the probabilistic outputs (timelines, confidence intervals).

**File:** `src/database/schema/ml_results.sql`

```sql
CREATE TABLE ml_analysis_results (
    id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    project_id TEXT REFERENCES projects(monday_id) ON DELETE CASCADE,
    analysis_timestamp TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    -- New Probabilistic Fields
    win_prob_30d NUMERIC(4, 3),  -- Survival model output
    win_prob_90d NUMERIC(4, 3),
    gestation_p25 INTEGER,       -- Quantile regression output
    gestation_p50 INTEGER,
    gestation_p75 INTEGER,
    
    -- Standard Fields (for comparison)
    rating_score INTEGER,
    
    -- Metadata
    model_version TEXT,          -- e.g., "v1.0-survival"
    features_used JSONB,
    processing_time_ms INTEGER
);
```

#### **Step 2: Code Structure (New Modules)**

Instead of one large file, we should organize the ML logic into a dedicated module and a service wrapper.

-   **`src/core/ml/`**: A new package for the heavy lifting.
    -   `dataset.py`: Shared logic to fetch and flatten project history (prevents duplication).
    -   `survival.py`: Logic for CoxPH/Weibull models (conversion & timing).
    -   `quantiles.py`: Logic for LightGBM models (gestation ranges).

-   **`src/services/ml_analysis_service.py`**: The service class you requested.
    -   It will mimic the interface of `AnalysisService` but call the `src/core/ml/` components.
    -   It will save to the new `ml_analysis_results` table.

#### **Step 3: Integration (The "Shadow" Trigger)**

To automate the comparison, we will hook into the existing worker. When a project is updated, we run the standard analysis *and* the ML analysis (catching any errors so the main flow never breaks).

**File:** `src/services/queue_worker.py` (Concept)

```python
# Inside _handle_rehydrate method:

# 1. Run Standard Analysis (Critical Path)
analysis = AnalysisService()
result = analysis.analyze_and_store(project_id)

# 2. Run ML Shadow Analysis (Non-blocking / Safe)
try:
    ml_analysis = MLAnalysisService()  # The new service
    ml_analysis.analyze_and_store(project_id)
except Exception as e:
    logger.warning(f"ML Shadow Analysis failed: {e}") # Log but don't stop
```

### **Next Execution Steps**

1.  **Create the new Database Table:** Create the SQL file and run migration.
2.  **Create the ML Core Modules:** Implement `src/core/ml/` skeleton.
3.  **Create the ML Service:** Implement `src/services/ml_analysis_service.py`.