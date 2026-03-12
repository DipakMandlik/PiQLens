# Antigravity IDE - Jupyter Notebooks

This directory contains interactive notebooks for developing, testing, and debugging Antigravity's AI-powered observability engine.

## Notebooks

### 01_schema_registry.ipynb
**Purpose**: Build and validate the schema registry

- Introspect Snowflake `INFORMATION_SCHEMA`
- Build comprehensive table/column registry
- Validate completeness
- Export for LLM consumption

**Run this first** - Schema registry is mandatory for all other operations.

---

### 02_llm_plan_generation.ipynb
**Purpose**: Test LLM plan generation with Ollama

- Load schema registry
- Test sample questions
- Validate JSON plan output
- Debug plan generation failures

---

### 03_sql_execution.ipynb
**Purpose**: Build and execute SQL from plans

- Load validated plan
- Build deterministic SQL
- Execute on Snowflake
- Visualize results

---

### 04_llm_interpretation.ipynb
**Purpose**: Generate business insights from results

- Load query results
- Call Ollama for interpretation
- Format business-first output
- Compare with deterministic interpretation

---

### 05_business_response.ipynb
**Purpose**: Assemble final executive answer

- Combine plan + results + interpretation
- Format as ExecutiveAnswer block
- Add collapsible evidence
- Export for API response

---

## Getting Started

1. **Install dependencies**:
   ```bash
   pip install -r ../../requirements.txt
   ```

2. **Configure environment**:
   Create `.env` file with Snowflake credentials:
   ```
   SNOWFLAKE_ACCOUNT=your_account
   SNOWFLAKE_USER=your_user
   SNOWFLAKE_PASSWORD=your_password
   SNOWFLAKE_WAREHOUSE=your_warehouse
   ```

3. **Start Jupyter**:
   ```bash
   jupyter notebook
   ```

4. **Run notebooks in order** (01 → 02 → 03 → 04 → 05)

---

## Why Notebooks?

Notebooks make Antigravity:
- **Transparent**: See every step of the AI pipeline
- **Debuggable**: Inspect LLM outputs, SQL, and results
- **Impressive**: Demo real AI to technical reviewers
- **Improvable**: Tune prompts with evidence

This is the IDE-first architecture that sets Antigravity apart.
