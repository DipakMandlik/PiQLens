# Antigravity - Local LLM-Powered Observability

**Status**: 🚧 In Development - Ollama Integration Phase

Antigravity is an AI-powered data observability engine that uses **local open-source LLMs** (via Ollama) to generate insights from your data quality metrics.

## Key Features

✅ **No API Limits** - Runs entirely on local Mixtral/Llama3 models  
✅ **IDE-First Architecture** - Jupyter notebooks for transparency and debugging  
✅ **Anti-Hallucination** - Strict schema validation prevents LLM errors  
✅ **Business-First Insights** - Natural language answers, not SQL jargon  
✅ **Learning Loop** - Comprehensive audit logging for continuous improvement  

## Architecture

```
User Question
     ↓
[Ollama LLM] → Generate Execution Plan
     ↓
[Schema Validator] → Validate against Registry
     ↓
[SQL Builder] → Generate Deterministic SQL
     ↓
[Snowflake] → Execute Query
     ↓
[Ollama LLM] → Interpret Results
     ↓
Business Insights (Executive Answer)
```

## Directory Structure

```
antigravity/
├── engine/              # Core execution modules
│   ├── llm_router.ts           # Ollama API client
│   ├── plan_validator.ts       # Schema validation
│   ├── sql_builder.ts          # Deterministic SQL generation
│   ├── ollama_execution_engine.ts  # Main orchestrator
│   └── audit_logger.ts         # Execution logging
│
├── prompts/             # LLM prompt templates
│   ├── plan_generation.prompt
│   └── business_interpretation.prompt
│
├── ide/notebooks/       # Interactive development notebooks
│   ├── 01_schema_registry.ipynb
│   ├── 02_llm_plan_generation.ipynb
│   ├── 03_sql_execution.ipynb
│   ├── 04_llm_interpretation.ipynb
│   └── 05_business_response.ipynb
│
├── setup_ollama.ps1     # Automated Ollama installation
└── requirements.txt     # Python dependencies
```

## Quick Start

### 1. Install Ollama

**Windows (PowerShell as Administrator)**:
```powershell
.\antigravity\setup_ollama.ps1
```

**Manual Installation**:
1. Download from https://ollama.com/download
2. Install and start service
3. Pull model: `ollama pull mixtral:8x7b`

### 2. Verify Ollama

```bash
# Check service
curl http://localhost:11434/api/tags

# Test model
ollama run mixtral:8x7b "Respond with SUCCESS if you can read this"
```

### 3. Set Up Database

Run the SQL setup script in Snowflake:
```sql
-- Execute this in Snowflake
@sql/setup_antigravity_observability.sql
```

This creates:
- `DQ_METRICS` - Unified metrics table
- `DQ_AI_INSIGHTS` - AI-generated insights
- `DQ_LLM_EXECUTION_LOG` - Audit log

### 4. Install Python Dependencies (for notebooks)

```bash
cd antigravity
pip install -r requirements.txt
```

### 5. Try the Notebooks

```bash
cd ide/notebooks
jupyter notebook
```

Run notebooks in order: 01 → 02 → 03 → 04 → 05

## Usage

### Via API (TypeScript/Next.js)

```typescript
import { executeWithOllama } from '@/antigravity/engine/ollama_execution_engine';
import { introspectSchema } from '@/lib/antigravity/schema-reader';

// 1. Build schema registry
const schema = await introspectSchema('DATA_QUALITY_DB', ['DB_METRICS']);

// 2. Execute question
const result = await executeWithOllama(
  "How has quality changed this week?",
  "BANKING_DW.BRONZE.STG_ACCOUNT",
  schema
);

// 3. Display insights
if (result.status === 'SUCCESS') {
  console.log(result.interpretation.summary);
  console.log(result.interpretation.actions);
}
```

### Via Notebooks (Interactive)

See `ide/notebooks/README.md` for detailed notebook usage.

## Configuration

### Environment Variables

Create `.env.local` in project root:

```env
# Snowflake
SNOWFLAKE_ACCOUNT=your_account
SNOWFLAKE_USER=your_user
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_WAREHOUSE=your_warehouse

# Ollama (optional, defaults shown)
OLLAMA_ENDPOINT=http://localhost:11434
OLLAMA_MODEL=mixtral:8x7b
```

### Model Selection

**Mixtral 8x7b** (default):
- Size: ~26GB
- Speed: Slower (~10-20s per request)
- Quality: Higher accuracy, better reasoning

**Llama3 8b** (alternative):
- Size: ~4.7GB
- Speed: Faster (~3-5s per request)
- Quality: Good for simpler questions

Change model in `.env.local`:
```env
OLLAMA_MODEL=llama3:8b
```

## Operating Rules

🚫 **LLM execution is mandatory** - No silent fallbacks  
🚫 **No dummy SQL** - All SQL is executed on real data  
🚫 **Schema always validated** - Refuse execution if registry is stale  
✅ **Business text first** - Evidence collapsed by default  
✅ **Loud failures** - All errors logged and surfaced to UI  

## Monitoring

### Execution Stats

```sql
SELECT
  EXECUTION_STATUS,
  COUNT(*) as count,
  AVG(TOTAL_EXECUTION_TIME_MS) as avg_time_ms
FROM DATA_QUALITY_DB.DB_METRICS.DQ_LLM_EXECUTION_LOG
WHERE CREATED_AT >= DATEADD('DAY', -7, CURRENT_TIMESTAMP())
GROUP BY EXECUTION_STATUS;
```

### Common Errors

```sql
SELECT
  ERROR_MESSAGE,
  COUNT(*) as occurrences
FROM DATA_QUALITY_DB.DB_METRICS.DQ_LLM_EXECUTION_LOG
WHERE EXECUTION_STATUS != 'SUCCESS'
  AND CREATED_AT >= DATEADD('DAY', -7, CURRENT_TIMESTAMP())
GROUP BY ERROR_MESSAGE
ORDER BY occurrences DESC
LIMIT 10;
```

## Troubleshooting

### Ollama Not Responding

```bash
# Check if Ollama is running
curl http://localhost:11434/api/tags

# Start Ollama service
ollama serve

# Check logs
ollama logs
```

### Schema Validation Failures

```bash
# Refresh schema registry
cd ide/notebooks
jupyter notebook 01_schema_registry.ipynb
# Run all cells
```

### Slow Response Times

- Switch to Llama3: `OLLAMA_MODEL=llama3:8b`
- Reduce time windows in questions
- Check Ollama resource usage: `ollama ps`

## Development

### Adding New Prompts

1. Create prompt file in `prompts/`
2. Use template variables: `{{VARIABLE_NAME}}`
3. Test in notebook before production
4. Update `llm_router.ts` to load new prompt

### Tuning Prompts

1. Run notebook with current prompt
2. Review LLM output quality
3. Modify prompt template
4. Re-run and compare
5. Log improvements in audit table

## Roadmap

- [x] Phase 1: Infrastructure & Ollama setup
- [x] Phase 2: Schema registry
- [x] Phase 3: LLM router & integration
- [x] Phase 4: Plan generation & validation
- [x] Phase 5: SQL builder & execution
- [x] Phase 6: Interpretation engine
- [x] Phase 7: Audit & learning loop
- [ ] Phase 8: API integration
- [ ] Phase 9: Frontend updates
- [ ] Phase 10: Testing & deployment

## License

Proprietary - Pi_Qualytics

## Support

For issues or questions, see `implementation_plan.md` for detailed architecture and troubleshooting.
