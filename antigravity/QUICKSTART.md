# Antigravity Ollama Integration - Quick Start Guide

## ✅ What's Been Implemented

### Core Infrastructure (Phases 1-7)
- ✅ Ollama setup script (`setup_ollama.ps1`)
- ✅ LLM router with health checks (`engine/llm_router.ts`)
- ✅ Plan validator with schema validation (`engine/plan_validator.ts`)
- ✅ Deterministic SQL builder (`engine/sql_builder.ts`)
- ✅ Audit logger with auto-healing (`engine/audit_logger.ts`)
- ✅ Complete execution engine (`engine/ollama_execution_engine.ts`)
- ✅ Prompt templates (plan generation + business interpretation)
- ✅ Schema registry notebook (`ide/notebooks/01_schema_registry.ipynb`)
- ✅ SQL schema with audit table (`DQ_LLM_EXECUTION_LOG`)

### API Integration (Phase 8)
- ✅ `/api/antigravity/ask-question` - Uses Ollama for Q&A
- ✅ `/api/antigravity/generate-insights` - Uses Ollama for insights
- ✅ `/api/antigravity/health` - Ollama service status

---

## 🚀 Getting Started (5 Steps)

### Step 1: Install Ollama

**Windows (PowerShell as Administrator)**:
```powershell
cd "c:\Users\Dipak.Mandlik\OneDrive - PibyThree Consulting Services Private Limited\Desktop\Pi-Qualytics-main"
.\antigravity\setup_ollama.ps1
```

This will:
- Download and install Ollama
- Start the service
- Pull Mixtral 8x7b model (~26GB, takes 10-20 min)
- Verify installation

**Manual Alternative**:
```bash
# Download from https://ollama.com/download
# Install and run:
ollama serve

# Pull model:
ollama pull mixtral:8x7b
# OR for faster/smaller:
ollama pull llama3:8b
```

---

### Step 2: Verify Ollama is Running

```bash
# Check service
curl http://localhost:11434/api/tags

# Test model
ollama run mixtral:8x7b "Respond with SUCCESS if you can read this"
```

Expected output: Model should respond with "SUCCESS"

---

### Step 3: Run SQL Setup in Snowflake

Execute this file in your Snowflake worksheet:
```
sql/setup_antigravity_observability.sql
```

This creates:
- `DQ_METRICS` - Unified metrics table
- `DQ_AI_INSIGHTS` - AI-generated insights
- `DQ_LLM_EXECUTION_LOG` - Audit log (NEW)

Verify:
```sql
SHOW TABLES IN SCHEMA DATA_QUALITY_DB.DB_METRICS;
SELECT * FROM DATA_QUALITY_DB.DB_METRICS.DQ_LLM_EXECUTION_LOG LIMIT 1;
```

---

### Step 4: Test the API

**Health Check**:
```bash
curl http://localhost:3000/api/antigravity/health
```

Expected response:
```json
{
  "status": "healthy",
  "service": "ollama",
  "endpoint": "http://localhost:11434",
  "model": "mixtral:8x7b",
  "availableModels": ["mixtral:8x7b", ...]
}
```

**Ask a Question** (via UI or API):
1. Start dev server: `npm run dev`
2. Navigate to Investigation Mode
3. Select an asset (e.g., `BANKING_DW.BRONZE.STG_ACCOUNT`)
4. Ask: "How has quality changed this week?"
5. Wait 20-40 seconds for Ollama response

---

### Step 5: Monitor Execution Logs

```sql
-- View recent executions
SELECT
  EXECUTION_ID,
  USER_QUESTION,
  EXECUTION_STATUS,
  TOTAL_EXECUTION_TIME_MS,
  CREATED_AT
FROM DATA_QUALITY_DB.DB_METRICS.DQ_LLM_EXECUTION_LOG
ORDER BY CREATED_AT DESC
LIMIT 10;

-- Check success rate
SELECT
  EXECUTION_STATUS,
  COUNT(*) as count,
  AVG(TOTAL_EXECUTION_TIME_MS) as avg_time_ms
FROM DATA_QUALITY_DB.DB_METRICS.DQ_LLM_EXECUTION_LOG
WHERE CREATED_AT >= DATEADD('DAY', -1, CURRENT_TIMESTAMP())
GROUP BY EXECUTION_STATUS;
```

---

## 🔧 Configuration

### Environment Variables

Add to `.env.local`:
```env
# Ollama (optional, defaults shown)
OLLAMA_ENDPOINT=http://localhost:11434
OLLAMA_MODEL=mixtral:8x7b

# Snowflake (required)
SNOWFLAKE_ACCOUNT=your_account
SNOWFLAKE_USER=your_user
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_WAREHOUSE=your_warehouse
```

### Switch to Llama3 (Faster)

```env
OLLAMA_MODEL=llama3:8b
```

Then pull the model:
```bash
ollama pull llama3:8b
```

---

## 🐛 Troubleshooting

### Ollama Not Responding

**Symptom**: API returns `503 Service Unavailable`

**Fix**:
```bash
# Check if Ollama is running
curl http://localhost:11434/api/tags

# If not, start it:
ollama serve

# Check model is available:
ollama list
```

---

### Schema Validation Errors

**Symptom**: `VALIDATION_ERROR: Table not found in schema`

**Fix**:
1. Run schema registry notebook:
   ```bash
   cd antigravity/ide/notebooks
   jupyter notebook 01_schema_registry.ipynb
   # Run all cells
   ```

2. Verify tables exist:
   ```sql
   SELECT * FROM DATA_QUALITY_DB.INFORMATION_SCHEMA.TABLES
   WHERE TABLE_SCHEMA = 'DB_METRICS';
   ```

---

### Slow Response Times

**Symptom**: Queries take >60 seconds

**Solutions**:
1. **Switch to Llama3** (faster model):
   ```env
   OLLAMA_MODEL=llama3:8b
   ```

2. **Check Ollama resource usage**:
   ```bash
   ollama ps
   ```

3. **Reduce time windows** in questions:
   - Instead of: "How has quality changed this month?"
   - Ask: "How has quality changed this week?"

---

### Plan Generation Failures

**Symptom**: `PLAN_ERROR: Failed to generate execution plan`

**Common Causes**:
1. **Ollama timeout** - Increase timeout in `llm_router.ts` (default: 120s)
2. **Invalid JSON response** - Check Ollama logs: `ollama logs`
3. **Model not loaded** - Restart Ollama: `ollama serve`

**Debug**:
```sql
-- Check recent errors
SELECT
  USER_QUESTION,
  ERROR_MESSAGE,
  CREATED_AT
FROM DATA_QUALITY_DB.DB_METRICS.DQ_LLM_EXECUTION_LOG
WHERE EXECUTION_STATUS = 'PLAN_ERROR'
ORDER BY CREATED_AT DESC
LIMIT 5;
```

---

## 📊 Performance Expectations

### Mixtral 8x7b
- **Plan Generation**: 10-20 seconds
- **Interpretation**: 10-20 seconds
- **Total (with SQL)**: 20-40 seconds
- **Quality**: High accuracy, excellent reasoning
- **Size**: ~26GB

### Llama3 8b
- **Plan Generation**: 3-5 seconds
- **Interpretation**: 3-5 seconds
- **Total (with SQL)**: 6-15 seconds
- **Quality**: Good for standard questions
- **Size**: ~4.7GB

---

## ✅ Success Checklist

Before going to production, verify:

- [ ] Ollama is running: `curl http://localhost:11434/api/tags`
- [ ] Model is loaded: `ollama list` shows your model
- [ ] SQL tables exist: `DQ_LLM_EXECUTION_LOG` table created
- [ ] Health endpoint works: `/api/antigravity/health` returns `healthy`
- [ ] Test question succeeds: Ask a question in UI, get response
- [ ] Audit logs working: Check `DQ_LLM_EXECUTION_LOG` has entries
- [ ] No Gemini API calls: Check logs for absence of Gemini errors

---

## 🎯 Next Steps

### Optional Enhancements

1. **Create remaining notebooks**:
   - `02_llm_plan_generation.ipynb` - Test plan generation
   - `03_sql_execution.ipynb` - Test SQL building
   - `04_llm_interpretation.ipynb` - Test interpretation
   - `05_business_response.ipynb` - Test final assembly

2. **Frontend updates**:
   - Add "Powered by Local LLM" badge
   - Show execution plan preview
   - Add retry button for failures

3. **Monitoring dashboard**:
   - Create Streamlit/Grafana dashboard for audit logs
   - Track success rates, response times, common errors

---

## 📚 Documentation

- **Full Implementation Plan**: `implementation_plan.md`
- **Detailed Walkthrough**: `walkthrough.md`
- **Task Breakdown**: `task.md`
- **Antigravity README**: `antigravity/README.md`
- **Notebooks README**: `antigravity/ide/notebooks/README.md`

---

## 🆘 Support

If you encounter issues:

1. Check Ollama logs: `ollama logs`
2. Check execution logs in Snowflake: `SELECT * FROM DQ_LLM_EXECUTION_LOG`
3. Review walkthrough.md for detailed architecture
4. Test with notebooks for step-by-step debugging

---

**Status**: ✅ Ready for Testing
**Estimated Setup Time**: 30-45 minutes (including model download)
**Production Ready**: After successful testing
