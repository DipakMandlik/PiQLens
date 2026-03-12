# Antigravity Engine

This directory contains the core execution engines for Antigravity's local LLM-powered observability.

## Modules

- **llm_router.ts** - Ollama API client and model routing
- **plan_validator.ts** - Schema validation and plan verification
- **sql_builder.ts** - Deterministic SQL generation from validated plans
- **execution_engine.ts** - SQL execution and result handling
- **interpretation_engine.ts** - LLM-powered business insight generation
- **audit_logger.ts** - Execution logging and learning loop

## Architecture

```
User Question
     ↓
LLM Router (Ollama) → Generate Plan
     ↓
Plan Validator → Validate against Schema Registry
     ↓
SQL Builder → Generate deterministic SQL
     ↓
Execution Engine → Execute on Snowflake
     ↓
Interpretation Engine (Ollama) → Generate business insights
     ↓
Audit Logger → Log for learning
```

## Key Principles

1. **No Hallucinations**: All plans validated against schema registry
2. **Deterministic SQL**: LLM generates plans, not SQL syntax
3. **Loud Failures**: All errors logged and surfaced to UI
4. **Business-First**: Insights in business language, not technical jargon
