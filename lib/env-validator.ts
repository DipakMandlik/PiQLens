/**
 * Environment Variable Validator
 * Validates required and optional environment variables with helpful error messages
 */

export interface EnvValidationResult {
  isValid: boolean;
  errors: string[];
  warnings: string[];
  config: {
    snowflake: {
      account?: string;
      user?: string;
      warehouse?: string;
      database?: string;
      schema?: string;
      dqDatabase?: string;
    };
    ai: {
      geminiApiKey?: string;
      ollamaUrl?: string;
    };
    app: {
      nodeEnv: string;
      port: number;
    };
  };
}

const REQUIRED_SNOWFLAKE_VARS = [
  'SNOWFLAKE_ACCOUNT',
  'SNOWFLAKE_USER',
  'SNOWFLAKE_PASSWORD'
];

const OPTIONAL_SNOWFLAKE_VARS = [
  'SNOWFLAKE_WAREHOUSE',
  'SNOWFLAKE_DATABASE',
  'SNOWFLAKE_SCHEMA',
  'DQ_DATABASE'
];

const AI_VARS = [
  'GEMINI_API_KEY',
  'OLLAMA_BASE_URL'
];

/**
 * Validate all environment variables
 */
export function validateEnvironment(): EnvValidationResult {
  const errors: string[] = [];
  const warnings: string[] = [];

  // Check required Snowflake variables
  for (const varName of REQUIRED_SNOWFLAKE_VARS) {
    if (!process.env[varName]) {
      errors.push(`❌ Missing required environment variable: ${varName}`);
    }
  }

  // Check optional Snowflake variables with warnings
  for (const varName of OPTIONAL_SNOWFLAKE_VARS) {
    if (!process.env[varName]) {
      const displayName = varName === 'DQ_DATABASE' ? 'DQ_DATABASE' : varName;
      const defaultValue = getDefaultValue(varName);
      warnings.push(`⚠️  ${displayName} not set (default: ${defaultValue})`);
    }
  }

  // Check AI configuration
  const hasGeminiKey = !!process.env.GEMINI_API_KEY;
  const hasOllamaUrl = !!process.env.OLLAMA_BASE_URL;

  if (!hasGeminiKey && !hasOllamaUrl) {
    warnings.push('⚠️  No AI provider configured (GEMINI_API_KEY or OLLAMA_BASE_URL). Antigravity features will be limited.');
  }

  if (!hasGeminiKey) {
    warnings.push('⚠️  GEMINI_API_KEY not set. Gemini AI insights will not be available.');
  }

  if (!hasOllamaUrl) {
    warnings.push('⚠️  OLLAMA_BASE_URL not set (default: http://localhost:11434). Make sure Ollama is running.');
  }

  // Build config object
  const config: EnvValidationResult['config'] = {
    snowflake: {
      account: process.env.SNOWFLAKE_ACCOUNT || '(missing)',
      user: process.env.SNOWFLAKE_USER || '(missing)',
      warehouse: process.env.SNOWFLAKE_WAREHOUSE || 'COMPUTE_WH',
      database: process.env.SNOWFLAKE_DATABASE || 'BANKING_DW',
      schema: process.env.SNOWFLAKE_SCHEMA || 'BRONZE',
      dqDatabase: process.env.DQ_DATABASE || 'DATA_QUALITY_DB'
    },
    ai: {
      geminiApiKey: hasGeminiKey ? '(configured)' : '(not set)',
      ollamaUrl: process.env.OLLAMA_BASE_URL || 'http://localhost:11434'
    },
    app: {
      nodeEnv: process.env.NODE_ENV || 'development',
      port: parseInt(process.env.PORT || '3000')
    }
  };

  return {
    isValid: errors.length === 0,
    errors,
    warnings,
    config
  };
}

/**
 * Log validation results
 */
export function logValidationResults(result: EnvValidationResult): void {
  console.log('\n' + '='.repeat(60));
  console.log('🔐 ENVIRONMENT VALIDATION RESULTS');
  console.log('='.repeat(60) + '\n');

  if (result.isValid) {
    console.log('✅ All required environment variables are configured\n');
  } else {
    console.log('❌ CONFIGURATION ERRORS:\n');
    result.errors.forEach(error => {
      console.log(`   ${error}`);
    });
    console.log();
  }

  if (result.warnings.length > 0) {
    console.log('⚠️  WARNINGS:\n');
    result.warnings.forEach(warning => {
      console.log(`   ${warning}`);
    });
    console.log();
  }

  console.log('📋 CURRENT CONFIGURATION:');
  console.log('\nSnowflake:');
  console.log(`   Account: ${result.config.snowflake.account}`);
  console.log(`   User: ${result.config.snowflake.user}`);
  console.log(`   Warehouse: ${result.config.snowflake.warehouse}`);
  console.log(`   Database: ${result.config.snowflake.database}`);
  console.log(`   Schema: ${result.config.snowflake.schema}`);
  console.log(`   DQ Database: ${result.config.snowflake.dqDatabase}`);

  console.log('\nAI Configuration:');
  console.log(`   Gemini API Key: ${result.config.ai.geminiApiKey}`);
  console.log(`   Ollama URL: ${result.config.ai.ollamaUrl}`);

  console.log('\nApplication:');
  console.log(`   Node Env: ${result.config.app.nodeEnv}`);
  console.log(`   Port: ${result.config.app.port}`);

  console.log('\n' + '='.repeat(60) + '\n');

  if (!result.isValid) {
    console.log('❌ STARTUP BLOCKED: Please fix the configuration errors above.\n');
    console.log('📖 See docs/ENVIRONMENT_SETUP.md for detailed instructions.\n');
  }
}

/**
 * Get default value for optional variable
 */
function getDefaultValue(varName: string): string {
  const defaults: { [key: string]: string } = {
    'SNOWFLAKE_WAREHOUSE': 'COMPUTE_WH',
    'SNOWFLAKE_DATABASE': 'BANKING_DW',
    'SNOWFLAKE_SCHEMA': 'BRONZE',
    'DQ_DATABASE': 'DATA_QUALITY_DB',
    'OLLAMA_BASE_URL': 'http://localhost:11434'
  };
  return defaults[varName] || '(dynamic)';
}

/**
 * Require valid environment (throws error if invalid)
 */
export function requireValidEnvironment(): void {
  const result = validateEnvironment();
  if (!result.isValid) {
    logValidationResults(result);
    throw new Error('Invalid environment configuration. See logs above.');
  }
}
