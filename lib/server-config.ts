/**
 * Server-side configuration storage
 * Stores Snowflake config with environment fallback for persistence
 */
import { SnowflakeConfig } from './snowflake';
import { logger } from './logger';

// In-memory storage for server-side config
let serverConfig: SnowflakeConfig | null = null;

/**
 * Get current server config
 * Falls back to environment variables if in-memory config is not set
 */
export function getServerConfig(): SnowflakeConfig | null {
  // Return in-memory config if available
  if (serverConfig) {
    return serverConfig;
  }

  // Fallback to environment variables
  const hasBaseConfig = !!(process.env.SNOWFLAKE_ACCOUNT && process.env.SNOWFLAKE_USER);
  const hasPasswordAuth = !!process.env.SNOWFLAKE_PASSWORD;
  const hasKeyPairAuth = !!process.env.SNOWFLAKE_PRIVATE_KEY_PATH;

  if (hasBaseConfig && (hasPasswordAuth || hasKeyPairAuth)) {
    const account = process.env.SNOWFLAKE_ACCOUNT;
    const username = process.env.SNOWFLAKE_USER;

    if (!account || !username) {
      return null;
    }

    return {
      account,
      username,
      password: process.env.SNOWFLAKE_PASSWORD,
      privateKeyPath: process.env.SNOWFLAKE_PRIVATE_KEY_PATH,
      privateKeyPassphrase: process.env.SNOWFLAKE_PRIVATE_KEY_PASSPHRASE || process.env.SNOWFLAKE_PRIVATE_KEY_PASS,
      publicKeyFingerprint: process.env.SNOWFLAKE_PUBLIC_KEY_FINGERPRINT,
      warehouse: process.env.SNOWFLAKE_WAREHOUSE || 'COMPUTE_WH',
      database: process.env.SNOWFLAKE_DATABASE || 'BANKING_DW',
      schema: process.env.SNOWFLAKE_SCHEMA || 'BRONZE',
    };
  }

  return null;
}

/**
 * Set server config (stores in memory)
 */
export function setServerConfig(config: SnowflakeConfig | null): void {
  serverConfig = config;
  if (config) {
    logger.info('Server config updated: Snowflake connection configured');
  } else {
    logger.info('Server config cleared');
  }
}

/**
 * Check if server config is available (from memory or env)
 */
export function hasServerConfig(): boolean {
  const config = getServerConfig();
  return config !== null;
}

/**
 * Get config source for logging/debugging
 */
export function getConfigSource(): 'memory' | 'environment' | 'none' {
  if (serverConfig) return 'memory';

  const fromEnv = getServerConfig();
  if (fromEnv && fromEnv === getServerConfig()) return 'environment';

  return 'none';
}

