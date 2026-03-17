// Lazy-load snowflake-sdk at runtime to avoid bundler/Turbopack issues
let snowflake: any = null;
import { existsSync } from 'fs';

import { logger } from './logger';
import { SnowflakeConfig, QueryResult } from './snowflake-types';
export type { SnowflakeConfig, QueryResult };
function getSessionTimezoneOverride(): string | null {
  const value = (process.env.SNOWFLAKE_SESSION_TIMEZONE || '').trim();
  return value || null;
}

function escapeSqlLiteral(value: string): string {
  return value.replace(/'/g, "''");
}




export function extractAccountFromUrl(accountUrl: string): string {
  if (!accountUrl) return '';

  // Handle different URL formats:
  // - https://xyz123.snowflakecomputing.com
  // - https://xyz123.us-east-1.snowflakecomputing.com
  // - xyz123.snowflakecomputing.com
  // - xyz123.us-east-1
  // - xyz123

  let account = accountUrl.trim();

  // Remove protocol if present
  account = account.replace(/^https?:\/\//, '');

  // Remove .snowflakecomputing.com and everything after
  account = account.replace(/\.snowflakecomputing\.com.*$/, '');

  // If it still contains a dot, it might be in format: account.region
  // Snowflake SDK accepts both formats, but prefers account identifier without region
  // However, if region is included (e.g., xyz123.us-east-1), we should keep it

  // Clean up any trailing slashes or paths
  account = account.replace(/\/.*$/, '');

  // Remove any query parameters or fragments
  account = account.split('?')[0].split('#')[0];

  return account;
}



/**
 * Creates a Snowflake connection using the provided configuration
 */
export async function createSnowflakeConnection(config: SnowflakeConfig): Promise<any> {
  if (!snowflake) {
    // use dynamic import so bundlers don't attempt to resolve native modules at compile time
    snowflake = await import('snowflake-sdk');
    // default export fallback
    snowflake = snowflake?.default || snowflake;
  }

  return new Promise((resolve, reject) => {
    // Extract account from URL if accountUrl is provided
    let account = config.account;
    if (!account && config.accountUrl) {
      account = extractAccountFromUrl(config.accountUrl);
      logger.debug(`Extracted account from URL: ${JSON.stringify({ accountUrl: config.accountUrl, extractedAccount: account })}`);
    }

    if (!account || account.trim() === '') {
      reject(new Error('Account is required. Provide either account or accountUrl.'));
      return;
    }

    // Validate account format
    // Snowflake account formats:
    // - Simple: xyz123
    // - With region: xyz123.us-east-1
    // - Organization-Account: UXEQGOS-NP89851
    // - Organization-Account with region: UXEQGOS-NP89851.us-east-1
    // Account can contain letters, numbers, and hyphens
    const accountPattern = /^[a-zA-Z0-9-]+(\.[a-zA-Z0-9-]+)?$/;
    if (!accountPattern.test(account)) {
      logger.error(`Invalid account format: ${account}`);
      reject(new Error(`Invalid account format: "${account}". Account should be like "xyz123", "UXEQGOS-NP89851", or "xyz123.us-east-1".`));
      return;
    }

    // Auth options:
    // 1) token/password flow (backward compatible)
    // 2) private key path flow (for Snowflake key-pair auth)
    const password = config.token || config.password;
    const hasPasswordAuth = !!password;
    const hasKeyPairAuth = !!config.privateKeyPath;

    if (!hasPasswordAuth && !hasKeyPairAuth) {
      reject(new Error('Authentication requires either password/token or privateKeyPath.'));
      return;
    }

    const connectionOptions: any = {
      account,
      username: config.username,
      warehouse: config.warehouse,
      database: config.database,
      schema: config.schema,
      role: config.role,
    };

    if (hasKeyPairAuth) {
      const privateKeyPath = config.privateKeyPath;
      if (!privateKeyPath || !existsSync(privateKeyPath)) {
        reject(new Error(`Private key file not found at path: ${privateKeyPath || '(empty)'}`));
        return;
      }

      connectionOptions.authenticator = 'SNOWFLAKE_JWT';
      connectionOptions.privateKeyPath = privateKeyPath;
      if (config.privateKeyPassphrase) {
        connectionOptions.privateKeyPass = config.privateKeyPassphrase;
      }
    } else {
      connectionOptions.authenticator = 'SNOWFLAKE';
      connectionOptions.password = password;
    }

    const connection = snowflake.createConnection(connectionOptions);

    connection.connect((err: any, conn: any) => {
      if (err) {
        // Enhanced error logging with detailed context
        logger.error('Failed to connect to Snowflake', {
          message: err.message,
          code: err.code,
          sqlState: err.sqlState,
          account,
          username: config.username,
        });

        // Preserve the original error object for proper error propagation
        reject(err);
      } else {
        logger.info('Successfully connected to Snowflake');

        // Set warehouse, database, and schema explicitly using connection.execute
        const setupCommands: string[] = [];

        if (config.warehouse) {
          setupCommands.push(`USE WAREHOUSE ${config.warehouse}`);
        }
        if (config.database) {
          setupCommands.push(`USE DATABASE ${config.database}`);
        }
        if (config.schema) {
          setupCommands.push(`USE SCHEMA ${config.schema}`);
        }
        if (config.role) {
          setupCommands.push(`USE ROLE ${config.role}`);
        }
        const sessionTimezone = getSessionTimezoneOverride();
        if (sessionTimezone) {
          setupCommands.push(`ALTER SESSION SET TIMEZONE = '${escapeSqlLiteral(sessionTimezone)}'`);
        }

        // Execute setup commands sequentially
        if (setupCommands.length > 0) {
          let commandIndex = 0;

          const executeNextCommand = () => {
            if (commandIndex >= setupCommands.length) {
              resolve(conn);
              return;
            }

            conn.execute({
              sqlText: setupCommands[commandIndex],
              complete: (err: any, stmt: any) => {
                if (err) {
                  logger.error(`Error executing setup command "${setupCommands[commandIndex]}"`, err);
                  // Continue with next command even if one fails
                } else {
                  logger.debug(`Executed: ${setupCommands[commandIndex]}`);
                }
                commandIndex++;
                executeNextCommand();
              },
            });
          };

          executeNextCommand();
        } else {
          resolve(conn);
        }
      }
    });
  });
}

/**
 * Ensures the connection context (warehouse, database, schema) is set
 */
export async function ensureConnectionContext(
  connection: any, // snowflake.Connection
  config: SnowflakeConfig
): Promise<void> {
  return new Promise((resolve, reject) => {
    const commands: string[] = [];

    if (config.warehouse) {
      commands.push(`USE WAREHOUSE ${config.warehouse}`);
    }
    if (config.database) {
      commands.push(`USE DATABASE ${config.database}`);
    }
    if (config.schema) {
      commands.push(`USE SCHEMA ${config.schema}`);
    }
    if (config.role) {
      commands.push(`USE ROLE ${config.role}`);
    }
    const sessionTimezone = getSessionTimezoneOverride();
    if (sessionTimezone) {
      commands.push(`ALTER SESSION SET TIMEZONE = '${escapeSqlLiteral(sessionTimezone)}'`);
    }

    if (commands.length === 0) {
      resolve();
      return;
    }

    let commandIndex = 0;

    const executeNext = () => {
      if (commandIndex >= commands.length) {
        resolve();
        return;
      }

      connection.execute({
        sqlText: commands[commandIndex],
        complete: (err: any) => {
          if (err) {
            logger.error(`Error setting context "${commands[commandIndex]}"`, err);
            // Continue anyway - might already be set
          }
          commandIndex++;
          executeNext();
        },
      });
    };

    executeNext();
  });
}

/**
 * Executes a SQL query against Snowflake and returns the results formatted as columns and rows
 */
export function executeQuery(
  connection: any, // snowflake.Connection
  sqlText: string,
  binds?: any[] // Optional bind parameters
): Promise<QueryResult> {
  return new Promise((resolve, reject) => {
    connection.execute({
      sqlText,
      binds, // Pass binds to snowflake driver
      complete: (err: any, stmt: any, rows: any[]) => {
        if (err) {
          logger.error('Failed to execute query', err);
          reject(err);
        } else if (!stmt) {
          reject(new Error('Statement is undefined'));
        } else {
          // Get column metadata
          const columns = (stmt.getColumns() || []).map((col: any) => col.getName());

          // Convert rows to array format
          const rowArray = rows?.map((row: any) => {
            return columns.map((col: string) => row[col]);
          }) || [];

          resolve({
            columns,
            rows: rowArray,
            rowCount: rowArray.length,
          });
        }
      },
    });
  });
}

/**
 * Executes a SQL query and returns rows as objects (key-value pairs)
 * This is preferred when you need to access columns by name
 */
export function executeQueryObjects(
  connection: any, // snowflake.Connection
  sqlText: string,
  binds?: any[]
): Promise<any[]> {
  return new Promise((resolve, reject) => {
    connection.execute({
      sqlText,
      binds,
      complete: (err: any, stmt: any, rows: any[]) => {
        if (err) {
          logger.error('Failed to execute query objects', err);
          reject(err);
        } else {
          resolve(rows || []);
        }
      },
    });
  });
}

/**
 * Gets Snowflake configuration from environment variables
 */
export function getSnowflakeConfigFromEnv(): SnowflakeConfig {
  const account = process.env.SNOWFLAKE_ACCOUNT;
  const username = process.env.SNOWFLAKE_USER || process.env.SNOWFLAKE_USERNAME; // Support both naming conventions
  const password = process.env.SNOWFLAKE_PASSWORD;
  const privateKeyPath = process.env.SNOWFLAKE_PRIVATE_KEY_PATH;
  const privateKeyPassphrase = process.env.SNOWFLAKE_PRIVATE_KEY_PASSPHRASE || process.env.SNOWFLAKE_PRIVATE_KEY_PASS;
  const publicKeyFingerprint = process.env.SNOWFLAKE_PUBLIC_KEY_FINGERPRINT;
  const warehouse = process.env.SNOWFLAKE_WAREHOUSE;
  const database = process.env.SNOWFLAKE_DATABASE;
  const schema = process.env.SNOWFLAKE_SCHEMA;
  const role = process.env.SNOWFLAKE_ROLE;
  const token = process.env.SNOWFLAKE_TOKEN;

  const hasPasswordAuth = !!(token || password);
  const hasKeyPairAuth = !!privateKeyPath;

  if (!account || !username || !warehouse || !database || !schema || (!hasPasswordAuth && !hasKeyPairAuth)) {
    throw new Error(
      'Missing required Snowflake environment variables. Set account/user/context and one auth method: password/token or private key path.'
    );
  }

  return {
    account,
    username,
    password,
    token,
    privateKeyPath,
    privateKeyPassphrase,
    publicKeyFingerprint,
    warehouse,
    database,
    schema,
    role,
  };
}

/**
 * Connection pool manager for reusing connections
 */
class SnowflakeConnectionPool {
  private connection: any = null;
  private config: SnowflakeConfig | null = null;

  async getConnection(config?: SnowflakeConfig): Promise<any> {
    // If no config provided, try to use existing config or env vars
    if (!config) {
      if (this.config) {
        // Use existing stored config
        config = this.config;
      } else {
        try {
          config = getSnowflakeConfigFromEnv();
        } catch (error) {
          throw new Error('No Snowflake configuration provided and environment variables are not set.');
        }
      }
    }

    // Check if we need a new connection (different config or no connection)
    const needsNewConnection =
      !this.connection ||
      !this.config ||
      JSON.stringify(this.config) !== JSON.stringify(config);

    if (!needsNewConnection && this.connection) {
      // Check if existing connection is still valid
      try {
        await executeQuery(this.connection, 'SELECT 1');
        return this.connection;
      } catch (error) {
        // Connection is dead, create a new one
        this.connection = null;
        this.config = null;
      }
    }

    // Close existing connection if we're switching configs
    if (this.connection && needsNewConnection) {
      try {
        await this.closeConnection();
      } catch (error) {
        // Ignore errors when closing
      }
    }

    this.config = config;
    this.connection = await createSnowflakeConnection(config);
    return this.connection;
  }

  async closeConnection(): Promise<void> {
    if (this.connection) {
      return new Promise((resolve, reject) => {
        this.connection!.destroy((err: any) => {
          if (err) {
            reject(err);
          } else {
            this.connection = null;
            this.config = null;
            resolve();
          }
        });
      });
    }
  }

  getCurrentConfig(): SnowflakeConfig | null {
    return this.config;
  }

  hasConnection(): boolean {
    return this.connection !== null;
  }
}

// Singleton instance
export const snowflakePool = new SnowflakeConnectionPool();

