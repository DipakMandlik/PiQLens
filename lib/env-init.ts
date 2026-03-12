/**
 * Environment Initialization
 * Validates environment on application startup
 */

import { validateEnvironment, logValidationResults } from './env-validator';

/**
 * Validate environment on startup
 * Call this in your main entry point
 */
export function initializeEnvironment(): void {
  const result = validateEnvironment();
  logValidationResults(result);

  // In development, show warnings but don't block
  // In production, block startup if errors exist
  if (!result.isValid) {
    if (process.env.NODE_ENV === 'production') {
      console.error('\n❌ STARTUP FAILED: Invalid environment configuration');
      console.error('Please fix the errors above and restart the application.\n');
      process.exit(1);
    } else {
      console.warn('\n⚠️  DEVELOPMENT MODE: Errors detected but continuing...\n');
    }
  }
}
