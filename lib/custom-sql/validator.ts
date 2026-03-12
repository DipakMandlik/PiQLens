export type SqlMode = 'execute' | 'explain';

export type SqlCommandType =
  | 'SELECT'
  | 'WITH'
  | 'SHOW'
  | 'DESCRIBE'
  | 'EXPLAIN'
  | 'UPDATE'
  | 'DELETE'
  | 'UNKNOWN';

export interface ValidateSqlInput {
  sql: string;
  database: string;
  schema: string;
  table: string;
  isAdmin: boolean;
  mode: SqlMode;
}

export interface ValidateSqlOutput {
  compiledSql: string;
  commandType: SqlCommandType;
  appliedLimit: number | null;
  limitAdded: boolean;
  maxRows: number;
  timeoutSeconds: number;
  notices: string[];
  quotedTableFqn: string;
}

export class SqlValidationError extends Error {
  code: string;
  statusCode: number;

  constructor(message: string, code = 'VALIDATION_ERROR', statusCode = 400) {
    super(message);
    this.name = 'SqlValidationError';
    this.code = code;
    this.statusCode = statusCode;
  }
}

const NON_ADMIN_ALLOWED = new Set(['SELECT', 'WITH', 'SHOW', 'DESCRIBE', 'EXPLAIN']);
const ADMIN_ONLY_WRITES = new Set(['UPDATE', 'DELETE']);
const BLOCKED_FOR_ALL = new Set([
  'DROP',
  'TRUNCATE',
  'ALTER',
  'CREATE',
  'GRANT',
  'REVOKE',
  'CALL',
  'COPY',
  'PUT',
  'GET',
  'MERGE',
  'INSERT',
]);

function quoteIdentifier(identifier: string): string {
  return `"${identifier.replace(/"/g, '""')}"`;
}

function stripSqlCommentsAndStrings(sqlText: string): string {
  let output = '';
  let i = 0;

  while (i < sqlText.length) {
    const current = sqlText[i];
    const next = i + 1 < sqlText.length ? sqlText[i + 1] : '';

    if (current === '-' && next === '-') {
      i += 2;
      while (i < sqlText.length && sqlText[i] !== '\n') i += 1;
      output += ' ';
      continue;
    }

    if (current === '/' && next === '*') {
      i += 2;
      while (i + 1 < sqlText.length && !(sqlText[i] === '*' && sqlText[i + 1] === '/')) {
        i += 1;
      }
      i += 2;
      output += ' ';
      continue;
    }

    if (current === '\'') {
      i += 1;
      while (i < sqlText.length) {
        if (sqlText[i] === '\'' && sqlText[i + 1] === '\'') {
          i += 2;
          continue;
        }
        if (sqlText[i] === '\'') {
          i += 1;
          break;
        }
        i += 1;
      }
      output += ' ';
      continue;
    }

    output += current;
    i += 1;
  }

  return output;
}

function hasMultipleStatements(sqlText: string): boolean {
  const stripped = stripSqlCommentsAndStrings(sqlText);
  for (let i = 0; i < stripped.length; i += 1) {
    if (stripped[i] !== ';') continue;
    const remainder = stripped.slice(i + 1).trim();
    if (remainder.length > 0) {
      return true;
    }
  }
  return false;
}

function getFirstKeyword(sqlText: string): SqlCommandType {
  const normalizedSql = stripSqlCommentsAndStrings(sqlText).trim().toUpperCase();
  const matched = normalizedSql.match(/^([A-Z]+)/);
  if (!matched) return 'UNKNOWN';
  const token = matched[1];

  if (
    token === 'SELECT' ||
    token === 'WITH' ||
    token === 'SHOW' ||
    token === 'DESCRIBE' ||
    token === 'EXPLAIN' ||
    token === 'UPDATE' ||
    token === 'DELETE'
  ) {
    return token;
  }

  return 'UNKNOWN';
}

function assertNoBlockedKeywords(sqlText: string): void {
  const strippedUpper = stripSqlCommentsAndStrings(sqlText).toUpperCase();
  for (const keyword of BLOCKED_FOR_ALL) {
    const pattern = new RegExp(`\\b${keyword}\\b`, 'i');
    if (pattern.test(strippedUpper)) {
      throw new SqlValidationError(
        `Command "${keyword}" is not allowed in Custom SQL workbench.`,
        'BLOCKED_COMMAND',
        403
      );
    }
  }
}

function validateCommandAuthorization(commandType: SqlCommandType, isAdmin: boolean): void {
  if (commandType === 'UNKNOWN') {
    throw new SqlValidationError('Unable to determine SQL command type.', 'UNKNOWN_COMMAND', 400);
  }

  if (NON_ADMIN_ALLOWED.has(commandType)) {
    return;
  }

  if (ADMIN_ONLY_WRITES.has(commandType) && !isAdmin) {
    throw new SqlValidationError(
      `Command "${commandType}" is restricted to admin roles.`,
      'ADMIN_ONLY_COMMAND',
      403
    );
  }

  if (!NON_ADMIN_ALLOWED.has(commandType) && !ADMIN_ONLY_WRITES.has(commandType)) {
    throw new SqlValidationError(`Command "${commandType}" is not allowed.`, 'COMMAND_NOT_ALLOWED', 403);
  }
}

function normalizeTableReference(reference: string): string {
  return reference
    .trim()
    .replace(/;$/, '')
    .replace(/\s+AS\s+[A-Z0-9_"$]+$/i, '')
    .replace(/\s+[A-Z0-9_"$]+$/i, '')
    .trim()
    .toUpperCase();
}

function extractCteNames(sqlText: string): Set<string> {
  const stripped = stripSqlCommentsAndStrings(sqlText);
  if (!/^\s*WITH\b/i.test(stripped)) {
    return new Set<string>();
  }

  const cteNames = new Set<string>();
  const ctePattern = /\b([A-Z0-9_$"]+)\s+AS\s*\(/gi;
  let match = ctePattern.exec(stripped);

  while (match) {
    const normalized = normalizeTableReference(match[1]);
    if (normalized) {
      cteNames.add(normalized);

      const unquoted = normalized.replace(/^"(.+)"$/, '$1').replace(/""/g, '"');
      if (unquoted) {
        cteNames.add(unquoted.toUpperCase());
      }
    }
    match = ctePattern.exec(stripped);
  }

  return cteNames;
}

function assertTableScopeForNonAdmin(
  sqlText: string,
  commandType: SqlCommandType,
  database: string,
  schema: string,
  table: string,
  quotedTableFqn: string
): void {
  if (commandType === 'SHOW' || commandType === 'DESCRIBE' || commandType === 'EXPLAIN') {
    return;
  }

  const stripped = stripSqlCommentsAndStrings(sqlText);
  const strippedUpper = stripped.toUpperCase();
  const cteNames = extractCteNames(stripped);

  const allowed = new Set([
    quotedTableFqn.toUpperCase(),
    `${database}.${schema}.${table}`.toUpperCase(),
    `${schema}.${table}`.toUpperCase(),
    table.toUpperCase(),
  ]);
  for (const cteName of cteNames) {
    allowed.add(cteName);
  }

  const referencePattern = /\b(?:FROM|JOIN|UPDATE|INTO|DELETE\s+FROM)\s+([^\s,()]+)/gi;
  const references: string[] = [];
  let match = referencePattern.exec(stripped);

  while (match) {
    references.push(normalizeTableReference(match[1]));
    match = referencePattern.exec(stripped);
  }

  if (references.length === 0 && !strippedUpper.includes(quotedTableFqn.toUpperCase())) {
    throw new SqlValidationError(
      'Query must target the current table context using {{TABLE}}.',
      'TABLE_SCOPE_REQUIRED',
      403
    );
  }

  const disallowed = references.filter((ref) => !allowed.has(ref));
  if (disallowed.length > 0) {
    throw new SqlValidationError(
      `Non-admin queries can only target ${database}.${schema}.${table}.`,
      'TABLE_SCOPE_VIOLATION',
      403
    );
  }
}

function findLimit(sqlText: string): number | null {
  const stripped = stripSqlCommentsAndStrings(sqlText);
  const match = stripped.match(/\bLIMIT\s+(\d+)\b/i);
  if (!match) return null;
  const value = Number(match[1]);
  return Number.isFinite(value) ? value : null;
}

function removeTrailingSemicolon(sqlText: string): string {
  return sqlText.trim().replace(/;+\s*$/g, '').trim();
}

export function validateAndCompileSql(input: ValidateSqlInput): ValidateSqlOutput {
  const rawSql = input.sql?.trim() || '';
  if (!rawSql) {
    throw new SqlValidationError('SQL is required.', 'MISSING_SQL', 400);
  }

  if (hasMultipleStatements(rawSql)) {
    throw new SqlValidationError(
      'Only a single SQL statement is allowed per execution.',
      'MULTI_STATEMENT_BLOCKED',
      400
    );
  }

  assertNoBlockedKeywords(rawSql);

  let commandType = getFirstKeyword(rawSql);
  validateCommandAuthorization(commandType, input.isAdmin);

  const quotedTableFqn = `${quoteIdentifier(input.database)}.${quoteIdentifier(input.schema)}.${quoteIdentifier(input.table)}`;
  let compiledSql = rawSql.replace(/\{\{\s*TABLE\s*\}\}/gi, quotedTableFqn);
  compiledSql = removeTrailingSemicolon(compiledSql);

  if (commandType === 'WITH') {
    const strippedUpper = stripSqlCommentsAndStrings(compiledSql).toUpperCase();
    if (!/\bSELECT\b/.test(strippedUpper)) {
      throw new SqlValidationError('WITH statements must resolve to a SELECT query.', 'WITH_NOT_SELECT', 400);
    }

    if (/\b(UPDATE|DELETE|INSERT|MERGE|CREATE|ALTER|DROP|TRUNCATE|CALL|COPY|PUT|GET|GRANT|REVOKE)\b/.test(strippedUpper)) {
      throw new SqlValidationError('WITH statement includes disallowed write/DDL operations.', 'WITH_NOT_READ_ONLY', 403);
    }
  }

  if (!input.isAdmin) {
    assertTableScopeForNonAdmin(
      compiledSql,
      commandType,
      input.database,
      input.schema,
      input.table,
      quotedTableFqn
    );
  }

  const maxRows = input.isAdmin ? 5000 : 1000;
  const defaultResultLimit = 100;
  const timeoutSeconds = input.isAdmin ? 120 : 30;
  const notices: string[] = [];
  let appliedLimit: number | null = null;
  let limitAdded = false;

  if (commandType === 'SELECT' || commandType === 'WITH') {
    const existingLimit = findLimit(compiledSql);

    if (existingLimit !== null && existingLimit > maxRows) {
      throw new SqlValidationError(
        `LIMIT ${existingLimit} exceeds maximum allowed ${maxRows} rows.`,
        'LIMIT_EXCEEDS_CAP',
        400
      );
    }

    if (existingLimit === null) {
      compiledSql = `${compiledSql}\nLIMIT ${defaultResultLimit}`;
      appliedLimit = defaultResultLimit;
      limitAdded = true;
      notices.push(`Result limited to ${defaultResultLimit} rows.`);
    } else {
      appliedLimit = existingLimit;
    }

  }
  if (input.mode === 'explain') {
    const explainFirst = getFirstKeyword(compiledSql);
    if (explainFirst !== 'EXPLAIN') {
      compiledSql = `EXPLAIN USING TEXT ${compiledSql}`;
      commandType = 'EXPLAIN';
      notices.push('Explain plan generated for compiled query.');
    }
  }

  return {
    compiledSql,
    commandType,
    appliedLimit,
    limitAdded,
    maxRows,
    timeoutSeconds,
    notices,
    quotedTableFqn,
  };
}



