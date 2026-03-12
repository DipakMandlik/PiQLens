import type { SnowflakeConfig } from '@/lib/snowflake';

const DEFAULT_ADMIN_ROLES = ['ACCOUNTADMIN', 'SECURITYADMIN'];
const DEFAULT_DATA_ENGINEER_ROLES = ['DATA_ENGINEER_ROLE'];
const DEFAULT_ANALYST_ROLES = ['ANALYST_ROLE'];
const DEFAULT_VIEWER_ROLES = ['VIEWER_ROLE'];

export type CustomSqlAppRole = 'ADMIN' | 'DATA_ENGINEER' | 'ANALYST' | 'VIEWER';

export interface CustomSqlPermissions {
  canRunSql: boolean;
  canEditSql: boolean;
  canViewHistory: boolean;
  canConfigureDataset: boolean;
  canManageUsers: boolean;
  allowedCommands: string[];
}

export interface CustomSqlAccess {
  appRole: CustomSqlAppRole;
  permissions: CustomSqlPermissions;
}

function normalizeRole(value: string): string {
  return value.trim().toUpperCase();
}

function parseRoleList(raw: string | undefined, fallback: string[]): Set<string> {
  if (!raw || !raw.trim()) {
    return new Set(fallback.map(normalizeRole));
  }

  const parsed = raw
    .split(',')
    .map(normalizeRole)
    .filter((item) => item.length > 0);

  return new Set(parsed.length > 0 ? parsed : fallback.map(normalizeRole));
}

export function getAdminRoleAllowlist(): Set<string> {
  return parseRoleList(process.env.CUSTOM_SQL_ADMIN_ROLES, DEFAULT_ADMIN_ROLES);
}

export function getDataEngineerRoleAllowlist(): Set<string> {
  return parseRoleList(process.env.CUSTOM_SQL_DATA_ENGINEER_ROLES, DEFAULT_DATA_ENGINEER_ROLES);
}

export function getAnalystRoleAllowlist(): Set<string> {
  return parseRoleList(process.env.CUSTOM_SQL_ANALYST_ROLES, DEFAULT_ANALYST_ROLES);
}

export function getViewerRoleAllowlist(): Set<string> {
  return parseRoleList(process.env.CUSTOM_SQL_VIEWER_ROLES, DEFAULT_VIEWER_ROLES);
}

export function isAdminRole(role: string | null | undefined): boolean {
  if (!role) return false;
  const allowlist = getAdminRoleAllowlist();
  return allowlist.has(normalizeRole(role));
}

export function resolveCustomSqlRole(role: string | null | undefined): CustomSqlAppRole {
  const normalized = normalizeRole(role || '');

  if (!normalized) return 'VIEWER';
  if (getAdminRoleAllowlist().has(normalized)) return 'ADMIN';
  if (getDataEngineerRoleAllowlist().has(normalized)) return 'DATA_ENGINEER';
  if (getAnalystRoleAllowlist().has(normalized)) return 'ANALYST';
  if (getViewerRoleAllowlist().has(normalized)) return 'VIEWER';

  return 'VIEWER';
}

export function getCustomSqlAccess(role: string | null | undefined): CustomSqlAccess {
  const appRole = resolveCustomSqlRole(role);

  if (appRole === 'ADMIN') {
    return {
      appRole,
      permissions: {
        canRunSql: true,
        canEditSql: true,
        canViewHistory: true,
        canConfigureDataset: true,
        canManageUsers: true,
        allowedCommands: ['SELECT', 'WITH', 'SHOW', 'DESCRIBE', 'UPDATE', 'DELETE'],
      },
    };
  }

  if (appRole === 'DATA_ENGINEER') {
    return {
      appRole,
      permissions: {
        canRunSql: true,
        canEditSql: true,
        canViewHistory: true,
        canConfigureDataset: false,
        canManageUsers: false,
        allowedCommands: ['SELECT', 'WITH', 'SHOW', 'DESCRIBE'],
      },
    };
  }

  if (appRole === 'ANALYST') {
    return {
      appRole,
      permissions: {
        canRunSql: true,
        canEditSql: true,
        canViewHistory: true,
        canConfigureDataset: false,
        canManageUsers: false,
        allowedCommands: ['SELECT', 'WITH'],
      },
    };
  }

  return {
    appRole: 'VIEWER',
    permissions: {
      canRunSql: false,
      canEditSql: false,
      canViewHistory: false,
      canConfigureDataset: false,
      canManageUsers: false,
      allowedCommands: [],
    },
  };
}

export function isAdminFromConfig(config: Pick<SnowflakeConfig, 'role'> | null): boolean {
  if (!config) return false;
  return resolveCustomSqlRole(config.role) === 'ADMIN';
}

