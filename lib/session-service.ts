import {
    getCache,
    setCache,
    deleteCache,
    CachePrefix,
    CacheTTL,
    generateCacheKey,
    extendCacheTTL,
} from './cache-service';

/**
 * User session data structure
 */
export interface UserSession {
    userId: string;
    projectId?: string;
    lastActiveRoute?: string;
    preferences?: Record<string, unknown>;
    createdAt: number;
    lastAccessedAt: number;
}

/**
 * Page state data structure
 */
export interface PageState {
    route: string;
    filters?: Record<string, unknown>;
    scrollPosition?: number;
    selectedItems?: string[];
    viewMode?: string;
    timestamp: number;
}

/**
 * Get user session from cache
 */
export async function getUserSession(userId: string): Promise<UserSession | null> {
    const key = generateCacheKey(CachePrefix.USER_SESSION, userId);
    const session = await getCache<UserSession>(key);

    // Extend TTL on access (sliding expiration)
    if (session) {
        await extendCacheTTL(key, CacheTTL.USER_SESSION);
    }

    return session;
}

/**
 * Set user session in cache
 */
export async function setUserSession(session: UserSession): Promise<boolean> {
    const key = generateCacheKey(CachePrefix.USER_SESSION, session.userId);

    // Update last accessed timestamp
    const updatedSession: UserSession = {
        ...session,
        lastAccessedAt: Date.now(),
    };

    return setCache(key, updatedSession, CacheTTL.USER_SESSION);
}

/**
 * Update user session partially
 */
export async function updateUserSession(
    userId: string,
    updates: Partial<Omit<UserSession, 'userId' | 'createdAt'>>
): Promise<boolean> {
    const existing = await getUserSession(userId);

    if (!existing) {
        // Create new session if doesn't exist
        const newSession: UserSession = {
            userId,
            ...updates,
            createdAt: Date.now(),
            lastAccessedAt: Date.now(),
        };
        return setUserSession(newSession);
    }

    // Update existing session
    const updated: UserSession = {
        ...existing,
        ...updates,
        lastAccessedAt: Date.now(),
    };

    return setUserSession(updated);
}

/**
 * Delete user session
 */
export async function deleteUserSession(userId: string): Promise<boolean> {
    const key = generateCacheKey(CachePrefix.USER_SESSION, userId);
    return deleteCache(key);
}

/**
 * Get page state from cache
 */
export async function getPageState(userId: string, route: string): Promise<PageState | null> {
    const key = generateCacheKey(CachePrefix.PAGE_STATE, userId, route);
    return getCache<PageState>(key);
}

/**
 * Set page state in cache
 */
export async function setPageState(userId: string, state: PageState): Promise<boolean> {
    const key = generateCacheKey(CachePrefix.PAGE_STATE, userId, state.route);

    const stateWithTimestamp: PageState = {
        ...state,
        timestamp: Date.now(),
    };

    return setCache(key, stateWithTimestamp, CacheTTL.PAGE_STATE);
}

/**
 * Delete page state
 */
export async function deletePageState(userId: string, route: string): Promise<boolean> {
    const key = generateCacheKey(CachePrefix.PAGE_STATE, userId, route);
    return deleteCache(key);
}

/**
 * Get filters from cache
 */
export async function getFilters(userId: string, context: string): Promise<Record<string, unknown> | null> {
    const key = generateCacheKey(CachePrefix.FILTERS, userId, context);
    return getCache<Record<string, unknown>>(key);
}

/**
 * Set filters in cache
 */
export async function setFilters(
    userId: string,
    context: string,
    filters: Record<string, unknown>
): Promise<boolean> {
    const key = generateCacheKey(CachePrefix.FILTERS, userId, context);
    return setCache(key, filters, CacheTTL.FILTERS);
}

/**
 * Delete filters
 */
export async function deleteFilters(userId: string, context: string): Promise<boolean> {
    const key = generateCacheKey(CachePrefix.FILTERS, userId, context);
    return deleteCache(key);
}

/**
 * Store navigation history
 */
export interface NavigationHistory {
    routes: Array<{
        path: string;
        timestamp: number;
        state?: Record<string, unknown>;
    }>;
    currentIndex: number;
}

/**
 * Get navigation history
 */
export async function getNavigationHistory(userId: string): Promise<NavigationHistory | null> {
    const key = generateCacheKey(CachePrefix.PAGE_STATE, userId, 'nav-history');
    return getCache<NavigationHistory>(key);
}

/**
 * Update navigation history
 */
export async function updateNavigationHistory(
    userId: string,
    route: string,
    state?: Record<string, unknown>
): Promise<boolean> {
    const existing = await getNavigationHistory(userId);

    const newEntry = {
        path: route,
        timestamp: Date.now(),
        state,
    };

    let history: NavigationHistory;

    if (!existing) {
        history = {
            routes: [newEntry],
            currentIndex: 0,
        };
    } else {
        // Remove any forward history if we're not at the end
        const routes = existing.routes.slice(0, existing.currentIndex + 1);
        routes.push(newEntry);

        // Keep only last 50 entries
        if (routes.length > 50) {
            routes.shift();
        }

        history = {
            routes,
            currentIndex: routes.length - 1,
        };
    }

    const key = generateCacheKey(CachePrefix.PAGE_STATE, userId, 'nav-history');
    return setCache(key, history, CacheTTL.PAGE_STATE);
}

/**
 * Get dataset cache key
 */
export function getDatasetCacheKey(projectId: string, datasetId?: string): string {
    if (datasetId) {
        return generateCacheKey(CachePrefix.DATASET, projectId, datasetId);
    }
    return generateCacheKey(CachePrefix.DATASET, projectId);
}

/**
 * Get table metadata cache key
 */
export function getTableMetadataCacheKey(
    database: string,
    schema: string,
    table: string
): string {
    return generateCacheKey(CachePrefix.TABLE_METADATA, database, schema, table);
}
