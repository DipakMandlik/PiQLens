import { useState, useEffect, useCallback } from 'react';

/**
 * Options for useCachedData hook
 */
export interface UseCachedDataOptions<T> {
    /** Initial data to use before fetching */
    initialData?: T;
    /** Whether to fetch data immediately on mount */
    fetchOnMount?: boolean;
    /** Refetch interval in milliseconds (0 = no auto-refetch) */
    refetchInterval?: number;
    /** Whether to refetch on window focus */
    refetchOnFocus?: boolean;
    /** Custom error handler */
    onError?: (error: Error) => void;
    /** Custom success handler */
    onSuccess?: (data: T) => void;
}

/**
 * Result from useCachedData hook
 */
export interface UseCachedDataResult<T> {
    /** The fetched data */
    data: T | null;
    /** Loading state */
    isLoading: boolean;
    /** Error state */
    error: Error | null;
    /** Manually trigger a refetch */
    refetch: () => Promise<void>;
    /** Whether data is from cache (check X-Cache header) */
    isFromCache: boolean;
}

/**
 * Custom hook for fetching data with cache awareness
 * Automatically handles loading states, errors, and refetching
 * 
 * IMPORTANT: refetchOnFocus is disabled by default to prevent data loss on window focus
 * 
 * @example
 * ```tsx
 * const { data, isLoading, error, refetch } = useCachedData<Dataset[]>(
 *   '/api/dq/datasets',
 *   { fetchOnMount: true }
 * );
 * ```
 */
export function useCachedData<T>(
    url: string | null,
    options: UseCachedDataOptions<T> = {}
): UseCachedDataResult<T> {
    const {
        initialData = null,
        fetchOnMount = true,
        refetchInterval = 0,
        refetchOnFocus = false, // CRITICAL: Disabled by default to prevent refresh issues
        onError,
        onSuccess,
    } = options;

    const [data, setData] = useState<T | null>(initialData);
    const [isLoading, setIsLoading] = useState<boolean>(false);
    const [error, setError] = useState<Error | null>(null);
    const [isFromCache, setIsFromCache] = useState<boolean>(false);

    const fetchData = useCallback(async () => {
        if (!url) return;

        setIsLoading(true);
        setError(null);

        try {
            const response = await fetch(url);

            if (!response.ok) {
                throw new Error(`HTTP error! status: ${response.status}`);
            }

            // Check if response is from cache
            const cacheHeader = response.headers.get('X-Cache');
            setIsFromCache(cacheHeader === 'HIT');

            const result = await response.json();

            // Handle both direct data and wrapped responses
            const fetchedData = result.data !== undefined ? result.data : result;

            setData(fetchedData);

            if (onSuccess) {
                onSuccess(fetchedData);
            }
        } catch (err) {
            const error = err instanceof Error ? err : new Error('An unknown error occurred');
            setError(error);

            if (onError) {
                onError(error);
            }
        } finally {
            setIsLoading(false);
        }
    }, [url, onSuccess, onError]);

    // Fetch on mount
    useEffect(() => {
        if (fetchOnMount && url) {
            fetchData();
        }
    }, [fetchOnMount, url, fetchData]);

    // Auto-refetch interval
    useEffect(() => {
        if (refetchInterval > 0 && url) {
            const intervalId = setInterval(fetchData, refetchInterval);
            return () => clearInterval(intervalId);
        }
    }, [refetchInterval, url, fetchData]);

    // Refetch on window focus
    useEffect(() => {
        if (refetchOnFocus && url) {
            const handleFocus = () => fetchData();
            window.addEventListener('focus', handleFocus);
            return () => window.removeEventListener('focus', handleFocus);
        }
    }, [refetchOnFocus, url, fetchData]);

    return {
        data,
        isLoading,
        error,
        refetch: fetchData,
        isFromCache,
    };
}

/**
 * Hook for fetching data with manual trigger
 * Useful for POST requests or actions that shouldn't auto-fetch
 */
export function useLazyCachedData<T>(
    url: string
): [
        (options?: RequestInit) => Promise<T | null>,
        UseCachedDataResult<T>
    ] {
    const [data, setData] = useState<T | null>(null);
    const [isLoading, setIsLoading] = useState<boolean>(false);
    const [error, setError] = useState<Error | null>(null);
    const [isFromCache, setIsFromCache] = useState<boolean>(false);

    const execute = useCallback(async (options?: RequestInit): Promise<T | null> => {
        setIsLoading(true);
        setError(null);

        try {
            const response = await fetch(url, options);

            if (!response.ok) {
                throw new Error(`HTTP error! status: ${response.status}`);
            }

            const cacheHeader = response.headers.get('X-Cache');
            setIsFromCache(cacheHeader === 'HIT');

            const result = await response.json();
            const fetchedData = result.data !== undefined ? result.data : result;

            setData(fetchedData);
            return fetchedData;
        } catch (err) {
            const error = err instanceof Error ? err : new Error('An unknown error occurred');
            setError(error);
            return null;
        } finally {
            setIsLoading(false);
        }
    }, [url]);

    const refetch = useCallback(async () => {
        await execute();
    }, [execute]);

    return [
        execute,
        {
            data,
            isLoading,
            error,
            refetch,
            isFromCache,
        },
    ];
}
