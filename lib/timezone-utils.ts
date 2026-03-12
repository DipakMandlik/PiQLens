/**
 * Timezone Utilities for Pi-Qualytics
 * 
 * CRITICAL POLICY: All timestamps MUST be displayed in IST (Asia/Kolkata, UTC+05:30)
 * 
 * Snowflake stores TIMESTAMP_NTZ (UTC logical format, no timezone metadata).
 * Frontend receives these as ISO strings and must convert to IST for display.
 */

/**
 * Format a UTC timestamp to IST (Asia/Kolkata) display format.
 * 
 * @param utcTimestamp - ISO string or Date object from Snowflake (stored as TIMESTAMP_NTZ)
 * @returns Formatted string in IST timezone (e.g., "11 Feb 2026, 06:12:30 IST")
 * 
 * @example
 * // Snowflake stores: 2026-02-11 00:42:00 (UTC NTZ)
 * formatIST('2026-02-11T00:42:00Z')
 * // Returns: "11 Feb 2026, 06:12:00 IST" (UTC+05:30)
 */
export const formatIST = (utcTimestamp: string | Date): string => {
    if (!utcTimestamp) return 'Unknown';

    try {
        const date = new Date(utcTimestamp);

        // Return structured IST format
        return new Intl.DateTimeFormat('en-IN', {
            year: 'numeric',
            month: 'short',
            day: '2-digit',
            hour: '2-digit',
            minute: '2-digit',
            second: '2-digit',
            hour12: false,
            timeZone: 'Asia/Kolkata'
        }).format(date) + ' IST';
    } catch (error) {
        console.error('Error formatting timestamp to IST:', error);
        return 'Invalid Date';
    }
};

/**
 * Format a UTC timestamp to short IST format (without seconds).
 * 
 * @param utcTimestamp - ISO string or Date object
 * @returns Formatted string (e.g., "11 Feb 2026, 06:12 IST")
 */
export const formatISTShort = (utcTimestamp: string | Date): string => {
    if (!utcTimestamp) return 'Unknown';

    try {
        const date = new Date(utcTimestamp);

        return new Intl.DateTimeFormat('en-IN', {
            year: 'numeric',
            month: 'short',
            day: '2-digit',
            hour: '2-digit',
            minute: '2-digit',
            hour12: false,
            timeZone: 'Asia/Kolkata'
        }).format(date) + ' IST';
    } catch (error) {
        console.error('Error formatting timestamp to IST:', error);
        return 'Invalid Date';
    }
};

/**
 * Get today's date in IST timezone as YYYY-MM-DD string.
 * Used for date comparisons and filtering.
 * 
 * @returns Date string in IST (e.g., "2026-02-11")
 * 
 * @example
 * getTodayIST() // Returns: "2026-02-11" (when called at 15:46 IST on Feb 11)
 */
export const getTodayIST = (): string => {
    try {
        return new Date().toLocaleDateString('en-CA', {
            timeZone: 'Asia/Kolkata'
        });
    } catch (error) {
        console.error('Error getting today in IST:', error);
        return new Date().toISOString().split('T')[0]; // Fallback to UTC
    }
};

/**
 * Get current date and time in IST as a Date object.
 * 
 * @returns Date object representing current IST time
 */
export const getNowIST = (): Date => {
    const now = new Date();
    const istOffset = 5.5 * 60 * 60 * 1000; // IST is UTC+05:30
    return new Date(now.getTime() + istOffset);
};

/**
 * Check if a given date string matches today in IST.
 * Used to determine if scanning is allowed (only for current day).
 * 
 * @param dateString - Date string in YYYY-MM-DD format
 * @returns true if date matches today in IST
 */
export const isTodayIST = (dateString: string): boolean => {
    return dateString === getTodayIST();
};

/**
 * Format a date string (YYYY-MM-DD) to human-readable format.
 * 
 * @param dateString - Date string (e.g., "2026-02-11")
 * @returns Formatted string (e.g., "11 Feb 2026")
 */
export const formatDateReadable = (dateString: string): string => {
    if (!dateString) return 'Unknown';

    try {
        const [year, month, day] = dateString.split('-');
        const date = new Date(parseInt(year), parseInt(month) - 1, parseInt(day));

        return new Intl.DateTimeFormat('en-IN', {
            year: 'numeric',
            month: 'short',
            day: '2-digit'
        }).format(date);
    } catch (error) {
        console.error('Error formatting date:', error);
        return dateString;
    }
};
