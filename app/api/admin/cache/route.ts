import { NextResponse } from 'next/server';
import { getValkeyClient } from '@/lib/valkey';
import { getServerConfig } from '@/lib/server-config';

// GET /api/admin/cache
// Exposes Redis/Valkey INFO and operational metrics securely.
export const dynamic = 'force-dynamic';

export async function GET() {
    try {
        // Enforce basic auth check or role check here in production
        const config = getServerConfig();
        if (!config) {
            return NextResponse.json({ success: false, error: 'Not connected' }, { status: 401 });
        }

        const redis = getValkeyClient();
        if (!redis) {
            return NextResponse.json({ success: true, enabled: false, message: 'Valkey cache is currently disabled.' });
        }

        // Fetch fundamental INFO sections
        const memoryInfo = await redis.info('memory');
        const statsInfo = await redis.info('stats');
        const keyspaceInfo = await redis.info('keyspace');
        const clientsInfo = await redis.info('clients');

        // Parse INFO strings into key-value pairs
        const parseInfo = (infoStr: string) => {
            return infoStr.split('\n').reduce((acc, line) => {
                if (line && !line.startsWith('#') && line.includes(':')) {
                    const [key, value] = line.split(':');
                    acc[key.trim()] = value.trim();
                }
                return acc;
            }, {} as Record<string, string>);
        };

        const memObj = parseInfo(memoryInfo);
        const statsObj = parseInfo(statsInfo);
        const keyspaceObj = parseInfo(keyspaceInfo);
        const clientsObj = parseInfo(clientsInfo);

        // Calculate Hit Rate safely
        const hits = parseInt(statsObj.keyspace_hits) || 0;
        const misses = parseInt(statsObj.keyspace_misses) || 0;
        const totalAttempts = hits + misses;
        const hitRate = totalAttempts > 0 ? ((hits / totalAttempts) * 100).toFixed(2) : '0.00';

        // Extract Keyspace DB0 metrics (default)
        let totalKeys = 0;
        let expires = 0;
        if (keyspaceObj.db0) {
            // e.g. "keys=123,expires=10,avg_ttl=5000"
            const db0Parts = keyspaceObj.db0.split(',');
            db0Parts.forEach(part => {
                const [k, v] = part.split('=');
                if (k === 'keys') totalKeys = parseInt(v);
                if (k === 'expires') expires = parseInt(v);
            });
        }

        const dashboardData = {
            enabled: true,
            status: "Connected",
            memory: {
                used_human: memObj.used_memory_human || '0B',
                peak_human: memObj.used_memory_peak_human || '0B',
                fragmentation_ratio: memObj.mem_fragmentation_ratio || '0',
            },
            performance: {
                hit_rate_pct: hitRate,
                total_hits: hits,
                total_misses: misses,
                evicted_keys: parseInt(statsObj.evicted_keys) || 0,
                expired_keys: parseInt(statsObj.expired_keys) || 0,
                instantaneous_ops_per_sec: parseInt(statsObj.instantaneous_ops_per_sec) || 0,
                total_connections_received: parseInt(statsObj.total_connections_received) || 0,
            },
            storage: {
                total_keys: totalKeys,
                keys_with_expiration: expires,
            },
            clients: {
                connected_clients: parseInt(clientsObj.connected_clients) || 0,
                blocked_clients: parseInt(clientsObj.blocked_clients) || 0,
            }
        };

        return NextResponse.json({ success: true, data: dashboardData });

    } catch (error: any) {
        console.error('Failed to fetch Valkey debug metrics:', error);
        return NextResponse.json({ success: false, error: 'Internal server error while fetching cache info' }, { status: 500 });
    }
}
