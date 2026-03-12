import { NextResponse } from 'next/server';
import { getTableDetails } from '@/lib/catalog-service';

export async function GET(
    request: Request,
    { params }: { params: Promise<{ database: string; schema: string; table: string }> | { database: string; schema: string; table: string } }
) {
    try {
        // Await params to support Next.js 15+ async params
        const resolvedParams = await params;
        const { database, schema, table } = resolvedParams;

        if (!database || !schema || !table) {
            return NextResponse.json({ error: 'Missing table path parameters' }, { status: 400 });
        }

        const details = await getTableDetails(database, schema, table);

        if (!details) {
            return NextResponse.json({ error: 'Table not found' }, { status: 404 });
        }

        return NextResponse.json(details);
    } catch (error: any) {
        console.error('Catalog Table Details API Error:', error);
        return NextResponse.json({ error: error.message || 'Failed to fetch table details' }, { status: 500 });
    }
}
