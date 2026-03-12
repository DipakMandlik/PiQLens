import { NextResponse } from 'next/server';
import { getCatalogOverview } from '@/lib/catalog-service';

export async function GET(request: Request) {
    try {
        const { searchParams } = new URL(request.url);
        const search = searchParams.get('search')?.toLowerCase();
        const database = searchParams.get('database');
        const schema = searchParams.get('schema');
        const domain = searchParams.get('domain');
        const minDqScore = searchParams.get('minDqScore');
        const classification = searchParams.get('classification');
        const forceRefresh = searchParams.get('refresh') === 'true';

        let tables = await getCatalogOverview(forceRefresh);

        if (search) {
            tables = tables.filter(t =>
                t.table.toLowerCase().includes(search) ||
                t.database.toLowerCase().includes(search) ||
                t.schema.toLowerCase().includes(search)
            );
        }
        if (database) {
            tables = tables.filter(t => t.database === database);
        }
        if (schema) {
            tables = tables.filter(t => t.schema === schema);
        }
        if (domain) {
            tables = tables.filter(t => t.businessDomain === domain);
        }
        if (classification && classification !== 'All') {
            tables = tables.filter(t => t.usageClassification === classification);
        }
        if (minDqScore) {
            const minScore = Number(minDqScore);
            tables = tables.filter(t => t.dqScore !== null && t.dqScore >= minScore);
        }

        return NextResponse.json(tables);
    } catch (error: any) {
        console.error('Catalog API Error:', error);
        return NextResponse.json({ error: error.message || 'Failed to fetch catalog' }, { status: 500 });
    }
}
