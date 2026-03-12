import { AppLayout } from '@/components/layout/AppLayout';
import DatasetDetailPage from '@/app/dataset/[id]/DatasetDetailPage';

export default function DatasetPage() {
    return (
        <AppLayout>
            <DatasetDetailPage />
        </AppLayout>
    );
}
