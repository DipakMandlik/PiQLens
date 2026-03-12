
import { NextResponse } from 'next/server';

export const dynamic = 'force-dynamic'; // Prevent caching

export async function GET() {
    try {
        const ollamaUrl = process.env.OLLAMA_ENDPOINT || 'http://localhost:11434';

        // Timeout after 2 seconds to fail fast
        const controller = new AbortController();
        const timeoutId = setTimeout(() => controller.abort(), 2000);

        const response = await fetch(`${ollamaUrl}/api/tags`, {
            signal: controller.signal
        });

        clearTimeout(timeoutId);

        if (response.ok) {
            const data = await response.json();
            return NextResponse.json({
                status: 'ok',
                message: 'Ollama is online',
                modelCount: data.models?.length || 0,
                models: data.models?.map((m: any) => m.name) || []
            });
        } else {
            return NextResponse.json({
                status: 'error',
                message: `Ollama returned status ${response.status}`
            }, { status: 503 });
        }
    } catch (error: any) {
        return NextResponse.json({
            status: 'error',
            message: 'Ollama is unreachable. Is "ollama serve" running?',
            details: error.message
        }, { status: 503 });
    }
}
