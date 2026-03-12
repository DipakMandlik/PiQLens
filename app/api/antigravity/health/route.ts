/**
 * Antigravity Health Check API
 * Returns Ollama service status and configuration
 */

import { NextResponse } from 'next/server';
import { OllamaRouter } from '@/antigravity/engine/llm_router';

export async function GET() {
    try {
        const router = new OllamaRouter();
        const health = await router.healthCheck();
        const config = router.getConfig();

        if (health.healthy) {
            return NextResponse.json({
                status: 'healthy',
                service: 'ollama',
                endpoint: config.endpoint,
                model: config.model,
                availableModels: health.models || [],
                message: 'Ollama is running and ready'
            });
        } else {
            return NextResponse.json({
                status: 'unavailable',
                service: 'ollama',
                endpoint: config.endpoint,
                model: config.model,
                error: health.error,
                message: 'Ollama service is not responding',
                hint: 'Please ensure Ollama is running: ollama serve'
            }, { status: 503 });
        }
    } catch (e: any) {
        return NextResponse.json({
            status: 'error',
            service: 'ollama',
            error: e.message,
            message: 'Failed to check Ollama health'
        }, { status: 500 });
    }
}
