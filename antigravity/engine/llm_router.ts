/**
 * Antigravity Ollama LLM Router
 * Replaces Google Gemini with local Ollama models
 * 
 * Supported models:
 * - mixtral:8x7b (recommended, slower but smarter)
 * - llama3:8b (faster, lighter)
 */

import fs from 'fs';
import path from 'path';

export interface OllamaConfig {
    endpoint: string;
    model: string;
    timeout: number;
}

export interface OllamaResponse {
    model: string;
    created_at: string;
    response: string;
    done: boolean;
}

export class OllamaRouter {
    private endpoint: string;
    private model: string;
    private timeout: number;

    constructor(config?: Partial<OllamaConfig>) {
        this.endpoint = config?.endpoint || process.env.OLLAMA_ENDPOINT || 'http://localhost:11434';
        this.model = config?.model || process.env.OLLAMA_MODEL || 'tinyllama';
        this.timeout = config?.timeout || 300000; // 300 seconds (5 minutes) for model loading
    }

    /**
     * Health check - verify Ollama is running and model is available
     */
    async healthCheck(): Promise<{ healthy: boolean; error?: string; models?: string[] }> {
        try {
            const response = await fetch(`${this.endpoint}/api/tags`, {
                method: 'GET',
                signal: AbortSignal.timeout(5000)
            });

            if (!response.ok) {
                return {
                    healthy: false,
                    error: `Ollama service returned HTTP ${response.status}. Is Ollama running? Try: ollama serve`
                };
            }

            const data = await response.json() as { models: Array<{ name: string }> };
            const models = data.models.map(m => m.name);

            if (models.length === 0) {
                return {
                    healthy: false,
                    error: 'No models loaded in Ollama. Pull a model first: ollama pull mixtral:8x7b',
                    models: []
                };
            }

            const hasModel = models.some(m => m.includes(this.model.split(':')[0]));

            if (!hasModel) {
                return {
                    healthy: false,
                    error: `Model ${this.model} not found. Pull it first: ollama pull ${this.model}. Available models: ${models.join(', ')}`,
                    models
                };
            }

            return { healthy: true, models };
        } catch (e: any) {
            // Provide helpful error messages for common issues
            if (e.name === 'AbortError' || e.message.includes('timeout')) {
                return {
                    healthy: false,
                    error: 'Ollama service timeout. Is Ollama running? Check: curl http://localhost:11434/api/tags'
                };
            }
            if (e.message.includes('ECONNREFUSED') || e.message.includes('fetch failed')) {
                return {
                    healthy: false,
                    error: `Cannot connect to Ollama at ${this.endpoint}. Start Ollama: ollama serve`
                };
            }
            return { healthy: false, error: `Ollama health check failed: ${e.message}` };
        }
    }

    /**
     * Generate execution plan from user question
     */
    async generatePlan(
        question: string,
        assetId: string,
        schemaRegistry: string
    ): Promise<{ plan: any; rawOutput: string; responseTimeMs: number }> {
        const startTime = Date.now();

        // Load prompt template
        const promptTemplate = this.loadPromptTemplate('plan_generation.prompt');
        const prompt = promptTemplate
            .replace('{{SCHEMA_REGISTRY}}', schemaRegistry)
            .replace('{{USER_QUESTION}}', question)
            .replace('{{ASSET_ID}}', assetId);

        const rawOutput = await this.callOllama(prompt, true);
        const plan = this.parseJsonResponse(rawOutput);

        const responseTimeMs = Date.now() - startTime;

        return { plan, rawOutput, responseTimeMs };
    }

    /**
     * Generate business interpretation from query results
     */
    async interpretResults(
        question: string,
        assetId: string,
        results: any[]
    ): Promise<{ interpretation: any; rawOutput: string; responseTimeMs: number }> {
        const startTime = Date.now();

        // Load prompt template
        const promptTemplate = this.loadPromptTemplate('business_interpretation.prompt');
        const inputData = JSON.stringify({
            question,
            asset: assetId,
            results: results.slice(0, 50) // Limit to first 50 rows to avoid token limits
        }, null, 2);

        const prompt = promptTemplate.replace('{{INPUT_DATA}}', inputData);

        const rawOutput = await this.callOllama(prompt, true);
        const interpretation = this.parseJsonResponse(rawOutput);

        const responseTimeMs = Date.now() - startTime;

        return { interpretation, rawOutput, responseTimeMs };
    }

    /**
     * Call Ollama API with retry logic
     */
    private async callOllama(prompt: string, expectJson: boolean = false): Promise<string> {
        const maxRetries = 3;
        let lastError: Error | null = null;

        for (let attempt = 1; attempt <= maxRetries; attempt++) {
            try {
                const response = await fetch(`${this.endpoint}/api/generate`, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({
                        model: this.model,
                        prompt,
                        format: expectJson ? 'json' : undefined,
                        stream: false,
                        options: {
                            temperature: 0.1, // Low temperature for consistent, deterministic output
                            top_p: 0.9
                        }
                    }),
                    signal: AbortSignal.timeout(this.timeout)
                });

                if (!response.ok) {
                    throw new Error(`Ollama API error: HTTP ${response.status}`);
                }

                const data = await response.json() as OllamaResponse;
                return data.response;

            } catch (e: any) {
                lastError = e;
                console.warn(`[OLLAMA] Attempt ${attempt}/${maxRetries} failed: ${e.message}`);

                if (attempt < maxRetries) {
                    // Exponential backoff
                    const waitTime = Math.pow(2, attempt) * 1000;
                    await new Promise(resolve => setTimeout(resolve, waitTime));
                }
            }
        }

        throw new Error(`Ollama call failed after ${maxRetries} attempts: ${lastError?.message}`);
    }

    /**
     * Parse JSON response from LLM, handling markdown code blocks
     */
    private parseJsonResponse(text: string): any {
        // Remove markdown code blocks if present
        let cleaned = text.trim();
        cleaned = cleaned.replace(/```json\n?/gi, '').replace(/```\n?/g, '');

        try {
            return JSON.parse(cleaned);
        } catch (e) {
            throw new Error(`Failed to parse LLM JSON response: ${e instanceof Error ? e.message : 'Unknown error'}\n\nRaw output:\n${text}`);
        }
    }

    /**
     * Load prompt template from file
     */
    private loadPromptTemplate(filename: string): string {
        const promptPath = path.join(process.cwd(), 'antigravity', 'prompts', filename);

        if (!fs.existsSync(promptPath)) {
            throw new Error(`Prompt template not found: ${promptPath}`);
        }

        return fs.readFileSync(promptPath, 'utf-8');
    }

    /**
     * Get current configuration
     */
    getConfig(): OllamaConfig {
        return {
            endpoint: this.endpoint,
            model: this.model,
            timeout: this.timeout
        };
    }
}
