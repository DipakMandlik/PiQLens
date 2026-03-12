// next.config.ts
import type { NextConfig } from 'next'

const nextConfig: NextConfig = {
  trailingSlash: true,
  output: 'standalone',
  serverExternalPackages: ['pdfkit', 'snowflake-sdk'],
}

export default nextConfig