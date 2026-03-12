'use client';

import { useState } from 'react';
import Image from 'next/image';
import { ConnectionDialog } from './ConnectionDialog';
import { Database } from 'lucide-react';
import { Button } from './ui/button';

export function Navbar() {
  const [isDialogOpen, setIsDialogOpen] = useState(false);

  return (
    <>
      <nav className="border-b border-slate-200 dark:border-slate-700 bg-white dark:bg-slate-900 shrink-0 z-50">
        <div className="container mx-auto px-4">
          <div className="flex items-center justify-between h-16">
            <div className="flex items-center gap-8">
              <a href="/" className="flex items-center gap-2">
                <Image
                  src="/assets/branding/piqlens-logo.png"
                  alt="πQLens"
                  width={200}
                  height={50}
                  className="h-12 w-auto object-contain"
                  priority
                />
                <span className="brand-text">πQLens</span>
              </a>
              <div className="hidden md:flex items-center gap-6">
                <a href="/" className="text-sm font-medium text-slate-700 hover:text-blue-600 transition-colors">
                  Observability
                </a>
                <a href="/ai" className="text-sm font-medium text-slate-500 hover:text-blue-600 transition-colors">
                  AI Studio
                </a>
              </div>
            </div>
            <div className="flex items-center">
              <Button
                variant="outline"
                onClick={() => setIsDialogOpen(true)}
                className="flex items-center gap-2"
              >
                <Database className="size-4" />
                Connect
              </Button>
            </div>
          </div>
        </div>
      </nav>
      <ConnectionDialog
        isOpen={isDialogOpen}
        onClose={() => setIsDialogOpen(false)}
      />
    </>
  );
}

