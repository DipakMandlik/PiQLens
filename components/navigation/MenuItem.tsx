'use client';

import { useState } from 'react';
import Link from 'next/link';
import { usePathname } from 'next/navigation';
import { ChevronRight } from 'lucide-react';
import { cn } from '@/lib/utils';
import type { NavigationItem } from '@/lib/config/navigation';

interface MenuItemProps {
  item: NavigationItem;
  level?: number;
}

export function MenuItem({ item, level = 0 }: MenuItemProps) {
  const pathname = usePathname();
  const [isExpanded, setIsExpanded] = useState(true);
  const hasChildren = item.children && item.children.length > 0;
  const isActive = pathname === item.href;
  const Icon = item.icon;

  const handleClick = () => {
    if (hasChildren) {
      setIsExpanded((prev) => !prev);
    }
  };

  const iconColor = (item as any).snowflakeIcon ? 'text-sky-500' : 'text-slate-400';

  const content = (
    <div
      className={cn(
        'group flex items-center gap-2.5 rounded-md px-3 py-2 text-sm transition-colors duration-150 cursor-pointer',
        level === 0 ? 'font-medium' : 'font-normal',
        isActive
          ? 'bg-slate-100 text-slate-900'
          : 'text-slate-600 hover:bg-slate-50 hover:text-slate-900'
      )}
      style={{ paddingLeft: `${12 + level * 16}px` }}
      onClick={handleClick}
    >
      {hasChildren ? (
        <ChevronRight
          className={cn(
            'h-3.5 w-3.5 flex-shrink-0 text-slate-400 transition-transform duration-150',
            isExpanded && 'rotate-90',
            isActive && 'text-slate-700'
          )}
        />
      ) : (
        <Icon
          className={cn(
            'h-4 w-4 flex-shrink-0 transition-colors duration-150',
            iconColor,
            isActive && 'text-slate-700'
          )}
        />
      )}

      <span className="min-w-0 flex-1 truncate">{item.label}</span>
    </div>
  );

  return (
    <div>
      {item.href && !hasChildren ? <Link href={item.href}>{content}</Link> : content}
      {hasChildren && isExpanded && (
        <div className="mt-0.5 space-y-0.5">
          {item.children!.map((child) => (
            <MenuItem key={child.id} item={child} level={level + 1} />
          ))}
        </div>
      )}
    </div>
  );
}
