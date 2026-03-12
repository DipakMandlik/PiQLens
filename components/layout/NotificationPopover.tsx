'use client';

import { useState, useEffect } from 'react';
import { Bell, AlertCircle, Info, Database, CheckCircle2 } from 'lucide-react';
import {
    Popover,
    PopoverContent,
    PopoverTrigger,
} from '@/components/ui/popover';

interface NotificationProps {
    id: string;
    title: string;
    desc: string;
    time: string;
    type: 'alert' | 'info' | 'success';
    read: boolean;
}

export function NotificationPopover() {
    const [notifications, setNotifications] = useState<NotificationProps[]>([]);

    useEffect(() => {
        let mounted = true;
        const fetchNotifications = async () => {
            try {
                const res = await fetch('/api/dq/notifications');
                if (!res.ok) return;
                const json = await res.json();
                if (json.success && json.data && mounted) {
                    setNotifications(json.data);
                }
            } catch (err) {
                console.error('Failed to fetch notifications:', err);
            }
        };

        fetchNotifications();
        
        // Poll every 60 seconds
        const intervalId = setInterval(fetchNotifications, 60000);
        
        return () => {
            mounted = false;
            clearInterval(intervalId);
        };
    }, []);
    const unreadCount = notifications.filter((n) => !n.read).length;

    const handleMarkAsRead = (id: string) => {
        setNotifications((prev) =>
            prev.map((n) => n.id === id ? { ...n, read: true } : n)
        );
    };

    const handleMarkAllAsRead = () => {
        setNotifications((prev) => prev.map((n) => ({ ...n, read: true })));
    };

    return (
        <Popover>
            <PopoverTrigger asChild>
                <button
                    className="p-2.5 hover:bg-gray-100/80 rounded-full transition-all duration-200 relative text-gray-400 hover:text-gray-700 outline-none focus-visible:ring-2 focus-visible:ring-blue-500/50"
                    aria-label="Notifications"
                >
                    <Bell className="h-5 w-5" />
                    {unreadCount > 0 && (
                        <span className="absolute top-2 right-2.5 h-2 w-2 bg-red-500 rounded-full ring-2 ring-white" />
                    )}
                </button>
            </PopoverTrigger>

            <PopoverContent align="end" className="w-[360px] p-0 rounded-xl overflow-hidden shadow-xl border-gray-100/60 mt-2">
                <div className="flex items-center justify-between p-4 border-b border-gray-100/80 bg-white sticky top-0">
                    <h3 className="font-semibold text-gray-900 text-[15px]">Notifications</h3>
                    <div className="flex items-center gap-3">
                        {unreadCount > 0 && (
                            <span className="text-xs bg-red-50 text-red-600 px-2.5 py-1 rounded-full font-medium">
                                {unreadCount} New
                            </span>
                        )}
                        {unreadCount > 0 && (
                            <button
                                onClick={handleMarkAllAsRead}
                                className="text-xs text-blue-600 hover:text-blue-700 hover:underline transition-all"
                            >
                                Mark all as read
                            </button>
                        )}
                    </div>
                </div>

                <div className="max-h-[380px] overflow-y-auto bg-gray-50/30">
                    {notifications.length === 0 ? (
                        <div className="p-8 text-center text-gray-500 text-sm">No notifications.</div>
                    ) : (
                        <ul className="divide-y divide-gray-100/60">
                            {notifications.map((notif) => {
                                return (
                                    <li
                                        key={notif.id}
                                        onClick={() => handleMarkAsRead(notif.id)}
                                        className={`p-4 hover:bg-white transition-colors cursor-pointer group ${!notif.read ? 'bg-blue-50/20' : ''}`}
                                    >
                                        <div className="flex gap-3 items-start">
                                            <div className={`mt-0.5 rounded-full p-1.5 shrink-0 ${notif.type === 'alert' ? 'bg-red-50 text-red-500' :
                                                notif.type === 'success' ? 'bg-emerald-50 text-emerald-500' :
                                                    'bg-blue-50 text-blue-500'
                                                }`}>
                                                {notif.type === 'alert' && <AlertCircle className="h-4 w-4" />}
                                                {notif.type === 'success' && <CheckCircle2 className="h-4 w-4" />}
                                                {notif.type === 'info' && <Info className="h-4 w-4" />}
                                            </div>
                                            <div className="flex-1 min-w-0">
                                                <div className="flex items-start justify-between gap-1 mb-1">
                                                    <p className={`text-[13px] font-semibold truncate ${!notif.read ? 'text-gray-900' : 'text-gray-700'}`}>
                                                        {notif.title}
                                                    </p>
                                                    <span className="text-[11px] text-gray-400 whitespace-nowrap shrink-0">{notif.time}</span>
                                                </div>
                                                <p className="text-[13px] text-gray-500 line-clamp-2 leading-snug">
                                                    {notif.desc}
                                                </p>
                                            </div>
                                        </div>
                                    </li>
                                );
                            })}
                        </ul>
                    )}
                </div>

                <div className="p-3 border-t border-gray-100 bg-white text-center">
                    <button className="text-[13px] text-blue-600 font-medium hover:text-blue-700 hover:underline underline-offset-2 transition-all">
                        View All Notifications
                    </button>
                </div>
            </PopoverContent>
        </Popover>
    );
}
