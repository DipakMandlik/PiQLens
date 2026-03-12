'use client';

import { useState } from 'react';
import { ChevronLeft, ChevronRight, X, Calendar as CalendarIcon } from 'lucide-react';
import { Popover, PopoverContent, PopoverTrigger } from '@/components/ui/popover';

interface SmartDatePickerProps {
  value: string; // YYYY-MM-DD format
  onChange: (date: string) => void;
  availableDates: string[]; // Array of YYYY-MM-DD dates that have data
  disabled?: boolean;
  placeholder?: string;
}

export function SmartDatePicker({
  value,
  onChange,
  availableDates,
  disabled = false,
  placeholder = 'Select date',
}: SmartDatePickerProps) {
  const [isOpen, setIsOpen] = useState(false);
  const [currentMonth, setCurrentMonth] = useState<Date>(() => {
    if (value) {
      const [year, month] = value.split('-');
      return new Date(parseInt(year), parseInt(month) - 1);
    }
    return new Date();
  });

  const availableDateSet = new Set(availableDates);
  const today = new Date();
  today.setHours(0, 0, 0, 0);
  
  // Format today's date as YYYY-MM-DD using local timezone (not UTC)
  const todayString = `${today.getFullYear()}-${String(today.getMonth() + 1).padStart(2, '0')}-${String(today.getDate()).padStart(2, '0')}`;

  // Format display value
  const displayValue = value
    ? new Date(`${value}T00:00:00`).toLocaleDateString('en-IN', {
        day: '2-digit',
        month: 'short',
        year: 'numeric',
      })
    : placeholder;

  // Get days in month
  const getDaysInMonth = (date: Date) => {
    return new Date(date.getFullYear(), date.getMonth() + 1, 0).getDate();
  };

  // Get first day of month (0 = Sunday, 1 = Monday, etc.)
  const getFirstDayOfMonth = (date: Date) => {
    return new Date(date.getFullYear(), date.getMonth(), 1).getDay();
  };

  // Format date for comparison
  const formatDateString = (year: number, month: number, day: number): string => {
    return `${year}-${String(month + 1).padStart(2, '0')}-${String(day).padStart(2, '0')}`;
  };

  // Check if date is available and not in future
  const isDateAvailable = (year: number, month: number, day: number): boolean => {
    const dateStr = formatDateString(year, month, day);
    const dateObj = new Date(`${dateStr}T00:00:00`);
    
    // Disable future dates
    if (dateObj > today) {
      return false;
    }
    
    // Check if date is in available dates
    return availableDateSet.has(dateStr);
  };

  // Generate calendar days
  const generateCalendarDays = () => {
    const daysInMonth = getDaysInMonth(currentMonth);
    const firstDay = getFirstDayOfMonth(currentMonth);
    
    const days: (number | null)[] = [];
    
    // Add empty slots for days before month starts
    for (let i = 0; i < firstDay; i++) {
      days.push(null);
    }
    
    // Add days of month
    for (let day = 1; day <= daysInMonth; day++) {
      days.push(day);
    }
    
    return days;
  };

  const handlePrevMonth = () => {
    setCurrentMonth(new Date(currentMonth.getFullYear(), currentMonth.getMonth() - 1, 1));
  };

  const handleNextMonth = () => {
    setCurrentMonth(new Date(currentMonth.getFullYear(), currentMonth.getMonth() + 1, 1));
  };

  const handleDateSelect = (day: number) => {
    const year = currentMonth.getFullYear();
    const month = currentMonth.getMonth();
    
    if (isDateAvailable(year, month, day)) {
      const dateStr = formatDateString(year, month, day);
      onChange(dateStr);
      setIsOpen(false);
    }
  };

  const handleClear = (e: React.MouseEvent) => {
    e.stopPropagation();
    onChange('');
  };

  const monthName = currentMonth.toLocaleDateString('en-US', { month: 'long', year: 'numeric' });
  const calendarDays = generateCalendarDays();
  const year = currentMonth.getFullYear();
  const month = currentMonth.getMonth();

  return (
    <Popover open={isOpen} onOpenChange={setIsOpen}>
      <PopoverTrigger asChild>
        <div className="relative w-[166px]">
          <input
            type="text"
            readOnly
            value={displayValue}
            onClick={() => !disabled && setIsOpen(true)}
            disabled={disabled}
            className={`h-9 w-full rounded-md border border-slate-300 bg-white px-3 text-sm text-slate-800 cursor-pointer focus:border-slate-500 focus:outline-none transition-colors ${
              disabled ? 'bg-slate-50 text-slate-400 cursor-not-allowed' : 'hover:border-slate-400'
            }`}
            placeholder={placeholder}
          />
          <div className="absolute right-2 top-1/2 -translate-y-1/2 flex items-center gap-1 pointer-events-none">
            {value && !disabled && (
              <button
                onClick={handleClear}
                className="pointer-events-auto p-0.5 hover:bg-slate-100 rounded transition-colors"
                title="Clear date"
              >
                <X size={14} className="text-slate-400 hover:text-slate-600" />
              </button>
            )}
            <CalendarIcon size={14} className="text-slate-400" />
          </div>
        </div>
      </PopoverTrigger>

      <PopoverContent className="w-80 p-4" align="start">
        <div className="space-y-4">
          {/* Month/Year Header */}
          <div className="flex items-center justify-between">
            <button
              onClick={handlePrevMonth}
              className="p-1.5 hover:bg-slate-100 rounded transition-colors"
              aria-label="Previous month"
            >
              <ChevronLeft size={18} className="text-slate-600" />
            </button>
            <h3 className="text-sm font-semibold text-slate-900">{monthName}</h3>
            <button
              onClick={handleNextMonth}
              className="p-1.5 hover:bg-slate-100 rounded transition-colors"
              aria-label="Next month"
            >
              <ChevronRight size={18} className="text-slate-600" />
            </button>
          </div>

          {/* Weekday Headers */}
          <div className="grid grid-cols-7 gap-1 mb-2">
            {['Su', 'Mo', 'Tu', 'We', 'Th', 'Fr', 'Sa'].map((day) => (
              <div
                key={day}
                className="text-center text-xs font-semibold text-slate-500 py-1"
              >
                {day}
              </div>
            ))}
          </div>

          {/* Calendar Grid */}
          <div className="grid grid-cols-7 gap-1">
            {calendarDays.map((day, idx) => {
              if (day === null) {
                return <div key={`empty-${idx}`} className="aspect-square" />;
              }

              const isAvailable = isDateAvailable(year, month, day);
              const dateStr = formatDateString(year, month, day);
              const isSelected = value === dateStr;
              const isToday = dateStr === todayString;

              return (
                <button
                  key={day}
                  onClick={() => handleDateSelect(day)}
                  disabled={!isAvailable}
                  className={`
                    aspect-square flex items-center justify-center rounded text-xs font-medium
                    transition-all duration-200
                    ${
                      isSelected
                        ? 'bg-indigo-600 text-white shadow-md'
                        : isToday
                        ? 'bg-indigo-100 text-indigo-900 border-2 border-indigo-300'
                        : isAvailable
                        ? 'bg-white text-slate-800 border border-slate-200 hover:bg-slate-50 hover:border-indigo-300'
                        : 'bg-slate-50 text-slate-300 cursor-not-allowed'
                    }
                  `}
                  title={
                    !isAvailable
                      ? 'No data available for this date'
                      : isToday
                      ? 'Today'
                      : undefined
                  }
                >
                  {day}
                </button>
              );
            })}
          </div>

          {/* Action Buttons */}
          <div className="flex gap-2 pt-2 border-t border-slate-200">
            <button
              onClick={() => {
                if (availableDateSet.has(todayString)) {
                  onChange(todayString);
                  setIsOpen(false);
                }
              }}
              disabled={!availableDateSet.has(todayString)}
              className="text-xs font-medium text-indigo-600 hover:text-indigo-700 disabled:text-slate-300 disabled:cursor-not-allowed"
            >
              Today
            </button>
            <button
              onClick={() => {
                setIsOpen(false);
              }}
              className="text-xs font-medium text-slate-600 hover:text-slate-800 ml-auto"
            >
              Close
            </button>
          </div>

          {/* Info Text */}
          {availableDates.length > 0 && (
            <div className="text-xs text-slate-500 pt-1 border-t border-slate-200">
              {availableDates.length} dates available • Grayed dates unavailable
            </div>
          )}
        </div>
      </PopoverContent>
    </Popover>
  );
}
