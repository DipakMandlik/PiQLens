'use client';

import React, { useState } from 'react';
import { useAppStore } from '@/lib/store';
import { SnowflakeConfig } from '@/lib/snowflake-types';
import { X, Database, Lock, Globe, User } from 'lucide-react';
import { useToast } from './ui/toast';

interface ConnectionDialogProps {
  isOpen: boolean;
  onClose: () => void;
}

type InputFieldProps = {
  label: string;
  icon: React.ComponentType<{ size?: number; className?: string }>;
} & React.InputHTMLAttributes<HTMLInputElement>;

const InputField = ({ label, icon: Icon, ...props }: InputFieldProps) => (
  <div className="mb-4">
    <label className="block text-sm font-medium text-slate-700 mb-1">{label}</label>
    <div className="relative">
      <div className="absolute inset-y-0 left-0 pl-3 flex items-center pointer-events-none text-slate-400">
        <Icon size={16} />
      </div>
      <input
        className="block w-full pl-10 pr-3 py-2 border border-slate-300 rounded-md leading-5 bg-white placeholder-slate-400 focus:outline-none focus:placeholder-slate-500 focus:ring-1 focus:ring-indigo-500 focus:border-indigo-500 sm:text-sm text-slate-900"
        {...props}
      />
    </div>
  </div>
);

export const ConnectionDialog = ({ isOpen, onClose }: ConnectionDialogProps) => {
  const { setSnowflakeConfig, snowflakeConfig, setIsConnected } = useAppStore();
  const { showToast } = useToast();
  const [isConnecting, setIsConnecting] = useState(false);
  const [authMode, setAuthMode] = useState<'password' | 'keypair'>('password');
  const [formData, setFormData] = useState<SnowflakeConfig>(snowflakeConfig || {
    accountUrl: '',
    username: '',
    password: '',
    role: ''
  });

  React.useEffect(() => {
    if (snowflakeConfig?.privateKeyPath) {
      setAuthMode('keypair');
    }
  }, [snowflakeConfig]);

  if (!isOpen) return null;

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setIsConnecting(true);

    try {
      const payload: SnowflakeConfig = {
        ...formData,
        role: (formData.role || '').trim() || undefined,
      };

      // Test connection by calling the API
      const response = await fetch('/api/snowflake/connect', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify(payload),
      });

      const data = await response.json() as { success?: boolean; error?: string };

      if (!response.ok || !data.success) {
        let errorMessage = data.error || 'Failed to connect to Snowflake';

        // Provide more helpful error messages
        if (errorMessage.includes('Invalid account')) {
          errorMessage = 'Invalid account format. Please check your Account URL. It should be like "xyz123" or "xyz123.snowflakecomputing.com"';
        } else if (errorMessage.includes('Network policy')) {
          errorMessage = 'Network policy error. Please configure your Snowflake network policy to allow connections from your IP address.';
        } else if (errorMessage.includes('Authentication')) {
          errorMessage = 'Authentication failed. Please check your username and token/password.';
        }

        throw new Error(errorMessage);
      }

      // Save config to store
      setSnowflakeConfig(payload);
      setIsConnected(true);
      showToast('Successfully connected to Snowflake!', 'success');
      onClose();
    } catch (error: unknown) {
      const message = error instanceof Error
        ? error.message
        : 'Failed to connect to Snowflake. Please check your credentials.';
      console.error('Connection error:', error);
      showToast(message, 'error');
    } finally {
      setIsConnecting(false);
    }
  };

  return (
    <div className="fixed inset-0 z-50 overflow-y-auto">
      <div className="flex items-center justify-center min-h-screen pt-4 px-4 pb-20 text-center sm:block sm:p-0">
        <div className="fixed inset-0 transition-opacity" aria-hidden="true">
          <div className="absolute inset-0 bg-slate-900 opacity-75" onClick={onClose}></div>
        </div>

        <span className="hidden sm:inline-block sm:align-middle sm:h-screen" aria-hidden="true">&#8203;</span>

        <div className="inline-block align-bottom bg-white rounded-lg text-left overflow-hidden shadow-xl transform transition-all sm:my-8 sm:align-middle sm:max-w-lg sm:w-full relative z-10">
          <div className="bg-white px-4 pt-5 pb-4 sm:p-6 sm:pb-4">
            <div className="flex justify-between items-start mb-5">
              <h3 className="text-lg leading-6 font-medium text-slate-900 flex items-center gap-2">
                <Database className="text-indigo-600" /> Connect to Snowflake
              </h3>
              <button
                onClick={onClose}
                className="text-slate-400 hover:text-slate-500 cursor-pointer"
                type="button"
              >
                <X size={20} />
              </button>
            </div>

            <form onSubmit={handleSubmit}>
              <InputField
                label="Account URL"
                icon={Globe}
                type="text"
                value={formData.accountUrl}
                onChange={(e: React.ChangeEvent<HTMLInputElement>) => setFormData({ ...formData, accountUrl: e.target.value })}
                placeholder="UXEQGOS-NP89851.snowflakecomputing.com or UXEQGOS-NP89851"
                required
              />

              <InputField
                label="Username"
                icon={User}
                type="text"
                value={formData.username}
                onChange={(e: React.ChangeEvent<HTMLInputElement>) => setFormData({ ...formData, username: e.target.value })}
                placeholder="Your Snowflake username"
                required
              />

              <div className="mb-4">
                <label className="block text-sm font-medium text-slate-700 mb-2">Authentication Method</label>
                <div className="flex gap-4 text-sm">
                  <label className="inline-flex items-center gap-2 text-slate-700">
                    <input
                      type="radio"
                      name="authMode"
                      checked={authMode === 'password'}
                      onChange={() => setAuthMode('password')}
                    />
                    Password / Token
                  </label>
                  <label className="inline-flex items-center gap-2 text-slate-700">
                    <input
                      type="radio"
                      name="authMode"
                      checked={authMode === 'keypair'}
                      onChange={() => setAuthMode('keypair')}
                    />
                    Key Pair
                  </label>
                </div>
              </div>

              {authMode === 'password' ? (
                <InputField
                  label="Password / Token"
                  icon={Lock}
                  type="password"
                  value={formData.password || formData.token || ''}
                  onChange={(e: React.ChangeEvent<HTMLInputElement>) => setFormData({
                    ...formData,
                    password: e.target.value,
                    token: undefined,
                    privateKeyPath: undefined,
                    privateKeyPassphrase: undefined,
                  })}
                  placeholder="Password or JWT/OAuth token"
                  required
                />
              ) : (
                <>
                  <InputField
                    label="Private Key Path"
                    icon={Lock}
                    type="text"
                    value={formData.privateKeyPath || ''}
                    onChange={(e: React.ChangeEvent<HTMLInputElement>) => setFormData({
                      ...formData,
                      privateKeyPath: e.target.value,
                      password: undefined,
                      token: undefined,
                    })}
                    placeholder="C:/path/to/piqlens_private_key_pk8.pem"
                    required
                  />
                  <InputField
                    label="Private Key Passphrase (Optional)"
                    icon={Lock}
                    type="password"
                    value={formData.privateKeyPassphrase || ''}
                    onChange={(e: React.ChangeEvent<HTMLInputElement>) => setFormData({ ...formData, privateKeyPassphrase: e.target.value })}
                    placeholder="Only if your private key is encrypted"
                  />
                </>
              )}

              <InputField
                label="Role (Optional)"
                icon={User}
                type="text"
                value={formData.role || ''}
                onChange={(e: React.ChangeEvent<HTMLInputElement>) => setFormData({ ...formData, role: e.target.value })}
                placeholder="PIQLENS_ENGINEER_ROLE (or leave blank)"
              />

              <div className="bg-blue-50 border border-blue-200 p-3 rounded-md mb-4 text-xs text-blue-800">
                <p><strong>💡 Next Steps:</strong> After connecting, select your warehouse, database, and schema from the sidebar to start viewing data quality metrics.</p>
              </div>

              <div className="flex justify-end pt-2 gap-2">
                <button
                  type="button"
                  onClick={onClose}
                  disabled={isConnecting}
                  className="px-4 py-2 text-sm font-medium text-slate-700 bg-white border border-slate-300 rounded-md hover:bg-slate-50 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-indigo-500 disabled:opacity-50 disabled:cursor-not-allowed"
                >
                  Cancel
                </button>
                <button
                  type="submit"
                  disabled={isConnecting}
                  className="px-4 py-2 text-sm font-medium text-white bg-slate-900 rounded-md hover:bg-slate-800 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-slate-500 disabled:opacity-50 disabled:cursor-not-allowed"
                >
                  {isConnecting ? 'Connecting...' : 'Connect'}
                </button>
              </div>
            </form>
          </div>
        </div>
      </div>
    </div>
  );
};
