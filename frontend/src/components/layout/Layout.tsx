import { useIsFetching } from '@tanstack/react-query';
import { Outlet } from 'react-router-dom';
import { Sidebar } from './Sidebar';
import { QueryLoadingOverlay } from './QueryLoadingOverlay';

export function Layout() {
  const isLoading = useIsFetching({ predicate: (q) => q.state.status === 'pending' }) > 0;

  return (
    <div className="flex min-h-screen bg-gray-50">
      <Sidebar />
      <main className={`flex-1 relative ${isLoading ? 'overflow-hidden' : 'overflow-auto'}`}>
        <QueryLoadingOverlay />
        <Outlet />
      </main>
    </div>
  );
}
