import { useIsFetching } from '@tanstack/react-query';
import { useEffect, useState } from 'react';

/**
 * Full-page white overlay shown only on initial page load (no cached data yet).
 * Background refetches and polling updates are silent — no overlay.
 */
export function QueryLoadingOverlay() {
  // Only count queries that are fetching for the first time (status === 'pending',
  // meaning no data exists yet). Queries with existing data refetching in the
  // background (status === 'success' | 'error') are excluded.
  const isInitialLoading = useIsFetching({
    predicate: (query) => query.state.status === 'pending',
  });
  const [show, setShow] = useState(false);

  useEffect(() => {
    if (isInitialLoading > 0) {
      const timer = setTimeout(() => setShow(true), 150);
      return () => clearTimeout(timer);
    } else {
      setShow(false);
    }
  }, [isInitialLoading]);

  if (!show) return null;

  return (
    <div className="fixed inset-y-0 left-56 right-0 z-50 flex items-center justify-center bg-white">
      <div className="flex flex-col items-center gap-4">
        <div className="relative w-12 h-12">
          <div className="absolute inset-0 rounded-full border-4 border-gray-100" />
          <div className="absolute inset-0 rounded-full border-4 border-transparent border-t-[#0e8136] animate-spin" />
        </div>
        <p className="text-sm font-medium text-gray-500">Loading…</p>
      </div>
    </div>
  );
}
