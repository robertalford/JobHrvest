import { BrowserRouter, Routes, Route } from 'react-router-dom';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { Layout } from './components/layout/Layout';
import { Dashboard } from './components/pages/Dashboard';
import { Companies } from './components/pages/Companies';
import { CareerPages } from './components/pages/CareerPages';
import { Jobs } from './components/pages/Jobs';
import { CrawlMonitor } from './components/pages/CrawlMonitor';
import { Analytics } from './components/pages/Analytics';
import { Settings } from './components/pages/Settings';

const queryClient = new QueryClient({
  defaultOptions: { queries: { retry: 1, staleTime: 30000 } },
});

export default function App() {
  return (
    <QueryClientProvider client={queryClient}>
      <BrowserRouter>
        <Routes>
          <Route element={<Layout />}>
            <Route path="/" element={<Dashboard />} />
            <Route path="/companies" element={<Companies />} />
            <Route path="/career-pages" element={<CareerPages />} />
            <Route path="/jobs" element={<Jobs />} />
            <Route path="/crawl" element={<CrawlMonitor />} />
            <Route path="/analytics" element={<Analytics />} />
            <Route path="/settings" element={<Settings />} />
          </Route>
        </Routes>
      </BrowserRouter>
    </QueryClientProvider>
  );
}
