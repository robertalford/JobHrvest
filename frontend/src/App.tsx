import { BrowserRouter, Routes, Route, Navigate } from 'react-router-dom';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { Layout } from './components/layout/Layout';
import { Login } from './components/pages/Login';
import { Overview } from './components/pages/Overview';
import { isLoggedIn } from './lib/auth';
import { Companies } from './components/pages/Companies';
import { CareerPages } from './components/pages/CareerPages';
import { Jobs } from './components/pages/Jobs';
import { CrawlMonitor } from './components/pages/CrawlMonitor';
import { LeadImports } from './components/pages/LeadImports';
import { DomainImport } from './components/pages/DomainImport';
import { HowTo } from './components/pages/HowTo';
import { Settings } from './components/pages/Settings';
import { BannedJobs } from './components/pages/BannedJobs';
import { ExcludedSites } from './components/pages/ExcludedSites';
import { Markets } from './components/pages/Markets';
import { DiscoverySources } from './components/pages/DiscoverySources';
import { CrawlSchedule } from './components/pages/CrawlSchedule';
import { BadWords } from './components/pages/BadWords';
import { ScamWords } from './components/pages/ScamWords';
import { JobQuality } from './components/pages/JobQuality';
import { Duplicates } from './components/pages/Duplicates';
import { SiteConfigRuns } from './components/pages/SiteConfigRuns';
import { CompanyConfigRuns } from './components/pages/CompanyConfigRuns';
import { DiscoveryRuns } from './components/pages/DiscoveryRuns';
import { MonitorRunsOverview } from './components/pages/MonitorRunsOverview';
import { Geocoder } from './components/pages/Geocoder';
// In dev mode: disable all caching so every navigation fetches fresh data.
// Set VITE_CACHE_BUST=false in .env to restore normal caching for production.
const isDev = import.meta.env.DEV || import.meta.env.VITE_CACHE_BUST !== 'false';
const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      retry: 1,
      staleTime: isDev ? 0 : 30000,
      gcTime: isDev ? 0 : 300000,
      refetchOnWindowFocus: isDev,
    },
  },
});

function RequireAuth({ children }: { children: React.ReactNode }) {
  return isLoggedIn() ? <>{children}</> : <Navigate to="/login" replace />;
}

export default function App() {
  return (
    <QueryClientProvider client={queryClient}>
      <BrowserRouter>
        <Routes>
          <Route path="/login" element={<Login />} />
          <Route element={<RequireAuth><Layout /></RequireAuth>}>
            <Route path="/" element={<Overview />} />
            <Route path="/companies" element={<Companies />} />
            <Route path="/career-pages" element={<CareerPages />} />
            <Route path="/jobs" element={<Jobs />} />
            <Route path="/crawl" element={<CrawlMonitor />} />
            <Route path="/lead-imports" element={<LeadImports />} />
            <Route path="/domain-import" element={<DomainImport />} />
            <Route path="/how-to" element={<HowTo />} />
            <Route path="/settings" element={<Settings />} />
            <Route path="/banned-jobs" element={<BannedJobs />} />
            <Route path="/excluded-sites" element={<ExcludedSites />} />
            <Route path="/markets" element={<Markets />} />
            <Route path="/discovery-sources" element={<DiscoverySources />} />
            <Route path="/crawl-schedule" element={<CrawlSchedule />} />
            <Route path="/bad-words" element={<BadWords />} />
            <Route path="/scam-words" element={<ScamWords />} />
            <Route path="/job-quality" element={<JobQuality />} />
            <Route path="/duplicates" element={<Duplicates />} />
            <Route path="/monitor-runs" element={<MonitorRunsOverview />} />
            <Route path="/site-config-runs" element={<SiteConfigRuns />} />
            <Route path="/company-config-runs" element={<CompanyConfigRuns />} />
            <Route path="/discovery-runs" element={<DiscoveryRuns />} />
            <Route path="/geocoder" element={<Geocoder />} />
          </Route>
        </Routes>
      </BrowserRouter>
    </QueryClientProvider>
  );
}
