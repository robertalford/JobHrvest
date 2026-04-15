import { BrowserRouter, Routes, Route, Navigate, useParams } from 'react-router-dom';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { Layout } from './components/layout/Layout';
import { Login } from './components/pages/Login';
import { Overview } from './components/pages/Overview';
import { SectionLanding } from './components/pages/SectionLanding';
import { SectionDisabled } from './components/pages/SectionDisabled';
import { isLoggedIn } from './lib/auth';
import { getSectionById, type SectionId } from './lib/sections';
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
import { Geocoder } from './components/pages/Geocoder';
import { Models } from './components/pages/Models';
import { TestData } from './components/pages/TestData';
import { BulkDomainProcessor } from './components/pages/BulkDomainProcessor';
import { PerplexityPlaceholder } from './components/pages/PerplexityPlaceholder';
import { PerplexityV2 } from './components/pages/PerplexityV2';

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

// Render the inner element only if the given section is enabled — otherwise
// show the "paused" notice. Lets deep links to disabled sections land softly.
function SectionGate({ id, children }: { id: SectionId; children: React.ReactNode }) {
  const section = getSectionById(id);
  if (!section) return <Navigate to="/" replace />;
  if (!section.enabled) return <SectionDisabled section={section} />;
  return <>{children}</>;
}

// Redirects legacy top-level URLs (pre-redesign bookmarks) to their new
// section-scoped path, so existing links keep working.
function LegacyRedirect({ to }: { to: string }) {
  const params = useParams();
  let target = to;
  for (const [k, v] of Object.entries(params)) {
    if (v) target = target.replace(`:${k}`, v);
  }
  return <Navigate to={target} replace />;
}

export default function App() {
  return (
    <QueryClientProvider client={queryClient}>
      <BrowserRouter>
        <Routes>
          <Route path="/login" element={<Login />} />
          <Route element={<RequireAuth><Layout /></RequireAuth>}>
            {/* Top-level landing — 3 section cards */}
            <Route path="/" element={<SectionLanding />} />

            {/* Site Config section (always enabled) */}
            <Route path="/site-config" element={<Navigate to="/site-config/bulk-process" replace />} />
            <Route path="/site-config/bulk-process"       element={<BulkDomainProcessor />} />
            <Route path="/site-config/models"             element={<Models />} />
            <Route path="/site-config/test-data"          element={<TestData />} />
            <Route path="/site-config/sites"              element={<CareerPages />} />
            <Route path="/site-config/site-config-runs"   element={<SiteConfigRuns />} />
            <Route path="/site-config/company-config-runs" element={<CompanyConfigRuns />} />
            <Route path="/site-config/excluded-sites"     element={<ExcludedSites />} />

            {/* Extraction section (feature-flagged) */}
            <Route path="/extraction"                   element={<SectionGate id="extraction"><Overview /></SectionGate>} />
            <Route path="/extraction/companies"         element={<SectionGate id="extraction"><Companies /></SectionGate>} />
            <Route path="/extraction/jobs"              element={<SectionGate id="extraction"><Jobs /></SectionGate>} />
            <Route path="/extraction/crawl"             element={<SectionGate id="extraction"><CrawlMonitor /></SectionGate>} />
            <Route path="/extraction/crawl-schedule"    element={<SectionGate id="extraction"><CrawlSchedule /></SectionGate>} />
            <Route path="/extraction/duplicates"        element={<SectionGate id="extraction"><Duplicates /></SectionGate>} />
            <Route path="/extraction/job-quality"       element={<SectionGate id="extraction"><JobQuality /></SectionGate>} />
            <Route path="/extraction/banned-jobs"       element={<SectionGate id="extraction"><BannedJobs /></SectionGate>} />
            <Route path="/extraction/bad-words"         element={<SectionGate id="extraction"><BadWords /></SectionGate>} />
            <Route path="/extraction/scam-words"        element={<SectionGate id="extraction"><ScamWords /></SectionGate>} />

            {/* Discovery section (feature-flagged) */}
            <Route path="/discovery"                  element={<SectionGate id="discovery"><DiscoverySources /></SectionGate>} />
            <Route path="/discovery/runs"             element={<SectionGate id="discovery"><DiscoveryRuns /></SectionGate>} />
            <Route path="/discovery/lead-imports"     element={<SectionGate id="discovery"><LeadImports /></SectionGate>} />
            <Route path="/discovery/domain-import"    element={<SectionGate id="discovery"><DomainImport /></SectionGate>} />
            <Route path="/discovery/markets"          element={<SectionGate id="discovery"><Markets /></SectionGate>} />
            <Route path="/discovery/geocoder"         element={<SectionGate id="discovery"><Geocoder /></SectionGate>} />

            {/* Global — always available regardless of section */}
            <Route
              path="/perplexity-v2"
              element={<PerplexityV2 />}
            />
            <Route
              path="/perplexity-v3"
              element={
                <PerplexityPlaceholder
                  title="Perplexity v3"
                  description="This is a placeholder for Perplexity v3, focused on improving the current Perplexity script and prompt with additional features."
                />
              }
            />
            <Route path="/how-to"   element={<HowTo />} />
            <Route path="/settings" element={<Settings />} />

            {/* Legacy URL redirects (pre-redesign bookmarks) */}
            <Route path="/companies"            element={<LegacyRedirect to="/extraction/companies" />} />
            <Route path="/career-pages"         element={<LegacyRedirect to="/site-config/sites" />} />
            <Route path="/jobs"                 element={<LegacyRedirect to="/extraction/jobs" />} />
            <Route path="/crawl"                element={<LegacyRedirect to="/extraction/crawl" />} />
            <Route path="/crawl-schedule"       element={<LegacyRedirect to="/extraction/crawl-schedule" />} />
            <Route path="/duplicates"           element={<LegacyRedirect to="/extraction/duplicates" />} />
            <Route path="/job-quality"          element={<LegacyRedirect to="/extraction/job-quality" />} />
            <Route path="/banned-jobs"          element={<LegacyRedirect to="/extraction/banned-jobs" />} />
            <Route path="/bad-words"            element={<LegacyRedirect to="/extraction/bad-words" />} />
            <Route path="/scam-words"           element={<LegacyRedirect to="/extraction/scam-words" />} />
            <Route path="/excluded-sites"       element={<LegacyRedirect to="/site-config/excluded-sites" />} />
            <Route path="/site-config-runs"     element={<LegacyRedirect to="/site-config/site-config-runs" />} />
            <Route path="/company-config-runs"  element={<LegacyRedirect to="/site-config/company-config-runs" />} />
            <Route path="/monitor-runs"         element={<LegacyRedirect to="/extraction/crawl" />} />
            <Route path="/discovery-sources"    element={<LegacyRedirect to="/discovery" />} />
            <Route path="/discovery-runs"       element={<LegacyRedirect to="/discovery/runs" />} />
            <Route path="/lead-imports"         element={<LegacyRedirect to="/discovery/lead-imports" />} />
            <Route path="/domain-import"        element={<LegacyRedirect to="/discovery/domain-import" />} />
            <Route path="/markets"              element={<LegacyRedirect to="/discovery/markets" />} />
            <Route path="/geocoder"             element={<LegacyRedirect to="/discovery/geocoder" />} />
            <Route path="/test-data"            element={<LegacyRedirect to="/site-config/test-data" />} />
            <Route path="/models"               element={<LegacyRedirect to="/site-config/models" />} />
          </Route>
        </Routes>
      </BrowserRouter>
    </QueryClientProvider>
  );
}
