import axios from 'axios';
import { getToken, clearToken } from './auth';

const BASE = import.meta.env.VITE_API_URL || '/api/v1';

export const api = axios.create({ baseURL: BASE });

// Attach JWT + ensure trailing slash on every request
api.interceptors.request.use(config => {
  const token = getToken();
  if (token) config.headers['Authorization'] = `Bearer ${token}`;
  if (config.url) {
    const [path, qs] = config.url.split('?');
    if (!path.endsWith('/')) config.url = path + '/' + (qs ? '?' + qs : '');
  }
  return config;
});

// On 401, clear token and redirect to login
api.interceptors.response.use(
  r => r,
  err => {
    if (err.response?.status === 401 && !err.config?.url?.includes('/auth/login')) {
      clearToken();
      window.location.href = '/login';
    }
    return Promise.reject(err);
  },
);

// Career Pages (Sites)
export const getCareerPages = (params: Record<string, unknown>) =>
  api.get('/career-pages/', { params }).then(r => r.data);
export const getCareerPageDetail = (pageId: string) =>
  api.get(`/career-pages/${pageId}/detail`).then(r => r.data);
export const recrawlCareerPage = (pageId: string) =>
  api.post(`/career-pages/${pageId}/recrawl`).then(r => r.data);
export const exportCareerPages = (params: Record<string, unknown>) => {
  const base = (import.meta.env.VITE_API_URL || '/api/v1');
  const qs = new URLSearchParams(
    Object.entries({ format: 'csv', ...params })
      .filter(([, v]) => v !== undefined && v !== null && v !== '')
      .map(([k, v]) => [k, String(v)])
  ).toString();
  window.open(`${base}/career-pages/export/?${qs}`);
};

// Companies
export const getCompanies = (params?: Record<string, unknown>) =>
  api.get('/companies', { params }).then(r => r.data);
export const exportCompanies = (params: Record<string, unknown>) => {
  const base = (import.meta.env.VITE_API_URL || '/api/v1');
  const qs = new URLSearchParams(
    Object.entries({ format: 'csv', ...params })
      .filter(([, v]) => v !== undefined && v !== null && v !== '')
      .map(([k, v]) => [k, String(v)])
  ).toString();
  window.open(`${base}/companies/export/?${qs}`);
};
export const createCompany = (data: Record<string, unknown>) =>
  api.post('/companies', data).then(r => r.data);
export const getCompany = (id: string) => api.get(`/companies/${id}`).then(r => r.data);
export const triggerCompanyCrawl = (id: string) =>
  api.post(`/companies/${id}/crawl`).then(r => r.data);

// Jobs
export const getJobs = (params?: Record<string, unknown>) =>
  api.get('/jobs', { params }).then(r => r.data);
export const exportJobs = (params: Record<string, unknown>) => {
  const base = (import.meta.env.VITE_API_URL || '/api/v1');
  const qs = new URLSearchParams(
    Object.entries({ format: 'csv', ...params })
      .filter(([, v]) => v !== undefined && v !== null && v !== '')
      .map(([k, v]) => [k, String(v)])
  ).toString();
  window.open(`${base}/jobs/export/?${qs}`);
};
export const getJob = (id: string) => api.get(`/jobs/${id}`).then(r => r.data);
export const getJobStats = () => api.get('/jobs/stats').then(r => r.data);
export const getJobCrawlBreakdown = (params?: Record<string, unknown>) =>
  api.get('/jobs/crawl-breakdown', { params }).then(r => r.data);

// Crawl
export const getCrawlStats = () => api.get('/crawl/stats').then(r => r.data);
export const getCrawlHistory = (page = 1, page_size = 50, status?: string, crawl_type?: string) =>
  api.get('/crawl/history', { params: { page, page_size, status, crawl_type } }).then(r => r.data);
export const getActiveCrawls = () => api.get('/crawl/active').then(r => r.data);
export const triggerFullCrawl = () => api.post('/crawl/trigger-full').then(r => r.data);
export const triggerAggregatorHarvest = () => api.post('/crawl/trigger-harvest').then(r => r.data);
export const triggerRun = (run_type: string) => api.post(`/crawl/trigger/${run_type}`).then(r => r.data);
export const getScheduleSettings = () => api.get('/crawl/schedule-settings').then(r => r.data);
export const updateScheduleSettings = (settings: Record<string, unknown>) => api.put('/crawl/schedule-settings', settings).then(r => r.data);

// Analytics
export const getOverview = () => api.get('/analytics/overview').then(r => r.data);
export const getFieldCoverage = () => api.get('/analytics/field-coverage').then(r => r.data);
export const getExtractionAccuracy = () => api.get('/analytics/extraction-accuracy').then(r => r.data);
export const getTrends = () => api.get('/analytics/trends').then(r => r.data);
export const getQualityDistribution = () => api.get('/analytics/quality-distribution').then(r => r.data);
export const getQualityBySite = () => api.get('/analytics/quality-by-site').then(r => r.data);
export const triggerQualityScoring = () => api.post('/analytics/trigger-quality-scoring').then(r => r.data);

// System
export const getSystemHealth = () => api.get('/health').then(r => r.data);

// Settings
export const getSetting = (key: string) => api.get(`/settings/system/${key}`).then(r => r.data);
export const updateSetting = (key: string, value: unknown) => api.put(`/settings/system/${key}`, value).then(r => r.data);
export const getWordFilters = (filterType: string, page = 1, search = '') => api.get('/settings/word-filters', { params: { filter_type: filterType, page, page_size: 50, search: search || undefined } }).then(r => r.data);
export const createWordFilter = (data: { word: string; filter_type: string; markets: string[] }) => api.post('/settings/word-filters', data).then(r => r.data);
export const updateWordFilter = (id: string, data: { word?: string; markets?: string[] }) => api.put(`/settings/word-filters/${id}`, data).then(r => r.data);
export const deleteWordFilter = (id: string) => api.delete(`/settings/word-filters/${id}`).then(r => r.data);
export const importWordFilters = (filterType: string, file: File) => {
  const fd = new FormData(); fd.append('file', file);
  return api.post(`/settings/word-filters/import?filter_type=${filterType}`, fd, { headers: { 'Content-Type': 'multipart/form-data' } }).then(r => r.data);
};

// Lead Imports
export const getLeadImportSummary = () =>
  api.get('/lead-imports/summary').then(r => r.data);
export const getLeadImports = (params?: Record<string, unknown>) =>
  api.get('/lead-imports/', { params }).then(r => r.data);
export const triggerLeadImport = (params?: { csv_path?: string; limit?: number; country?: string }) =>
  api.post('/lead-imports/trigger', null, { params }).then(r => r.data);

// Lead Import Batches
export const getLeadBatches = (page = 1, page_size = 20) =>
  api.get('/lead-imports/batches', { params: { page, page_size } }).then(r => r.data);
export const uploadLeadBatch = (file: File) => {
  const fd = new FormData(); fd.append('file', file);
  return api.post('/lead-imports/batches/upload', fd, { headers: { 'Content-Type': 'multipart/form-data' } }).then(r => r.data);
};
export const importLeadBatch = (batchId: string) =>
  api.post(`/lead-imports/batches/${batchId}/import`).then(r => r.data);
export const getLeadBatch = (batchId: string) =>
  api.get(`/lead-imports/batches/${batchId}`).then(r => r.data);
export const getLeadBatchLeads = (batchId: string, page = 1, page_size = 50, status?: string, country?: string) =>
  api.get(`/lead-imports/batches/${batchId}/leads`, { params: { page, page_size, status: status || undefined, country: country || undefined } }).then(r => r.data);

// Excluded Sites (unified blocked-domain list)
export const getExcludedSites = (page = 1, page_size = 50, search?: string, country?: string, site_type?: string) =>
  api.get('/excluded-sites/', { params: { page, page_size, search: search || undefined, country: country || undefined, site_type: site_type || undefined } }).then(r => r.data);
export const getExcludedSiteStats = () =>
  api.get('/excluded-sites/stats').then(r => r.data);
export const addExcludedSite = (data: { domain: string; company_name?: string; site_url?: string; reason?: string }) =>
  api.post('/excluded-sites/', data).then(r => r.data);
export const updateExcludedSite = (siteId: string, data: { reason?: string; company_name?: string }) =>
  api.put(`/excluded-sites/${siteId}`, data).then(r => r.data);
export const removeExcludedSite = (siteId: string) =>
  api.delete(`/excluded-sites/${siteId}`).then(r => r.data);

// Review — Quality & Duplicates
export const getQualityQueue = (page = 1, page_size = 20) =>
  api.get('/review/quality-queue', { params: { page, page_size } }).then(r => r.data);
export const submitQualityFeedback = (jobId: string, decision: string) =>
  api.post(`/review/quality/${jobId}/feedback`, null, { params: { decision } }).then(r => r.data);
export const getDuplicateQueue = (page = 1, page_size = 20) =>
  api.get('/review/duplicate-queue', { params: { page, page_size } }).then(r => r.data);
export const submitDuplicateFeedback = (jobId: string, decision: string) =>
  api.post(`/review/duplicate/${jobId}/feedback`, null, { params: { decision } }).then(r => r.data);

// Discovery Sources
export const getDiscoverySources = (params?: Record<string, unknown>) =>
  api.get('/discovery-sources/', { params }).then(r => r.data);
export const createDiscoverySource = (data: { name: string; base_url: string; market: string }) =>
  api.post('/discovery-sources/', data).then(r => r.data);
export const updateDiscoverySource = (id: string, data: Record<string, unknown>) =>
  api.put(`/discovery-sources/${id}`, data).then(r => r.data);
export const deleteDiscoverySource = (id: string) =>
  api.delete(`/discovery-sources/${id}`).then(r => r.data);

// Crawl worker stats
export const getCrawlWorkerStats = () =>
  api.get('/crawl/worker-stats').then(r => r.data);
export const getMarketBreakdown = () =>
  api.get('/analytics/market-breakdown').then(r => r.data);

// Queue stats (persistent run queue depths)
export const getQueueStats = (params?: Record<string, unknown>) =>
  api.get('/crawl/queue-stats', { params }).then(r => r.data);
export const resetStaleQueueItems = (staleAfterMinutes = 120) =>
  api.post(`/crawl/queue/reset-stale?stale_after_minutes=${staleAfterMinutes}`).then(r => r.data);

// Geocoder
export const getGeoStats      = () => api.get('/geocoder/stats/').then(r => r.data);
export const getGeoLocations  = (params?: Record<string, unknown>) =>
  api.get('/geocoder/', { params }).then(r => r.data);
export const getGeoCache      = (params?: Record<string, unknown>) =>
  api.get('/geocoder/cache/', { params }).then(r => r.data);
export const testGeocode      = (text: string, market_code: string) =>
  api.post('/geocoder/test/', { text, market_code }).then(r => r.data);
export const triggerGeoSeed   = (countries?: string[]) =>
  api.post('/geocoder/seed/', countries ? { countries } : {}).then(r => r.data);
export const triggerRetroGeocode = (retry_failed = false) =>
  api.post('/geocoder/retro/', null, { params: { retry_failed } }).then(r => r.data);

export const getLiveTimeline = (minutes = 30) =>
  api.get('/jobs/live-timeline', { params: { minutes } }).then(r => r.data);
