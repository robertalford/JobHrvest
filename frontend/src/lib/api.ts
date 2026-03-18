import axios from 'axios';

const BASE = import.meta.env.VITE_API_URL || '/api/v1';

export const api = axios.create({ baseURL: BASE });

// Companies
export const getCompanies = (params?: Record<string, unknown>) =>
  api.get('/companies', { params }).then(r => r.data);
export const createCompany = (data: Record<string, unknown>) =>
  api.post('/companies', data).then(r => r.data);
export const getCompany = (id: string) => api.get(`/companies/${id}`).then(r => r.data);
export const triggerCompanyCrawl = (id: string) =>
  api.post(`/companies/${id}/crawl`).then(r => r.data);

// Jobs
export const getJobs = (params?: Record<string, unknown>) =>
  api.get('/jobs', { params }).then(r => r.data);
export const getJob = (id: string) => api.get(`/jobs/${id}`).then(r => r.data);
export const getJobStats = () => api.get('/jobs/stats').then(r => r.data);

// Crawl
export const getCrawlHistory = (limit = 50) =>
  api.get('/crawl/history', { params: { limit } }).then(r => r.data);
export const getActiveCrawls = () => api.get('/crawl/active').then(r => r.data);
export const triggerFullCrawl = () => api.post('/crawl/trigger-full').then(r => r.data);

// Analytics
export const getFieldCoverage = () => api.get('/analytics/field-coverage').then(r => r.data);
export const getExtractionAccuracy = () => api.get('/analytics/extraction-accuracy').then(r => r.data);
export const getTrends = () => api.get('/analytics/trends').then(r => r.data);

// System
export const getSystemHealth = () => api.get('/health').then(r => r.data);
