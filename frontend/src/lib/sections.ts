/**
 * Section configuration — compartmentalises the app into three top-level
 * feature areas (Site Config, Extraction, Discovery). The landing page shows
 * one card per section; clicking a card routes into that section's sub-app
 * and the Sidebar filters its menu to that section's pages.
 *
 * Sections can be toggled off via Vite env vars (VITE_FEATURE_EXTRACTION,
 * VITE_FEATURE_DISCOVERY). A disabled section renders a "coming soon" card on
 * the landing page and its routes are guarded so deep-linked URLs show a
 * friendly notice instead of a blank page.
 */

import type { ComponentType } from 'react';
import {
  LayoutDashboard, Building2, Briefcase, Globe, Activity,
  Upload, HelpCircle, ShieldX, Ban, Globe2, Search, Clock,
  Type, AlertTriangle, Star, Copy, Info, MapPin, Database,
  Cog, Beaker, Cpu, FileSpreadsheet,
} from 'lucide-react';

export type SectionId = 'site-config' | 'extraction' | 'discovery';

export type NavEntry = {
  to: string;
  icon: ComponentType<{ className?: string }>;
  label: string;
  group?: string;
};

export type Section = {
  id: SectionId;
  path: string;              // URL prefix, e.g. "/site-config"
  title: string;
  tagline: string;           // one-line description for the landing card
  icon: ComponentType<{ className?: string }>;
  accentClass: string;       // tailwind classes for the card accent
  enabled: boolean;
  nav: NavEntry[];
};

// Vite env flags — a section is enabled by default; set the flag to "false"
// (string) to disable it. Absent / any other value = enabled.
const enabledFlag = (v: string | undefined) => v !== 'false';

export const SECTIONS: Section[] = [
  {
    id: 'site-config',
    path: '/site-config',
    title: 'Site Config',
    tagline: 'Train and run the champion model that maps a domain to its job-listing selectors.',
    icon: Cog,
    accentClass: 'bg-brand/10 text-brand',
    enabled: true,
    nav: [
      { group: 'Models',        to: '/site-config/bulk-process',       icon: FileSpreadsheet, label: 'Bulk Domain Processor' },
      { group: 'Models',        to: '/site-config/models',             icon: Cpu,             label: 'Models' },
      { group: 'Data',          to: '/site-config/sites',              icon: Globe,           label: 'Sites' },
      { group: 'Data',          to: '/site-config/test-data',          icon: Beaker,          label: 'Test Data' },
      { group: 'Runs',          to: '/site-config/company-config-runs', icon: Building2,      label: 'Company Config Runs' },
      { group: 'Runs',          to: '/site-config/site-config-runs',    icon: Globe,          label: 'Site Config Runs' },
      { group: 'Settings',      to: '/site-config/excluded-sites',     icon: Ban,             label: 'Excluded Sites' },
    ],
  },
  {
    id: 'extraction',
    path: '/extraction',
    title: 'Extraction',
    tagline: 'Scheduled job scraping using the site configs produced by the champion model.',
    icon: Activity,
    accentClass: 'bg-gray-100 text-gray-500',
    enabled: enabledFlag(import.meta.env.VITE_FEATURE_EXTRACTION),
    nav: [
      { group: 'Prod Database', to: '/extraction',                     icon: LayoutDashboard, label: 'Overview' },
      { group: 'Prod Database', to: '/extraction/companies',           icon: Building2,       label: 'Companies' },
      { group: 'Prod Database', to: '/extraction/jobs',                icon: Briefcase,       label: 'Jobs' },
      { group: 'Monitor',       to: '/extraction/crawl',               icon: Activity,        label: 'Site Crawling Runs' },
      { group: 'Schedule',      to: '/extraction/crawl-schedule',      icon: Clock,           label: 'Scheduled Runs' },
      { group: 'Review',        to: '/extraction/duplicates',          icon: Copy,            label: 'Duplicates' },
      { group: 'Review',        to: '/extraction/job-quality',         icon: Star,            label: 'Job Quality' },
      { group: 'Settings',      to: '/extraction/banned-jobs',         icon: ShieldX,         label: 'Banned Jobs' },
      { group: 'Settings',      to: '/extraction/bad-words',           icon: Type,            label: 'Bad Words' },
      { group: 'Settings',      to: '/extraction/scam-words',          icon: AlertTriangle,   label: 'Scam Words' },
    ],
  },
  {
    id: 'discovery',
    path: '/discovery',
    title: 'Domain Discovery',
    tagline: 'Crawl the web to discover new company domains to feed into the Site Config pipeline.',
    icon: Search,
    accentClass: 'bg-gray-100 text-gray-500',
    enabled: enabledFlag(import.meta.env.VITE_FEATURE_DISCOVERY),
    nav: [
      { group: 'Discovery',     to: '/discovery',                      icon: Globe2,          label: 'Link Discovery' },
      { group: 'Runs',          to: '/discovery/runs',                 icon: Search,          label: 'Discovery Runs' },
      { group: 'Import',        to: '/discovery/lead-imports',         icon: Upload,          label: 'Company Import' },
      { group: 'Import',        to: '/discovery/domain-import',        icon: Database,        label: 'Bulk Domain Import' },
      { group: 'Config',        to: '/discovery/markets',              icon: Globe2,          label: 'Live Markets' },
      { group: 'Config',        to: '/discovery/geocoder',             icon: MapPin,          label: 'Geocoder' },
    ],
  },
];

// Nav entries always visible, regardless of active section.
export const GLOBAL_NAV: NavEntry[] = [
  { to: '/how-to',   icon: HelpCircle, label: 'How To' },
  { to: '/settings', icon: Info,       label: 'System Health' },
];

export function getSectionByPath(pathname: string): Section | undefined {
  return SECTIONS.find(s => pathname === s.path || pathname.startsWith(s.path + '/'));
}

export function getSectionById(id: SectionId): Section | undefined {
  return SECTIONS.find(s => s.id === id);
}
