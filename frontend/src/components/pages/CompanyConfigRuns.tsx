import { Building2 } from 'lucide-react';
import { RunLogsPage } from './RunLogsPage';

export function CompanyConfigRuns() {
  return (
    <RunLogsPage
      title="Company Config Runs"
      description="Runs that identify or re-identify career page URLs for companies — ATS fingerprint, heuristic BFS, LLM"
      crawlType="company_config"
      runType="company_config"
      icon={Building2}
      iconColor="#8b5cf6"
    />
  );
}
