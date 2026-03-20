import { Globe } from 'lucide-react';
import { RunLogsPage } from './RunLogsPage';

export function SiteConfigRuns() {
  return (
    <RunLogsPage
      title="Site Config Runs"
      description="Runs that map job listing structure on career pages — extruct, repeating-block detector, LLM"
      crawlType="site_config"
      runType="site_config"
      icon={Globe}
      iconColor="#0ea5e9"
    />
  );
}
