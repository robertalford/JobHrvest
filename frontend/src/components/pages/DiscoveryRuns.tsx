import { Search } from 'lucide-react';
import { RunLogsPage } from './RunLogsPage';

export function DiscoveryRuns() {
  return (
    <RunLogsPage
      title="Discovery Runs"
      description="Runs that harvest new company career page links from aggregator sites (Indeed AU, LinkedIn)"
      crawlType="discovery"
      runType="discovery"
      icon={Search}
      iconColor="#f59e0b"
    />
  );
}
