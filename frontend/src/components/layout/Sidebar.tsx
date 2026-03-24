import { NavLink, useNavigate } from 'react-router-dom';
import {
  LayoutDashboard, Building2, Briefcase, Globe, Activity,
  Upload, HelpCircle, ShieldX, Ban, Globe2, Search, Clock,
  Type, AlertTriangle, Star, Copy, Info, MapPin, Database,
  LogOut,
} from 'lucide-react';
import logoImg from '/logo.png';
import { clearToken } from '../../lib/auth';

type NavItem = { to: string; icon: React.ElementType; label: string };

function NavItem({ to, icon: Icon, label }: NavItem) {
  return (
    <NavLink
      to={to}
      end={to === '/'}
      className={({ isActive }) =>
        `flex items-center gap-3 px-3 py-1.5 rounded-md text-sm font-medium transition-colors ${
          isActive ? 'bg-brand/10 text-brand' : 'text-gray-600 hover:bg-gray-100 hover:text-gray-900'
        }`
      }
    >
      <Icon className="w-4 h-4 flex-shrink-0" />
      {label}
    </NavLink>
  );
}

function SectionLabel({ label }: { label: string }) {
  return (
    <div className="px-3 pt-4 pb-1">
      <span className="text-[10px] font-bold uppercase tracking-widest text-gray-400">{label}</span>
    </div>
  );
}

export function Sidebar() {
  const navigate = useNavigate();

  function handleLogout() {
    clearToken();
    navigate('/login');
  }

  return (
    <aside className="w-56 bg-white border-r border-gray-200 flex flex-col min-h-screen">
      <div className="p-4 border-b border-gray-200">
        <div className="flex items-center justify-center gap-2">
          <img src={logoImg} alt="JobHarvest" className="w-8 h-8 rounded-lg object-cover" />
          <span className="font-semibold text-sm text-gray-900">JobHarvest</span>
        </div>
      </div>

      <nav className="flex-1 p-2 overflow-y-auto">
        {/* Prod Database */}
        <SectionLabel label="Prod Database" />
        <NavItem to="/" icon={LayoutDashboard} label="Overview" />
        <NavItem to="/discovery-sources" icon={Globe2} label="Link Discovery" />
        <NavItem to="/companies" icon={Building2} label="Companies" />
        <NavItem to="/career-pages" icon={Globe} label="Sites" />
        <NavItem to="/jobs" icon={Briefcase} label="Jobs" />

        {/* Monitor Runs */}
        <SectionLabel label="Monitor Runs" />
        <NavItem to="/monitor-runs" icon={LayoutDashboard} label="Overview" />
        <NavItem to="/discovery-runs" icon={Search} label="Discovery Runs" />
        <NavItem to="/company-config-runs" icon={Building2} label="Company Config Runs" />
        <NavItem to="/site-config-runs" icon={Globe} label="Site Config Runs" />
        <NavItem to="/crawl" icon={Activity} label="Site Crawling Runs" />

        {/* Settings */}
        <SectionLabel label="Settings" />
        <NavItem to="/lead-imports" icon={Upload} label="Company Import" />
        <NavItem to="/domain-import" icon={Database} label="Bulk Domain Import" />
        <NavItem to="/excluded-sites" icon={Ban} label="Excluded Sites" />
        <NavItem to="/banned-jobs" icon={ShieldX} label="Banned Jobs" />
        <NavItem to="/markets" icon={Globe2} label="Live Markets" />
        <NavItem to="/crawl-schedule" icon={Clock} label="Scheduled Runs" />
        <NavItem to="/geocoder" icon={MapPin} label="Geocoder" />
        <NavItem to="/bad-words" icon={Type} label="Bad Words" />
        <NavItem to="/scam-words" icon={AlertTriangle} label="Scam Words" />

        {/* Review & Train */}
        <SectionLabel label="Review & Train Model" />
        <NavItem to="/duplicates" icon={Copy} label="Duplicates" />
        <NavItem to="/job-quality" icon={Star} label="Job Quality" />

        {/* More */}
        <SectionLabel label="More" />
        <NavItem to="/how-to" icon={HelpCircle} label="How To" />
        <NavItem to="/settings" icon={Info} label="System Health" />
      </nav>

      <div className="p-3 border-t border-gray-200 space-y-2">
        <button
          onClick={handleLogout}
          className="flex items-center gap-2 w-full px-3 py-1.5 rounded-md text-sm font-medium text-gray-500 hover:bg-gray-100 hover:text-gray-900 transition-colors"
        >
          <LogOut className="w-4 h-4" />
          Log out
        </button>
        <div className="text-xs text-gray-400 text-center">v0.2.0</div>
      </div>
    </aside>
  );
}
