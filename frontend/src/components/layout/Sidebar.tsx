import { NavLink, useLocation, Link, useNavigate } from 'react-router-dom';
import { Home, LogOut } from 'lucide-react';
import logoImg from '/logo.png';
import type { NavEntry } from '../../lib/sections';
import { GLOBAL_NAV, getSectionByPath } from '../../lib/sections';
import { clearToken } from '../../lib/auth';

function NavItemLink({ to, icon: Icon, label, end }: NavEntry & { end?: boolean }) {
  return (
    <NavLink
      to={to}
      end={end}
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

function groupEntries(entries: NavEntry[]): Array<{ group: string; items: NavEntry[] }> {
  const groups: Array<{ group: string; items: NavEntry[] }> = [];
  for (const item of entries) {
    const g = item.group || '';
    const last = groups[groups.length - 1];
    if (last && last.group === g) last.items.push(item);
    else groups.push({ group: g, items: [item] });
  }
  return groups;
}

export function Sidebar() {
  const { pathname } = useLocation();
  const navigate = useNavigate();
  const section = getSectionByPath(pathname);
  const groups = section ? groupEntries(section.nav) : [];

  function handleLogout() {
    clearToken();
    navigate('/login');
  }

  return (
    <aside className="w-56 bg-white border-r border-gray-200 flex flex-col min-h-screen">
      <Link to="/" className="p-4 border-b border-gray-200 block hover:bg-gray-50 transition-colors">
        <div className="flex items-center justify-center gap-2">
          <img src={logoImg} alt="JobHarvest" className="w-8 h-8 rounded-lg object-cover" />
          <span className="font-semibold text-sm text-gray-900">JobHarvest</span>
        </div>
      </Link>

      <nav className="flex-1 p-2 overflow-y-auto">
        <NavItemLink to="/" icon={Home} label="Home" end />

        {section && (
          <>
            <div className="px-3 pt-4 pb-1">
              <span className="text-[11px] font-semibold uppercase tracking-wider text-brand">
                {section.title}
              </span>
            </div>
            {groups.map(({ group, items }, idx) => (
              <div key={`${group}-${idx}`}>
                {group && <SectionLabel label={group} />}
                {items.map((item) => (
                  <NavItemLink key={item.to} {...item} />
                ))}
              </div>
            ))}
          </>
        )}

        <SectionLabel label="More" />
        {GLOBAL_NAV.map((item) => (
          <NavItemLink key={item.to} {...item} />
        ))}
      </nav>

      <div className="p-3 border-t border-gray-200 space-y-2">
        <button
          onClick={handleLogout}
          className="flex items-center gap-2 w-full px-3 py-1.5 rounded-md text-sm font-medium text-gray-500 hover:bg-gray-100 hover:text-gray-900 transition-colors"
        >
          <LogOut className="w-4 h-4" />
          Log out
        </button>
        <div className="text-xs text-gray-400 text-center">v0.3.0</div>
      </div>
    </aside>
  );
}
