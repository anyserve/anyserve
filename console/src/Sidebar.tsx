import type { ConsoleTab, Theme } from "./app-constants";
import { formatNumber, formatShortTime, formatTimestamp } from "./format";
import {
  AnyserveLogo,
  IconDisconnect,
  IconExplore,
  IconInterfaces,
  IconJobs,
  IconMoon,
  IconOverview,
  IconPools,
  IconSun,
  IconWorkers,
} from "./icons";
import type { OverviewResponse } from "./types";

interface SidebarProps {
  activeTab: ConsoleTab;
  onSelectTab: (tab: ConsoleTab) => void;
  overview: OverviewResponse | null;
  theme: Theme;
  onToggleTheme: () => void;
  authenticated: boolean;
  onDisconnect: () => void;
}

const NAV_GROUPS: Array<{
  label: string;
  items: Array<{ tab: ConsoleTab; label: string; Icon: typeof IconOverview }>;
}> = [
  {
    label: "Monitor",
    items: [
      { tab: "overview", label: "Overview", Icon: IconOverview },
      { tab: "jobs", label: "Jobs", Icon: IconJobs },
    ],
  },
  {
    label: "Routing & Fleet",
    items: [
      { tab: "interfaces", label: "Interfaces", Icon: IconInterfaces },
      { tab: "pools", label: "Pools", Icon: IconPools },
      { tab: "workers", label: "Workers", Icon: IconWorkers },
    ],
  },
  {
    label: "Analytics",
    items: [{ tab: "explore", label: "Explore", Icon: IconExplore }],
  },
];

function badgeForTab(tab: ConsoleTab, overview: OverviewResponse | null): string | null {
  if (!overview) return null;
  if (tab === "jobs") return overview.queue.active > 0 ? formatNumber(overview.queue.active) : null;
  if (tab === "interfaces") return overview.interfaces.length > 0 ? formatNumber(overview.interfaces.length) : null;
  if (tab === "pools") return overview.pools && overview.pools.length > 0 ? formatNumber(overview.pools.length) : null;
  if (tab === "workers") return `${overview.workers.healthy}/${overview.workers.total}`;
  return null;
}

export function Sidebar({ activeTab, onSelectTab, overview, theme, onToggleTheme, authenticated, onDisconnect }: SidebarProps) {
  const health = overview?.health;
  const tone = !overview ? "offline" : health && !health.live ? "bad" : health && (!health.ready || health.journal.status === "unavailable") ? "warn" : "ok";

  return (
    <aside className="sidebar" aria-label="Console navigation">
      <div className="sidebar__top">
        <div className="sidebar__brand">
          <span className="sidebar__logo">
            <AnyserveLogo />
          </span>
          <div className="sidebar__title-group">
            <span className="sidebar__title">Anyserve</span>
            <span className="sidebar__subtitle">Control Plane</span>
          </div>
        </div>
      </div>

      <div className="sidebar__scroll">
        <nav className="sidebar__nav" aria-label="Console sections">
          {NAV_GROUPS.map((group) => (
            <div key={group.label} className="sidebar__group">
              <div className="sidebar__group-label">{group.label}</div>
              {group.items.map(({ tab, label, Icon }) => {
                const badge = badgeForTab(tab, overview);
                const isActive = activeTab === tab;
                return (
                  <button
                    key={tab}
                    type="button"
                    className={`sidebar__item${isActive ? " is-active" : ""}`}
                    aria-current={isActive ? "page" : undefined}
                    onClick={() => onSelectTab(tab)}
                  >
                    <Icon className="sidebar__icon" />
                    <span className="sidebar__label">{label}</span>
                    {badge ? <span className="sidebar__badge">{badge}</span> : null}
                  </button>
                );
              })}
            </div>
          ))}
        </nav>
      </div>

      <div className="sidebar__foot">
        <div className="sidebar__status" title={overview ? formatTimestamp(overview.refreshed_at_ms) : undefined}>
          <span className={`status-dot status-dot--${tone}`} aria-hidden="true" />
          <div className="sidebar__status-text">
            <span className="sidebar__runtime">{overview?.runtime.label ?? "Not connected"}</span>
            <span className="sidebar__meta">
              {overview?.control_plane ? `Gen #${overview.control_plane.generation} · ` : ""}
              {overview ? formatShortTime(overview.refreshed_at_ms) : "—"}
            </span>
          </div>
        </div>

        <div className="sidebar__controls">
          <button type="button" className="ctrl-pill" title="Switch theme" aria-label="Switch theme" onClick={onToggleTheme}>
            {theme === "dark" ? <IconSun /> : <IconMoon />}
            <span>{theme === "dark" ? "Light" : "Dark"}</span>
          </button>

          {authenticated ? (
            <button type="button" className="ctrl-pill ctrl-pill--danger" title="Disconnect" aria-label="Disconnect" onClick={onDisconnect}>
              <IconDisconnect />
              <span>Disconnect</span>
            </button>
          ) : null}
        </div>
      </div>
    </aside>
  );
}
