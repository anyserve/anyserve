import { type ConsoleTab } from "./app-constants";
import { formatNumber, formatTimestamp } from "./format";
import { t, type Lang, type Theme } from "./i18n";
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
  lang: Lang;
  onToggleLang: () => void;
  theme: Theme;
  onToggleTheme: () => void;
  authenticated: boolean;
  onDisconnect: () => void;
}

const TAB_GROUPS: Array<{
  groupKey: "nav.group.monitor" | "nav.group.routing" | "nav.group.analytics";
  tabs: ConsoleTab[];
}> = [
  { groupKey: "nav.group.monitor", tabs: ["overview", "jobs"] },
  { groupKey: "nav.group.routing", tabs: ["interfaces", "pools", "workers"] },
  { groupKey: "nav.group.analytics", tabs: ["explore"] },
];

const TAB_META: Record<
  ConsoleTab,
  {
    labelKey:
      | "nav.overview"
      | "nav.jobs"
      | "nav.interfaces"
      | "nav.pools"
      | "nav.workers"
      | "nav.explore";
    Icon: typeof IconOverview;
  }
> = {
  overview: { labelKey: "nav.overview", Icon: IconOverview },
  jobs: { labelKey: "nav.jobs", Icon: IconJobs },
  interfaces: { labelKey: "nav.interfaces", Icon: IconInterfaces },
  pools: { labelKey: "nav.pools", Icon: IconPools },
  workers: { labelKey: "nav.workers", Icon: IconWorkers },
  explore: { labelKey: "nav.explore", Icon: IconExplore },
};

function badgeForTab(tab: ConsoleTab, overview: OverviewResponse | null): string | null {
  if (!overview) return null;
  switch (tab) {
    case "jobs":
      return overview.queue.active > 0 ? formatNumber(overview.queue.active) : null;
    case "interfaces":
      return overview.interfaces.length > 0 ? formatNumber(overview.interfaces.length) : null;
    case "pools":
      return overview.pools && overview.pools.length > 0
        ? formatNumber(overview.pools.length)
        : null;
    case "workers":
      return `${overview.workers.healthy}/${overview.workers.total}`;
    default:
      return null;
  }
}

export function Sidebar(props: SidebarProps) {
  const {
    activeTab,
    onSelectTab,
    overview,
    lang,
    onToggleLang,
    theme,
    onToggleTheme,
    authenticated,
    onDisconnect,
  } = props;

  const health = overview?.health;
  const healthTone = !overview
    ? "offline"
    : health
      ? !health.live
        ? "bad"
        : !health.ready || health.journal.status === "unavailable"
          ? "warn"
          : "ok"
      : "ok";

  return (
    <aside className="sidebar" aria-label="Console navigation">
      <div className="sidebar__top">
        <div className="sidebar__brand">
          <span className="sidebar__logo">
            <AnyserveLogo />
          </span>
          <span className="sidebar__title-group">
            <span className="sidebar__title">Anyserve</span>
            <span className="sidebar__subtitle">{t(lang, "brand.subtitle")}</span>
          </span>
        </div>
      </div>

      <div className="sidebar__scroll">
        <nav className="sidebar__nav" aria-label="Console sections">
          {TAB_GROUPS.map((group) => (
            <div key={group.groupKey} className="sidebar__group">
              <div className="sidebar__group-label">{t(lang, group.groupKey)}</div>
              {group.tabs.map((tab) => {
                const { labelKey, Icon } = TAB_META[tab];
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
                    <span className="sidebar__label">{t(lang, labelKey)}</span>
                    {badge ? <span className="sidebar__badge">{badge}</span> : null}
                  </button>
                );
              })}
            </div>
          ))}
        </nav>
      </div>

      <div className="sidebar__foot">
        <div className="sidebar__status">
          <span className={`status-dot status-dot--${healthTone}`} aria-hidden="true" />
          <div className="sidebar__status-text">
            <span className="sidebar__runtime">
              {overview?.runtime.label ?? t(lang, "sidebar.notConnected")}
            </span>
            <span className="sidebar__meta">
              {overview?.control_plane ? (
                <>
                  {t(lang, "sidebar.leaderGen")} #{overview.control_plane.generation}
                  {" · "}
                </>
              ) : null}
              {overview
                ? `${t(lang, "sidebar.refreshed")} ${formatTimestamp(overview.refreshed_at_ms)}`
                : "—"}
            </span>
          </div>
        </div>

        <div className="sidebar__controls">
          <button
            type="button"
            className="ctrl-pill"
            title={t(lang, "sidebar.lang.title")}
            aria-label={t(lang, "sidebar.lang.title")}
            onClick={onToggleLang}
          >
            {lang === "en" ? "EN / 中文" : "中文 / EN"}
          </button>

          <button
            type="button"
            className="ctrl-pill"
            title={t(lang, "sidebar.theme.title")}
            aria-label={t(lang, "sidebar.theme.title")}
            onClick={onToggleTheme}
          >
            {theme === "dark" ? <IconSun /> : <IconMoon />}
            <span>{t(lang, theme === "dark" ? "sidebar.theme.light" : "sidebar.theme.dark")}</span>
          </button>

          {authenticated ? (
            <button
              type="button"
              className="ctrl-pill ctrl-pill--danger"
              title={t(lang, "sidebar.disconnect")}
              aria-label={t(lang, "sidebar.disconnect")}
              onClick={onDisconnect}
            >
              <IconDisconnect />
              <span>{t(lang, "sidebar.disconnect")}</span>
            </button>
          ) : null}
        </div>
      </div>
    </aside>
  );
}
