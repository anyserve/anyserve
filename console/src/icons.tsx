import type { SVGProps } from "react";

type IconProps = SVGProps<SVGSVGElement>;

function IconFrame({ children, ...rest }: IconProps) {
  return (
    <svg
      viewBox="0 0 16 16"
      width={16}
      height={16}
      fill="none"
      stroke="currentColor"
      strokeWidth={1.45}
      strokeLinecap="round"
      strokeLinejoin="round"
      aria-hidden="true"
      {...rest}
    >
      {children}
    </svg>
  );
}

export function AnyServeLogo() {
  return (
    <svg viewBox="0 0 24 24" width={19} height={19} aria-hidden="true">
      <rect x={2} y={2} width={20} height={20} rx={5.5} fill="currentColor" opacity={0.12} />
      <path d="M12 5.75 6.4 18h2.65l1.15-2.65h3.6L14.95 18h2.65L12 5.75Zm-1.05 7.65L12 10.7l1.05 2.7h-2.1Z" fill="currentColor" />
    </svg>
  );
}

export const IconOverview = (p: IconProps) => (
  <IconFrame {...p}>
    <rect x={2.25} y={2.25} width={4.75} height={4.75} rx={1.1} />
    <rect x={9} y={2.25} width={4.75} height={4.75} rx={1.1} />
    <rect x={2.25} y={9} width={4.75} height={4.75} rx={1.1} />
    <rect x={9} y={9} width={4.75} height={4.75} rx={1.1} />
  </IconFrame>
);

export const IconJobs = (p: IconProps) => (
  <IconFrame {...p}>
    <path d="M3 4.25h10M3 8h10M3 11.75h6.5" />
    <circle cx={12.25} cy={11.75} r={1.25} />
  </IconFrame>
);

export const IconInterfaces = (p: IconProps) => (
  <IconFrame {...p}>
    <circle cx={3.5} cy={8} r={1.5} />
    <circle cx={12.5} cy={4} r={1.5} />
    <circle cx={12.5} cy={12} r={1.5} />
    <path d="M5 8h2.25c1.2 0 1.5-4 3.75-4M7.25 8c1.2 0 1.5 4 3.75 4" />
  </IconFrame>
);

export const IconPools = (p: IconProps) => (
  <IconFrame {...p}>
    <path d="M8 2.25 13.5 5 8 7.75 2.5 5 8 2.25Z" />
    <path d="M2.5 8 8 10.75 13.5 8M2.5 11 8 13.75 13.5 11" />
  </IconFrame>
);

export const IconWorkers = (p: IconProps) => (
  <IconFrame {...p}>
    <rect x={2.25} y={2.5} width={11.5} height={4.25} rx={1.2} />
    <rect x={2.25} y={9.25} width={11.5} height={4.25} rx={1.2} />
    <path d="M4.75 4.6h.01M4.75 11.4h.01M8 4.6h3.25M8 11.4h3.25" />
  </IconFrame>
);

export const IconExplore = (p: IconProps) => (
  <IconFrame {...p}>
    <rect x={2.25} y={2.75} width={11.5} height={10.5} rx={1.75} />
    <path d="M4.75 6.25 7 8l-2.25 1.75M8.5 10h2.75" />
  </IconFrame>
);

export const IconSun = (p: IconProps) => (
  <IconFrame {...p}>
    <circle cx={8} cy={8} r={2.75} />
    <path d="M8 2.2v1.3M8 12.5v1.3M2.2 8h1.3M12.5 8h1.3M3.9 3.9l.95.95M11.15 11.15l.95.95M12.1 3.9l-.95.95M4.85 11.15l-.95.95" />
  </IconFrame>
);

export const IconMoon = (p: IconProps) => (
  <IconFrame {...p}>
    <path d="M12.75 9.8A4.75 4.75 0 0 1 6.2 3.25a5.25 5.25 0 1 0 6.55 6.55Z" />
  </IconFrame>
);

export const IconDisconnect = (p: IconProps) => (
  <IconFrame {...p}>
    <path d="M6.25 2.75H4a1.25 1.25 0 0 0-1.25 1.25v8A1.25 1.25 0 0 0 4 13.25h2.25M10 10.75 13 8l-3-2.75M6 8h7" />
  </IconFrame>
);

export const IconRefresh = (p: IconProps) => (
  <IconFrame width={14} height={14} {...p}>
    <path d="M13 8a5 5 0 1 1-1-3M12 2.5V5H9.5" />
  </IconFrame>
);

export const IconSearch = (p: IconProps) => (
  <IconFrame {...p}>
    <circle cx={7} cy={7} r={4.25} />
    <path d="m10.2 10.2 3.3 3.3" />
  </IconFrame>
);

export const IconArrowRight = (p: IconProps) => (
  <IconFrame {...p}>
    <path d="M3.5 8h9M9 4.5 12.5 8 9 11.5" />
  </IconFrame>
);

export const IconPlus = (p: IconProps) => (
  <IconFrame {...p}>
    <path d="M8 3.25v9.5M3.25 8h9.5" />
  </IconFrame>
);

export const IconCopy = (p: IconProps) => (
  <IconFrame {...p}>
    <rect x={5.5} y={5.5} width={7.75} height={7.75} rx={1.4} />
    <path d="M10.5 5.5V4a1.25 1.25 0 0 0-1.25-1.25H4A1.25 1.25 0 0 0 2.75 4v5.25A1.25 1.25 0 0 0 4 10.5h1.5" />
  </IconFrame>
);

export const IconCheck = (p: IconProps) => (
  <IconFrame {...p}>
    <path d="m3.5 8.5 3 3 6-6.5" />
  </IconFrame>
);

export const IconClose = (p: IconProps) => (
  <IconFrame {...p}>
    <path d="m4 4 8 8M12 4l-8 8" />
  </IconFrame>
);
