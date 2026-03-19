import "dotenv/config";

const required = ["SUPABASE_URL", "SUPABASE_SERVICE_ROLE_KEY"] as const;

function requireEnv(): Record<(typeof required)[number], string> {
  const out: Record<string, string> = {};
  for (const key of required) {
    const v = process.env[key];
    if (v === undefined || v.trim() === "") {
      console.error(`Missing required env: ${key}`);
      process.exit(1);
    }
    out[key] = v;
  }
  return out as Record<(typeof required)[number], string>;
}

const env = requireEnv();

export const config = {
  supabase: {
    url: env.SUPABASE_URL,
    serviceRoleKey: env.SUPABASE_SERVICE_ROLE_KEY,
  },
  ewg: {
    baseUrl: process.env.EWG_BASE_URL ?? "https://www.ewg.org",
    rateLimitMs: Math.max(1000, Number(process.env.EWG_RATE_LIMIT_MS) || 2000),
  },
} as const;
