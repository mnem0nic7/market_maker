const apiProxyTarget = (
  process.env.API_PROXY_TARGET?.trim()
  || (process.env.NODE_ENV === "production"
    ? "http://trading-core:8080"
    : "http://localhost:8080")
).replace(/\/$/, "");

/** @type {import('next').NextConfig} */
const nextConfig = {
  output: "standalone",
  async rewrites() {
    return [
      {
        source: "/api/:path*",
        destination: `${apiProxyTarget}/api/:path*`,
      },
    ];
  },
};

export default nextConfig;
