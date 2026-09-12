import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// Dev server proxies /api to cmd/deliverygateway so the dashboard can be
// run with `npm run dev` against a live gateway without a CORS dance; the
// gateway's own /api/summary handler also sets
// Access-Control-Allow-Origin: * for the built/static-served case.
export default defineConfig({
  plugins: [react()],
  server: {
    proxy: {
      "/api": "http://127.0.0.1:8090",
    },
  },
});
