import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

import { defineConfig } from "vite";

const __dirname = dirname(fileURLToPath(import.meta.url));

export default defineConfig({
  build: {
    outDir: resolve(__dirname, "../agent-core/public"),
    emptyOutDir: true,
    rollupOptions: {
      input: resolve(__dirname, "index.html")
    },
    cssCodeSplit: false
  }
});
