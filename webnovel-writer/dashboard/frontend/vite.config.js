import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

export default defineConfig({
  plugins: [react()],
  server: {
    proxy: {
      '/api': 'http://127.0.0.1:8765',
    },
  },
  build: {
    outDir: 'dist',
    emptyOutDir: true,
    chunkSizeWarningLimit: 1200,
    rollupOptions: {
      output: {
        manualChunks(id) {
          if (!id.includes('node_modules')) return undefined
          if (
            id.includes('/three/') ||
            id.includes('/three-render-objects/') ||
            id.includes('/three-spritetext/')
          ) {
            return 'three-core'
          }
          if (
            id.includes('/react-force-graph-3d/') ||
            id.includes('/3d-force-graph/') ||
            id.includes('/d3-force-3d/') ||
            id.includes('/ngraph.graph/')
          ) {
            return 'graph3d'
          }
          return undefined
        },
      },
    },
  },
})
