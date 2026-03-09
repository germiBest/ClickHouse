import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// https://vite.dev/config/
export default defineConfig({
  plugins: [react()],
  base: './', // Use relative paths for assets
  resolve: {
    // Force single instances when using a locally symlinked click-ui
    dedupe: ['react', 'react-dom', 'styled-components'],
  },
  server: {
    proxy: {
      '/s3-proxy': {
        target: 'https://s3.amazonaws.com',
        changeOrigin: true,
        rewrite: (path) => path.replace(/^\/s3-proxy/, ''),
      },
    },
  },
})
