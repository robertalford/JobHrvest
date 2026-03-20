import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    host: true,          // bind 0.0.0.0 so the container port is reachable
    watch: {
      usePolling: true,  // required on macOS/Colima — inotify doesn't propagate through bind mounts
      interval: 300,
    },
    proxy: {
      '/api': 'http://api:8000',  // Docker service hostname
    },
  },
})
