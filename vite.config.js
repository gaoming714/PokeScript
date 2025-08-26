import { fileURLToPath, URL } from 'node:url'

import { defineConfig } from 'vite'
import vue from '@vitejs/plugin-vue'
import vueDevTools from 'vite-plugin-vue-devtools'
import vuetify from 'vite-plugin-vuetify'


// https://vite.dev/config/
export default defineConfig({
  plugins: [
    vue(),
    vueDevTools(),
    vuetify({ autoImport: true }) ,
  ],
  resolve: {
    alias: {
      '@': fileURLToPath(new URL('./src', import.meta.url))
    },
  },
  server: {
      host: '0.0.0.0',
      port: 8000,
    // proxy: {
    //   // 匹配以 /api 开头的请求路径
    //   '/api': {
    //     target: 'http://127.0.0.1:5000', // Flask API 的地址
    //     changeOrigin: true, // 更改请求头中的 origin 为目标地址
    //     // rewrite: (path) => path.replace(/^\/api/, ''), // 去掉路径中的 /api 前缀
    //   },
    //   '/socket.io': {
    //     target: 'http://127.0.0.1:5000',
    //     ws: true,
    //     changeOrigin: true, // 更改请求头中的 origin 为目标地址
    //   },

    // },
  },
})

