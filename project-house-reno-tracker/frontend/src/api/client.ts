import axios from 'axios'

export const apiClient = axios.create({
  baseURL: (import.meta.env.VITE_API_BASE_URL ?? 'http://localhost:8003') + '/api/v1',
  timeout: 30_000,
})
