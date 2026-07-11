import { BrowserRouter, Routes, Route, Navigate } from 'react-router-dom'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { ReactQueryDevtools } from '@tanstack/react-query-devtools'

import AppShell from '@/components/layout/AppShell'
import HomePage from '@/pages/HomePage'
import SpacesPage from '@/pages/SpacesPage'
import SpaceDetailPage from '@/pages/SpaceDetailPage'
import RenovationDetailPage from '@/pages/RenovationDetailPage'
import MaterialsPage from '@/pages/MaterialsPage'

const queryClient = new QueryClient({
  defaultOptions: {
    queries: { staleTime: 30_000, retry: 1 },
  },
})

export default function App() {
  return (
    <QueryClientProvider client={queryClient}>
      <BrowserRouter>
        <Routes>
          <Route element={<AppShell />}>
            <Route index element={<HomePage />} />
            <Route path="spaces" element={<SpacesPage />} />
            <Route path="spaces/:spaceId" element={<SpaceDetailPage />} />
            <Route path="spaces/:spaceId/renovations/:renoId" element={<RenovationDetailPage />} />
            <Route path="materials" element={<MaterialsPage />} />
            <Route path="*" element={<Navigate to="/" replace />} />
          </Route>
        </Routes>
      </BrowserRouter>
      <ReactQueryDevtools initialIsOpen={false} />
    </QueryClientProvider>
  )
}
