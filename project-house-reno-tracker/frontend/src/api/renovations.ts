import { apiClient } from './client'
import type { Renovation, RenovationCreate, RenovationDetail, RenovationUpdate, MaterialUsage, MaterialUsageCreate } from '@/types'

export async function getRenovations(params?: { space_id?: string; status?: string }): Promise<Renovation[]> {
  const { data } = await apiClient.get<Renovation[]>('/renovations', { params })
  return data
}

export async function getRenovation(id: string): Promise<RenovationDetail> {
  const { data } = await apiClient.get<RenovationDetail>(`/renovations/${id}`)
  return data
}

export async function createRenovation(body: RenovationCreate): Promise<Renovation> {
  const { data } = await apiClient.post<Renovation>('/renovations', body)
  return data
}

export async function updateRenovation(id: string, body: RenovationUpdate): Promise<Renovation> {
  const { data } = await apiClient.put<Renovation>(`/renovations/${id}`, body)
  return data
}

export async function deleteRenovation(id: string): Promise<void> {
  await apiClient.delete(`/renovations/${id}`)
}

export async function attachMaterial(renoId: string, body: MaterialUsageCreate): Promise<MaterialUsage> {
  const { data } = await apiClient.post<MaterialUsage>(`/renovations/${renoId}/materials`, body)
  return data
}

export async function detachMaterial(renoId: string, materialId: string): Promise<void> {
  await apiClient.delete(`/renovations/${renoId}/materials/${materialId}`)
}
