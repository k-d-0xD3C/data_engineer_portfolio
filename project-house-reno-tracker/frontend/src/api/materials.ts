import { apiClient } from './client'
import type { Material, MaterialCreate } from '@/types'

export async function getMaterials(category?: string): Promise<Material[]> {
  const { data } = await apiClient.get<Material[]>('/materials', { params: category ? { category } : undefined })
  return data
}

export async function createMaterial(body: MaterialCreate): Promise<Material> {
  const { data } = await apiClient.post<Material>('/materials', body)
  return data
}

export async function updateMaterial(id: string, body: Partial<MaterialCreate>): Promise<Material> {
  const { data } = await apiClient.put<Material>(`/materials/${id}`, body)
  return data
}

export async function deleteMaterial(id: string): Promise<void> {
  await apiClient.delete(`/materials/${id}`)
}
