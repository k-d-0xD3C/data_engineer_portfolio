import { apiClient } from './client'
import type { Space, SpaceCreate, SpaceUpdate, Renovation } from '@/types'

export async function getSpaces(): Promise<Space[]> {
  const { data } = await apiClient.get<Space[]>('/spaces')
  return data
}

export async function getSpace(id: string): Promise<Space> {
  const { data } = await apiClient.get<Space>(`/spaces/${id}`)
  return data
}

export async function createSpace(body: SpaceCreate): Promise<Space> {
  const { data } = await apiClient.post<Space>('/spaces', body)
  return data
}

export async function updateSpace(id: string, body: SpaceUpdate): Promise<Space> {
  const { data } = await apiClient.put<Space>(`/spaces/${id}`, body)
  return data
}

export async function deleteSpace(id: string): Promise<void> {
  await apiClient.delete(`/spaces/${id}`)
}

export async function getSpaceRenovations(spaceId: string): Promise<Renovation[]> {
  const { data } = await apiClient.get<Renovation[]>(`/spaces/${spaceId}/renovations`)
  return data
}
