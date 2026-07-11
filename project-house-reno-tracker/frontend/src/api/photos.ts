import { apiClient } from './client'
import type { Photo } from '@/types'

export async function getPhotos(renoId: string): Promise<Photo[]> {
  const { data } = await apiClient.get<Photo[]>(`/renovations/${renoId}/photos`)
  return data
}

export async function uploadPhoto(
  renoId: string,
  file: File,
  phase: string,
  caption?: string,
  onProgress?: (pct: number) => void,
): Promise<Photo> {
  const form = new FormData()
  form.append('file', file)
  form.append('phase', phase)
  if (caption) form.append('caption', caption)

  const { data } = await apiClient.post<Photo>(`/renovations/${renoId}/photos`, form, {
    headers: { 'Content-Type': 'multipart/form-data' },
    onUploadProgress: (e) => {
      if (onProgress && e.total) onProgress(Math.round((e.loaded / e.total) * 100))
    },
  })
  return data
}

export async function deletePhoto(photoId: string): Promise<void> {
  await apiClient.delete(`/photos/${photoId}`)
}

export async function updatePhoto(photoId: string, body: { caption?: string; sort_order?: number }): Promise<Photo> {
  const { data } = await apiClient.patch<Photo>(`/photos/${photoId}`, body)
  return data
}
