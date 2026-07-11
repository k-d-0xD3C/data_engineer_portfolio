import { useState } from 'react'
import { useMutation, useQueryClient } from '@tanstack/react-query'
import { deletePhoto } from '@/api/photos'
import PhotoLightbox from './PhotoLightbox'
import type { Photo } from '@/types'

interface Props {
  photos: Photo[]
  renoId: string
}

export default function PhotoGrid({ photos, renoId }: Props) {
  const queryClient = useQueryClient()
  const [lightboxIndex, setLightboxIndex] = useState<number | null>(null)

  const deleteMutation = useMutation({
    mutationFn: (id: string) => deletePhoto(id),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['renovations', renoId] })
    },
  })

  if (photos.length === 0) return null

  const activPhoto = lightboxIndex !== null ? photos[lightboxIndex] ?? null : null

  return (
    <>
      <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-4 gap-3">
        {photos.map((photo, i) => (
          <div
            key={photo.id}
            className="relative group aspect-square rounded-lg overflow-hidden bg-gray-100 cursor-pointer"
            onClick={() => setLightboxIndex(i)}
          >
            <img
              src={photo.url}
              alt={photo.caption ?? photo.original_name ?? ''}
              className="w-full h-full object-cover transition-transform group-hover:scale-105"
            />
            <button
              onClick={e => {
                e.stopPropagation()
                deleteMutation.mutate(photo.id)
              }}
              className="absolute top-1.5 right-1.5 bg-black/50 text-white rounded-full w-6 h-6 flex items-center justify-center opacity-0 group-hover:opacity-100 transition-opacity hover:bg-red-600"
              aria-label="Delete photo"
            >
              <svg className="w-3.5 h-3.5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
              </svg>
            </button>
            {photo.caption && (
              <p className="absolute bottom-0 left-0 right-0 bg-black/50 text-white text-xs px-2 py-1 truncate opacity-0 group-hover:opacity-100 transition-opacity">
                {photo.caption}
              </p>
            )}
          </div>
        ))}
      </div>
      <PhotoLightbox
        photo={activPhoto}
        onClose={() => setLightboxIndex(null)}
        onPrev={lightboxIndex !== null && lightboxIndex > 0 ? () => setLightboxIndex(i => (i ?? 0) - 1) : undefined}
        onNext={lightboxIndex !== null && lightboxIndex < photos.length - 1 ? () => setLightboxIndex(i => (i ?? 0) + 1) : undefined}
      />
    </>
  )
}
