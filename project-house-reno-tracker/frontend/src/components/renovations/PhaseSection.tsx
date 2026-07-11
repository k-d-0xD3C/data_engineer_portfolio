import PhotoGrid from '@/components/photos/PhotoGrid'
import PhotoDropzone from '@/components/photos/PhotoDropzone'
import type { Photo, PhotoPhase } from '@/types'

interface Props {
  renoId: string
  phase: PhotoPhase
  photos: Photo[]
}

export default function PhaseSection({ renoId, phase, photos }: Props) {
  return (
    <div className="space-y-4">
      <PhotoDropzone renoId={renoId} phase={phase} />
      {photos.length > 0 && <PhotoGrid photos={photos} renoId={renoId} />}
    </div>
  )
}
