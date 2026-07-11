import { useState } from 'react'
import { clsx } from 'clsx'
import PhaseSection from './PhaseSection'
import type { PhotoPhase, RenovationDetail } from '@/types'

const PHASES: { key: PhotoPhase; label: string }[] = [
  { key: 'before', label: 'Before' },
  { key: 'during', label: 'During' },
  { key: 'after', label: 'After' },
]

interface Props {
  reno: RenovationDetail
}

export default function RenovationTimeline({ reno }: Props) {
  const [active, setActive] = useState<PhotoPhase>('before')
  const photos = reno.photos[active] ?? []

  return (
    <div>
      <div className="flex gap-1 mb-5 bg-gray-100 rounded-lg p-1 w-fit">
        {PHASES.map(({ key, label }) => {
          const count = (reno.photos[key] ?? []).length
          return (
            <button
              key={key}
              onClick={() => setActive(key)}
              className={clsx(
                'px-4 py-1.5 rounded-md text-sm font-medium transition-colors',
                active === key
                  ? 'bg-white text-gray-900 shadow-sm'
                  : 'text-gray-600 hover:text-gray-900',
              )}
            >
              {label}
              {count > 0 && (
                <span className="ml-1.5 text-xs text-gray-500">({count})</span>
              )}
            </button>
          )
        })}
      </div>
      <PhaseSection renoId={reno.id} phase={active} photos={photos} />
    </div>
  )
}
