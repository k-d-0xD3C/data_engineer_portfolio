import type { MaterialUsage } from '@/types'

interface Props {
  usage: MaterialUsage
  onRemove?: () => void
}

const categoryColors: Record<string, string> = {
  flooring: 'bg-amber-100 text-amber-700',
  paint: 'bg-blue-100 text-blue-700',
  tile: 'bg-orange-100 text-orange-700',
  cabinetry: 'bg-purple-100 text-purple-700',
  countertop: 'bg-pink-100 text-pink-700',
  fixtures: 'bg-cyan-100 text-cyan-700',
  lighting: 'bg-yellow-100 text-yellow-700',
  plumbing: 'bg-sky-100 text-sky-700',
  electrical: 'bg-red-100 text-red-700',
  trim: 'bg-green-100 text-green-700',
  other: 'bg-gray-100 text-gray-700',
}

export default function MaterialBadge({ usage, onRemove }: Props) {
  const colorClass = categoryColors[usage.material.category] ?? categoryColors.other
  return (
    <span className={`inline-flex items-center gap-1.5 px-2.5 py-1 rounded-full text-xs font-medium ${colorClass}`}>
      {usage.material.name}
      {usage.material.color && <span className="opacity-60">· {usage.material.color}</span>}
      {onRemove && (
        <button
          onClick={onRemove}
          className="ml-0.5 opacity-60 hover:opacity-100 transition-opacity"
          aria-label="Remove material"
        >
          <svg className="w-3 h-3" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
          </svg>
        </button>
      )}
    </span>
  )
}
