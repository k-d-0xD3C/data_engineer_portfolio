import { clsx } from 'clsx'
import type { RenovationStatus, PhotoPhase, MaterialCategory } from '@/types'

type Color = 'gray' | 'green' | 'yellow' | 'blue' | 'red' | 'purple'

interface Props {
  label: string
  color?: Color
  className?: string
}

const colors: Record<Color, string> = {
  gray: 'bg-gray-100 text-gray-700',
  green: 'bg-green-100 text-green-700',
  yellow: 'bg-yellow-100 text-yellow-700',
  blue: 'bg-blue-100 text-blue-700',
  red: 'bg-red-100 text-red-700',
  purple: 'bg-purple-100 text-purple-700',
}

export default function Badge({ label, color = 'gray', className }: Props) {
  return (
    <span className={clsx('inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium', colors[color], className)}>
      {label}
    </span>
  )
}

export function StatusBadge({ status }: { status: RenovationStatus }) {
  const map: Record<RenovationStatus, { label: string; color: Color }> = {
    planning: { label: 'Planning', color: 'gray' },
    in_progress: { label: 'In Progress', color: 'yellow' },
    complete: { label: 'Complete', color: 'green' },
  }
  const { label, color } = map[status]
  return <Badge label={label} color={color} />
}

export function PhaseBadge({ phase }: { phase: PhotoPhase }) {
  const map: Record<PhotoPhase, { label: string; color: Color }> = {
    before: { label: 'Before', color: 'blue' },
    during: { label: 'During', color: 'yellow' },
    after: { label: 'After', color: 'green' },
  }
  const { label, color } = map[phase]
  return <Badge label={label} color={color} />
}
