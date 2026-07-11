import { Link } from 'react-router-dom'
import Card from '@/components/ui/Card'
import { StatusBadge } from '@/components/ui/Badge'
import type { Space, RenovationStatus } from '@/types'

interface Props {
  space: Space
  latestStatus?: RenovationStatus
  thumbnailUrl?: string
}

const spaceTypeEmoji: Record<string, string> = {
  bathroom: '🚿',
  kitchen: '🍳',
  bedroom: '🛏️',
  living: '🛋️',
  exterior: '🌿',
  other: '🏠',
}

export default function SpaceCard({ space, latestStatus, thumbnailUrl }: Props) {
  return (
    <Link to={`/spaces/${space.id}`}>
      <Card padding={false} className="overflow-hidden hover:shadow-md transition-shadow cursor-pointer">
        <div className="h-40 bg-gray-100 relative overflow-hidden">
          {thumbnailUrl ? (
            <img src={thumbnailUrl} alt={space.name} className="w-full h-full object-cover" />
          ) : (
            <div className="w-full h-full flex items-center justify-center text-4xl">
              {spaceTypeEmoji[space.space_type] ?? '🏠'}
            </div>
          )}
        </div>
        <div className="p-4">
          <div className="flex items-start justify-between gap-2">
            <h3 className="font-medium text-gray-900 text-sm leading-tight">{space.name}</h3>
            {latestStatus && <StatusBadge status={latestStatus} />}
          </div>
          <p className="text-xs text-gray-500 mt-1 capitalize">{space.space_type}</p>
          <p className="text-xs text-gray-400 mt-2">
            {space.renovation_count} renovation{space.renovation_count !== 1 ? 's' : ''}
          </p>
        </div>
      </Card>
    </Link>
  )
}
