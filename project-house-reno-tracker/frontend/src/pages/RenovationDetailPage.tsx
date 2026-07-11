import { useParams, Link } from 'react-router-dom'
import { useQuery } from '@tanstack/react-query'
import { getRenovation } from '@/api/renovations'
import { StatusBadge } from '@/components/ui/Badge'
import Card from '@/components/ui/Card'
import LoadingSpinner from '@/components/ui/LoadingSpinner'
import RenovationTimeline from '@/components/renovations/RenovationTimeline'
import MaterialSelector from '@/components/materials/MaterialSelector'

export default function RenovationDetailPage() {
  const { spaceId, renoId } = useParams<{ spaceId: string; renoId: string }>()

  const { data: reno, isLoading } = useQuery({
    queryKey: ['renovations', renoId],
    queryFn: () => getRenovation(renoId!),
    enabled: !!renoId,
  })

  if (isLoading) return <LoadingSpinner />
  if (!reno) return <p className="text-gray-500">Renovation not found.</p>

  const totalPhotos = Object.values(reno.photos).reduce((sum, arr) => sum + arr.length, 0)

  return (
    <>
      <div className="mb-2">
        <Link to={`/spaces/${spaceId}`} className="text-sm text-green-600 hover:text-green-700">← Back to Space</Link>
      </div>

      <div className="flex items-start justify-between mb-6">
        <div>
          <h1 className="text-2xl font-bold text-gray-900">{reno.title}</h1>
          <div className="flex items-center gap-3 mt-1">
            <StatusBadge status={reno.status} />
            {reno.contractor && <span className="text-sm text-gray-500">{reno.contractor}</span>}
            {totalPhotos > 0 && <span className="text-xs text-gray-400">{totalPhotos} photo{totalPhotos !== 1 ? 's' : ''}</span>}
          </div>
        </div>
        <div className="text-right">
          {reno.estimated_cost && (
            <p className="text-sm text-gray-500">
              Est. <span className="font-medium text-gray-900">${parseFloat(reno.estimated_cost).toLocaleString()}</span>
            </p>
          )}
          {reno.actual_cost && (
            <p className="text-sm text-gray-500">
              Actual <span className="font-medium text-gray-900">${parseFloat(reno.actual_cost).toLocaleString()}</span>
            </p>
          )}
        </div>
      </div>

      {reno.notes && (
        <Card className="mb-6">
          <p className="text-sm text-gray-700">{reno.notes}</p>
        </Card>
      )}

      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
        <div className="lg:col-span-2">
          <h2 className="text-base font-semibold text-gray-900 mb-4">Photos</h2>
          <RenovationTimeline reno={reno} />
        </div>
        <div>
          <h2 className="text-base font-semibold text-gray-900 mb-4">Materials</h2>
          <Card>
            <MaterialSelector renoId={reno.id} usages={reno.materials} />
          </Card>
        </div>
      </div>
    </>
  )
}
