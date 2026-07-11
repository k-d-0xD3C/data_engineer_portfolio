import { useState } from 'react'
import { useParams, Link } from 'react-router-dom'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { getSpace } from '@/api/spaces'
import { getRenovations, createRenovation } from '@/api/renovations'
import { StatusBadge } from '@/components/ui/Badge'
import Card from '@/components/ui/Card'
import Button from '@/components/ui/Button'
import Modal from '@/components/ui/Modal'
import LoadingSpinner from '@/components/ui/LoadingSpinner'
import EmptyState from '@/components/ui/EmptyState'
import RenovationForm from '@/components/renovations/RenovationForm'

export default function SpaceDetailPage() {
  const { spaceId } = useParams<{ spaceId: string }>()
  const queryClient = useQueryClient()
  const [showForm, setShowForm] = useState(false)

  const { data: space, isLoading: spaceLoading } = useQuery({
    queryKey: ['spaces', spaceId],
    queryFn: () => getSpace(spaceId!),
    enabled: !!spaceId,
  })

  const { data: renovations = [], isLoading: renosLoading } = useQuery({
    queryKey: ['spaces', spaceId, 'renovations'],
    queryFn: () => getRenovations({ space_id: spaceId }),
    enabled: !!spaceId,
  })

  const createMutation = useMutation({
    mutationFn: createRenovation,
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['spaces', spaceId, 'renovations'] })
      queryClient.invalidateQueries({ queryKey: ['spaces'] })
      setShowForm(false)
    },
  })

  if (spaceLoading) return <LoadingSpinner />
  if (!space) return <p className="text-gray-500">Space not found.</p>

  return (
    <>
      <div className="mb-2">
        <Link to="/spaces" className="text-sm text-green-600 hover:text-green-700">← Spaces</Link>
      </div>
      <div className="flex items-start justify-between mb-6">
        <div>
          <h1 className="text-2xl font-bold text-gray-900">{space.name}</h1>
          <p className="text-sm text-gray-500 capitalize mt-0.5">{space.space_type}{space.address ? ` · ${space.address}` : ''}</p>
          {space.description && <p className="text-sm text-gray-600 mt-2 max-w-prose">{space.description}</p>}
        </div>
        <Button onClick={() => setShowForm(true)}>+ Add Renovation</Button>
      </div>

      {renosLoading ? (
        <LoadingSpinner />
      ) : renovations.length === 0 ? (
        <EmptyState
          title="No renovations yet"
          description="Track a project for this space — add before/during/after photos and materials."
          action={<Button onClick={() => setShowForm(true)}>Add First Renovation</Button>}
        />
      ) : (
        <div className="space-y-3">
          {renovations.map(reno => (
            <Link key={reno.id} to={`/spaces/${spaceId}/renovations/${reno.id}`}>
              <Card className="hover:shadow-md transition-shadow cursor-pointer">
                <div className="flex items-center justify-between">
                  <div>
                    <p className="font-medium text-gray-900">{reno.title}</p>
                    <p className="text-xs text-gray-500 mt-0.5">
                      {reno.contractor && `${reno.contractor} · `}
                      {reno.start_date ?? 'No start date'}
                    </p>
                  </div>
                  <div className="flex items-center gap-4">
                    {reno.estimated_cost && (
                      <span className="text-sm text-gray-600">${parseFloat(reno.estimated_cost).toLocaleString()}</span>
                    )}
                    <StatusBadge status={reno.status} />
                  </div>
                </div>
              </Card>
            </Link>
          ))}
        </div>
      )}

      <Modal open={showForm} onClose={() => setShowForm(false)} title="Add Renovation">
        <RenovationForm
          spaceId={spaceId!}
          onSubmit={async (data) => { await createMutation.mutateAsync(data) }}
          onCancel={() => setShowForm(false)}
        />
      </Modal>
    </>
  )
}
