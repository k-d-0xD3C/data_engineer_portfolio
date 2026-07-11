import { useState } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { getSpaces, createSpace } from '@/api/spaces'
import SpaceCard from '@/components/spaces/SpaceCard'
import SpaceForm from '@/components/spaces/SpaceForm'
import Modal from '@/components/ui/Modal'
import Button from '@/components/ui/Button'
import LoadingSpinner from '@/components/ui/LoadingSpinner'
import EmptyState from '@/components/ui/EmptyState'

export default function SpacesPage() {
  const queryClient = useQueryClient()
  const [showForm, setShowForm] = useState(false)

  const { data: spaces = [], isLoading } = useQuery({
    queryKey: ['spaces'],
    queryFn: getSpaces,
  })

  const createMutation = useMutation({
    mutationFn: createSpace,
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['spaces'] })
      setShowForm(false)
    },
  })

  return (
    <>
      <div className="flex items-center justify-between mb-6">
        <div>
          <h1 className="text-2xl font-bold text-gray-900">Spaces</h1>
          <p className="text-sm text-gray-500 mt-0.5">Track renovations room by room</p>
        </div>
        <Button onClick={() => setShowForm(true)}>+ Add Space</Button>
      </div>

      {isLoading ? (
        <LoadingSpinner />
      ) : spaces.length === 0 ? (
        <EmptyState
          title="No spaces yet"
          description="Add your first room or area to start tracking renovations."
          action={<Button onClick={() => setShowForm(true)}>Add Your First Space</Button>}
        />
      ) : (
        <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4 gap-4">
          {spaces.map(space => (
            <SpaceCard key={space.id} space={space} />
          ))}
        </div>
      )}

      <Modal open={showForm} onClose={() => setShowForm(false)} title="Add Space">
        <SpaceForm
          onSubmit={async (data) => { await createMutation.mutateAsync(data) }}
          onCancel={() => setShowForm(false)}
        />
      </Modal>
    </>
  )
}
