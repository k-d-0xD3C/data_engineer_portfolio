import { useState } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { getMaterials } from '@/api/materials'
import { attachMaterial, detachMaterial } from '@/api/renovations'
import MaterialBadge from './MaterialBadge'
import MaterialForm from './MaterialForm'
import Modal from '@/components/ui/Modal'
import Button from '@/components/ui/Button'
import type { MaterialUsage, Material } from '@/types'

interface Props {
  renoId: string
  usages: MaterialUsage[]
}

export default function MaterialSelector({ renoId, usages }: Props) {
  const queryClient = useQueryClient()
  const [showForm, setShowForm] = useState(false)
  const [search, setSearch] = useState('')

  const { data: catalog = [] } = useQuery({
    queryKey: ['materials'],
    queryFn: getMaterials,
  })

  const attachMutation = useMutation({
    mutationFn: (material: Material) => attachMaterial(renoId, { material_id: material.id }),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ['renovations', renoId] }),
  })

  const detachMutation = useMutation({
    mutationFn: (materialId: string) => detachMaterial(renoId, materialId),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ['renovations', renoId] }),
  })

  const usedIds = new Set(usages.map(u => u.material_id))
  const filtered = catalog.filter(m =>
    !usedIds.has(m.id) &&
    (m.name.toLowerCase().includes(search.toLowerCase()) || m.category.toLowerCase().includes(search.toLowerCase()))
  )

  return (
    <div className="space-y-3">
      {usages.length > 0 && (
        <div className="flex flex-wrap gap-2">
          {usages.map(u => (
            <MaterialBadge
              key={u.id}
              usage={u}
              onRemove={() => detachMutation.mutate(u.material_id)}
            />
          ))}
        </div>
      )}
      <div className="flex gap-2">
        <input
          value={search}
          onChange={e => setSearch(e.target.value)}
          placeholder="Search materials to add..."
          className="flex-1 border border-gray-300 rounded-lg px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-green-500"
        />
        <Button variant="secondary" size="sm" onClick={() => setShowForm(true)}>+ New</Button>
      </div>
      {search && filtered.length > 0 && (
        <div className="border border-gray-200 rounded-lg divide-y max-h-48 overflow-y-auto">
          {filtered.map(m => (
            <button
              key={m.id}
              onClick={() => { attachMutation.mutate(m); setSearch('') }}
              className="w-full flex items-center justify-between px-3 py-2 text-sm hover:bg-gray-50 text-left"
            >
              <span className="font-medium text-gray-900">{m.name}</span>
              <span className="text-gray-400 text-xs">{m.category}{m.color ? ` · ${m.color}` : ''}</span>
            </button>
          ))}
        </div>
      )}
      {search && filtered.length === 0 && (
        <p className="text-sm text-gray-500">No materials found. Add a new one above.</p>
      )}
      <Modal open={showForm} onClose={() => setShowForm(false)} title="Add Material to Catalog">
        <MaterialForm
          onSuccess={() => {
            queryClient.invalidateQueries({ queryKey: ['materials'] })
            setShowForm(false)
          }}
          onCancel={() => setShowForm(false)}
        />
      </Modal>
    </div>
  )
}
