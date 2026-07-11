import { useState } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { getMaterials, deleteMaterial } from '@/api/materials'
import MaterialForm from '@/components/materials/MaterialForm'
import Modal from '@/components/ui/Modal'
import Button from '@/components/ui/Button'
import Card from '@/components/ui/Card'
import LoadingSpinner from '@/components/ui/LoadingSpinner'
import EmptyState from '@/components/ui/EmptyState'
import type { MaterialCategory } from '@/types'

const CATEGORIES: (MaterialCategory | 'all')[] = [
  'all', 'flooring', 'paint', 'tile', 'cabinetry', 'countertop',
  'fixtures', 'lighting', 'plumbing', 'electrical', 'trim', 'other',
]

export default function MaterialsPage() {
  const queryClient = useQueryClient()
  const [showForm, setShowForm] = useState(false)
  const [activeCategory, setActiveCategory] = useState<MaterialCategory | 'all'>('all')

  const { data: materials = [], isLoading } = useQuery({
    queryKey: ['materials'],
    queryFn: () => getMaterials(),
  })

  const deleteMutation = useMutation({
    mutationFn: deleteMaterial,
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ['materials'] }),
  })

  const filtered = activeCategory === 'all'
    ? materials
    : materials.filter(m => m.category === activeCategory)

  return (
    <>
      <div className="flex items-center justify-between mb-6">
        <div>
          <h1 className="text-2xl font-bold text-gray-900">Materials</h1>
          <p className="text-sm text-gray-500 mt-0.5">Catalog of materials used across renovations</p>
        </div>
        <Button onClick={() => setShowForm(true)}>+ Add Material</Button>
      </div>

      <div className="flex gap-2 flex-wrap mb-6">
        {CATEGORIES.map(cat => (
          <button
            key={cat}
            onClick={() => setActiveCategory(cat)}
            className={`px-3 py-1 rounded-full text-xs font-medium transition-colors ${
              activeCategory === cat
                ? 'bg-green-600 text-white'
                : 'bg-gray-100 text-gray-600 hover:bg-gray-200'
            }`}
          >
            {cat.charAt(0).toUpperCase() + cat.slice(1)}
          </button>
        ))}
      </div>

      {isLoading ? (
        <LoadingSpinner />
      ) : filtered.length === 0 ? (
        <EmptyState
          title="No materials"
          description="Add materials to your catalog to track what's used in each renovation."
          action={<Button onClick={() => setShowForm(true)}>Add First Material</Button>}
        />
      ) : (
        <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-4">
          {filtered.map(m => (
            <Card key={m.id}>
              <div className="flex items-start justify-between">
                <div>
                  <p className="font-medium text-gray-900 text-sm">{m.name}</p>
                  <p className="text-xs text-gray-500 mt-0.5 capitalize">{m.category}{m.brand ? ` · ${m.brand}` : ''}</p>
                  {m.color && <p className="text-xs text-gray-400 mt-0.5">Color: {m.color}</p>}
                  {m.unit && <p className="text-xs text-gray-400">Unit: {m.unit}</p>}
                </div>
                <button
                  onClick={() => deleteMutation.mutate(m.id)}
                  className="text-gray-300 hover:text-red-500 transition-colors ml-2"
                  aria-label="Delete material"
                >
                  <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 7l-.867 12.142A2 2 0 0116.138 21H7.862a2 2 0 01-1.995-1.858L5 7m5 4v6m4-6v6m1-10V4a1 1 0 00-1-1h-4a1 1 0 00-1 1v3M4 7h16" />
                  </svg>
                </button>
              </div>
            </Card>
          ))}
        </div>
      )}

      <Modal open={showForm} onClose={() => setShowForm(false)} title="Add Material">
        <MaterialForm
          onSuccess={() => {
            queryClient.invalidateQueries({ queryKey: ['materials'] })
            setShowForm(false)
          }}
          onCancel={() => setShowForm(false)}
        />
      </Modal>
    </>
  )
}
