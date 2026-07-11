import { useState } from 'react'
import { useMutation } from '@tanstack/react-query'
import { createMaterial } from '@/api/materials'
import Button from '@/components/ui/Button'
import type { MaterialCategory, MaterialCreate } from '@/types'

const CATEGORIES: MaterialCategory[] = [
  'flooring', 'paint', 'tile', 'cabinetry', 'countertop',
  'fixtures', 'lighting', 'plumbing', 'electrical', 'trim', 'other',
]

interface Props {
  onSuccess: () => void
  onCancel: () => void
}

export default function MaterialForm({ onSuccess, onCancel }: Props) {
  const [name, setName] = useState('')
  const [category, setCategory] = useState<MaterialCategory>('other')
  const [brand, setBrand] = useState('')
  const [color, setColor] = useState('')
  const [unit, setUnit] = useState('')
  const [error, setError] = useState<string | null>(null)

  const mutation = useMutation({
    mutationFn: (body: MaterialCreate) => createMaterial(body),
    onSuccess,
    onError: () => setError('Failed to create material. Please try again.'),
  })

  function handleSubmit(e: React.FormEvent) {
    e.preventDefault()
    if (!name.trim()) { setError('Name is required'); return }
    setError(null)
    mutation.mutate({ name: name.trim(), category, brand: brand || null, color: color || null, unit: unit || null })
  }

  return (
    <form onSubmit={handleSubmit} className="space-y-4">
      <div>
        <label className="block text-sm font-medium text-gray-700 mb-1">Name *</label>
        <input
          value={name}
          onChange={e => setName(e.target.value)}
          placeholder="e.g. Shaw Oak Hardwood"
          className="w-full border border-gray-300 rounded-lg px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-green-500"
        />
      </div>
      <div>
        <label className="block text-sm font-medium text-gray-700 mb-1">Category</label>
        <select
          value={category}
          onChange={e => setCategory(e.target.value as MaterialCategory)}
          className="w-full border border-gray-300 rounded-lg px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-green-500"
        >
          {CATEGORIES.map(c => (
            <option key={c} value={c}>{c.charAt(0).toUpperCase() + c.slice(1)}</option>
          ))}
        </select>
      </div>
      <div className="grid grid-cols-2 gap-3">
        <div>
          <label className="block text-sm font-medium text-gray-700 mb-1">Brand</label>
          <input value={brand} onChange={e => setBrand(e.target.value)} placeholder="Optional" className="w-full border border-gray-300 rounded-lg px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-green-500" />
        </div>
        <div>
          <label className="block text-sm font-medium text-gray-700 mb-1">Color</label>
          <input value={color} onChange={e => setColor(e.target.value)} placeholder="Optional" className="w-full border border-gray-300 rounded-lg px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-green-500" />
        </div>
      </div>
      <div>
        <label className="block text-sm font-medium text-gray-700 mb-1">Unit</label>
        <input value={unit} onChange={e => setUnit(e.target.value)} placeholder="sq_ft, gallon, each..." className="w-full border border-gray-300 rounded-lg px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-green-500" />
      </div>
      {error && <p className="text-sm text-red-600">{error}</p>}
      <div className="flex justify-end gap-3 pt-2">
        <Button type="button" variant="secondary" onClick={onCancel}>Cancel</Button>
        <Button type="submit" loading={mutation.isPending}>Add Material</Button>
      </div>
    </form>
  )
}
