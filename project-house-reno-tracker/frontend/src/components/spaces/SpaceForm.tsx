import { useState } from 'react'
import Button from '@/components/ui/Button'
import type { SpaceCreate, SpaceType } from '@/types'

const SPACE_TYPES: SpaceType[] = ['bathroom', 'kitchen', 'bedroom', 'living', 'exterior', 'other']

interface Props {
  onSubmit: (data: SpaceCreate) => Promise<void>
  onCancel: () => void
  initial?: Partial<SpaceCreate>
  submitLabel?: string
}

export default function SpaceForm({ onSubmit, onCancel, initial = {}, submitLabel = 'Create Space' }: Props) {
  const [name, setName] = useState(initial.name ?? '')
  const [spaceType, setSpaceType] = useState<SpaceType>(initial.space_type ?? 'other')
  const [description, setDescription] = useState(initial.description ?? '')
  const [address, setAddress] = useState(initial.address ?? '')
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault()
    if (!name.trim()) { setError('Name is required'); return }
    setLoading(true)
    setError(null)
    try {
      await onSubmit({ name: name.trim(), space_type: spaceType, description: description || null, address: address || null })
    } catch {
      setError('Failed to save. Please try again.')
    } finally {
      setLoading(false)
    }
  }

  return (
    <form onSubmit={handleSubmit} className="space-y-4">
      <div>
        <label className="block text-sm font-medium text-gray-700 mb-1">Name *</label>
        <input
          value={name}
          onChange={e => setName(e.target.value)}
          placeholder="e.g. Master Bathroom"
          className="w-full border border-gray-300 rounded-lg px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-green-500"
        />
      </div>
      <div>
        <label className="block text-sm font-medium text-gray-700 mb-1">Type</label>
        <select
          value={spaceType}
          onChange={e => setSpaceType(e.target.value as SpaceType)}
          className="w-full border border-gray-300 rounded-lg px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-green-500"
        >
          {SPACE_TYPES.map(t => (
            <option key={t} value={t}>{t.charAt(0).toUpperCase() + t.slice(1)}</option>
          ))}
        </select>
      </div>
      <div>
        <label className="block text-sm font-medium text-gray-700 mb-1">Address</label>
        <input
          value={address}
          onChange={e => setAddress(e.target.value)}
          placeholder="123 Main St (optional)"
          className="w-full border border-gray-300 rounded-lg px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-green-500"
        />
      </div>
      <div>
        <label className="block text-sm font-medium text-gray-700 mb-1">Description</label>
        <textarea
          value={description}
          onChange={e => setDescription(e.target.value)}
          rows={3}
          placeholder="Optional notes about this space"
          className="w-full border border-gray-300 rounded-lg px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-green-500 resize-none"
        />
      </div>
      {error && <p className="text-sm text-red-600">{error}</p>}
      <div className="flex justify-end gap-3 pt-2">
        <Button type="button" variant="secondary" onClick={onCancel}>Cancel</Button>
        <Button type="submit" loading={loading}>{submitLabel}</Button>
      </div>
    </form>
  )
}
