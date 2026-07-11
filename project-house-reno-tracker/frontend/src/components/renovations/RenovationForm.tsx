import { useState } from 'react'
import Button from '@/components/ui/Button'
import type { RenovationCreate, RenovationStatus } from '@/types'

const STATUSES: RenovationStatus[] = ['planning', 'in_progress', 'complete']

interface Props {
  spaceId: string
  onSubmit: (data: RenovationCreate) => Promise<void>
  onCancel: () => void
  initial?: Partial<RenovationCreate>
  submitLabel?: string
}

export default function RenovationForm({ spaceId, onSubmit, onCancel, initial = {}, submitLabel = 'Create Renovation' }: Props) {
  const [title, setTitle] = useState(initial.title ?? '')
  const [status, setStatus] = useState<RenovationStatus>(initial.status ?? 'planning')
  const [contractor, setContractor] = useState(initial.contractor ?? '')
  const [estimatedCost, setEstimatedCost] = useState(initial.estimated_cost ?? '')
  const [notes, setNotes] = useState(initial.notes ?? '')
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault()
    if (!title.trim()) { setError('Title is required'); return }
    setLoading(true)
    setError(null)
    try {
      await onSubmit({
        space_id: spaceId,
        title: title.trim(),
        status,
        contractor: contractor || null,
        estimated_cost: estimatedCost || null,
        notes: notes || null,
      })
    } catch {
      setError('Failed to save. Please try again.')
    } finally {
      setLoading(false)
    }
  }

  return (
    <form onSubmit={handleSubmit} className="space-y-4">
      <div>
        <label className="block text-sm font-medium text-gray-700 mb-1">Title *</label>
        <input
          value={title}
          onChange={e => setTitle(e.target.value)}
          placeholder="e.g. Hardwood Floor Installation"
          className="w-full border border-gray-300 rounded-lg px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-green-500"
        />
      </div>
      <div>
        <label className="block text-sm font-medium text-gray-700 mb-1">Status</label>
        <select
          value={status}
          onChange={e => setStatus(e.target.value as RenovationStatus)}
          className="w-full border border-gray-300 rounded-lg px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-green-500"
        >
          {STATUSES.map(s => (
            <option key={s} value={s}>{s.replace('_', ' ').replace(/\b\w/g, c => c.toUpperCase())}</option>
          ))}
        </select>
      </div>
      <div>
        <label className="block text-sm font-medium text-gray-700 mb-1">Contractor</label>
        <input
          value={contractor}
          onChange={e => setContractor(e.target.value)}
          placeholder="Contractor name (optional)"
          className="w-full border border-gray-300 rounded-lg px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-green-500"
        />
      </div>
      <div>
        <label className="block text-sm font-medium text-gray-700 mb-1">Estimated Cost ($)</label>
        <input
          type="number"
          min="0"
          step="0.01"
          value={estimatedCost}
          onChange={e => setEstimatedCost(e.target.value)}
          placeholder="0.00"
          className="w-full border border-gray-300 rounded-lg px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-green-500"
        />
      </div>
      <div>
        <label className="block text-sm font-medium text-gray-700 mb-1">Notes</label>
        <textarea
          value={notes}
          onChange={e => setNotes(e.target.value)}
          rows={3}
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
