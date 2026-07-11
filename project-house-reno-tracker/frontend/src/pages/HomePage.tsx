import { Link } from 'react-router-dom'
import { useQuery } from '@tanstack/react-query'
import { getSpaces } from '@/api/spaces'
import { getRenovations } from '@/api/renovations'
import { getMaterials } from '@/api/materials'
import Card from '@/components/ui/Card'
import { StatusBadge } from '@/components/ui/Badge'
import LoadingSpinner from '@/components/ui/LoadingSpinner'
import Button from '@/components/ui/Button'

export default function HomePage() {
  const { data: spaces = [], isLoading: spacesLoading } = useQuery({ queryKey: ['spaces'], queryFn: getSpaces })
  const { data: renovations = [], isLoading: renosLoading } = useQuery({ queryKey: ['renovations'], queryFn: () => getRenovations() })
  const { data: materials = [] } = useQuery({ queryKey: ['materials'], queryFn: getMaterials })

  const inProgress = renovations.filter(r => r.status === 'in_progress')
  const complete = renovations.filter(r => r.status === 'complete')

  return (
    <>
      <div className="mb-6">
        <h1 className="text-2xl font-bold text-gray-900">Dashboard</h1>
        <p className="text-sm text-gray-500 mt-0.5">Overview of your renovation projects</p>
      </div>

      <div className="grid grid-cols-2 lg:grid-cols-4 gap-4 mb-8">
        <StatCard label="Spaces" value={spaces.length} color="blue" />
        <StatCard label="Renovations" value={renovations.length} color="gray" />
        <StatCard label="In Progress" value={inProgress.length} color="yellow" />
        <StatCard label="Complete" value={complete.length} color="green" />
      </div>

      {(spacesLoading || renosLoading) ? (
        <LoadingSpinner />
      ) : (
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
          <div>
            <div className="flex items-center justify-between mb-3">
              <h2 className="text-base font-semibold text-gray-900">Recent Spaces</h2>
              <Link to="/spaces" className="text-sm text-green-600 hover:text-green-700">View all</Link>
            </div>
            {spaces.length === 0 ? (
              <Card>
                <p className="text-sm text-gray-500 text-center py-4">No spaces yet.</p>
                <div className="flex justify-center mt-2">
                  <Link to="/spaces"><Button size="sm">Add Space</Button></Link>
                </div>
              </Card>
            ) : (
              <div className="space-y-2">
                {spaces.slice(0, 5).map(space => (
                  <Link key={space.id} to={`/spaces/${space.id}`}>
                    <Card className="hover:shadow-md transition-shadow cursor-pointer">
                      <div className="flex items-center justify-between">
                        <div>
                          <p className="text-sm font-medium text-gray-900">{space.name}</p>
                          <p className="text-xs text-gray-500 capitalize">{space.space_type} · {space.renovation_count} renovations</p>
                        </div>
                      </div>
                    </Card>
                  </Link>
                ))}
              </div>
            )}
          </div>

          <div>
            <div className="flex items-center justify-between mb-3">
              <h2 className="text-base font-semibold text-gray-900">Active Renovations</h2>
            </div>
            {inProgress.length === 0 ? (
              <Card>
                <p className="text-sm text-gray-500 text-center py-4">No active renovations.</p>
              </Card>
            ) : (
              <div className="space-y-2">
                {inProgress.slice(0, 5).map(reno => (
                  <Card key={reno.id} className="hover:shadow-md transition-shadow cursor-pointer">
                    <div className="flex items-center justify-between">
                      <div>
                        <p className="text-sm font-medium text-gray-900">{reno.title}</p>
                        {reno.contractor && <p className="text-xs text-gray-500">{reno.contractor}</p>}
                      </div>
                      <StatusBadge status={reno.status} />
                    </div>
                  </Card>
                ))}
              </div>
            )}
          </div>
        </div>
      )}
    </>
  )
}

function StatCard({ label, value, color }: { label: string; value: number; color: string }) {
  const colorMap: Record<string, string> = {
    blue: 'text-blue-600 bg-blue-50',
    gray: 'text-gray-600 bg-gray-100',
    yellow: 'text-yellow-600 bg-yellow-50',
    green: 'text-green-600 bg-green-50',
  }
  return (
    <Card>
      <p className="text-xs text-gray-500 font-medium uppercase tracking-wide">{label}</p>
      <p className={`text-3xl font-bold mt-1 ${colorMap[color]?.split(' ')[0] ?? 'text-gray-900'}`}>{value}</p>
    </Card>
  )
}
