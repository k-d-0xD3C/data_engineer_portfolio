export type SpaceType = 'bathroom' | 'kitchen' | 'bedroom' | 'living' | 'exterior' | 'other'
export type RenovationStatus = 'planning' | 'in_progress' | 'complete'
export type PhotoPhase = 'before' | 'during' | 'after'
export type MaterialCategory =
  | 'flooring' | 'paint' | 'tile' | 'cabinetry' | 'countertop'
  | 'fixtures' | 'lighting' | 'plumbing' | 'electrical' | 'trim' | 'other'

export interface Space {
  id: string
  name: string
  space_type: SpaceType
  description: string | null
  address: string | null
  created_at: string
  updated_at: string
  renovation_count: number
}

export interface SpaceCreate {
  name: string
  space_type?: SpaceType
  description?: string | null
  address?: string | null
}

export interface SpaceUpdate {
  name?: string
  space_type?: SpaceType
  description?: string | null
  address?: string | null
}

export interface Material {
  id: string
  name: string
  category: MaterialCategory
  brand: string | null
  sku: string | null
  color: string | null
  unit: string | null
  notes: string | null
  created_at: string
}

export interface MaterialCreate {
  name: string
  category: MaterialCategory
  brand?: string | null
  sku?: string | null
  color?: string | null
  unit?: string | null
  notes?: string | null
}

export interface MaterialUsage {
  id: string
  material_id: string
  quantity: string | null
  unit_cost: string | null
  notes: string | null
  material: Material
}

export interface MaterialUsageCreate {
  material_id: string
  quantity?: string | null
  unit_cost?: string | null
  notes?: string | null
}

export interface Photo {
  id: string
  renovation_id: string
  phase: PhotoPhase
  url: string
  original_name: string | null
  content_type: string | null
  file_size_bytes: number | null
  caption: string | null
  sort_order: number
  uploaded_at: string
}

export interface Renovation {
  id: string
  space_id: string
  title: string
  description: string | null
  status: RenovationStatus
  start_date: string | null
  end_date: string | null
  estimated_cost: string | null
  actual_cost: string | null
  contractor: string | null
  notes: string | null
  created_at: string
  updated_at: string
}

export interface RenovationDetail extends Renovation {
  materials: MaterialUsage[]
  photos: Record<PhotoPhase, Photo[]>
}

export interface RenovationCreate {
  space_id: string
  title: string
  description?: string | null
  status?: RenovationStatus
  start_date?: string | null
  end_date?: string | null
  estimated_cost?: string | null
  actual_cost?: string | null
  contractor?: string | null
  notes?: string | null
}

export interface RenovationUpdate {
  title?: string
  description?: string | null
  status?: RenovationStatus
  start_date?: string | null
  end_date?: string | null
  estimated_cost?: string | null
  actual_cost?: string | null
  contractor?: string | null
  notes?: string | null
}
