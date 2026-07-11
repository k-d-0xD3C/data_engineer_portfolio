import { useState, useCallback } from 'react'
import { useDropzone } from 'react-dropzone'
import { useMutation, useQueryClient } from '@tanstack/react-query'
import { clsx } from 'clsx'
import { uploadPhoto } from '@/api/photos'
import type { PhotoPhase } from '@/types'

interface Props {
  renoId: string
  phase: PhotoPhase
}

export default function PhotoDropzone({ renoId, phase }: Props) {
  const queryClient = useQueryClient()
  const [progress, setProgress] = useState<number | null>(null)

  const mutation = useMutation({
    mutationFn: (file: File) => uploadPhoto(renoId, file, phase, undefined, setProgress),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['renovations', renoId] })
      queryClient.invalidateQueries({ queryKey: ['photos', renoId] })
      setProgress(null)
    },
    onError: () => setProgress(null),
  })

  const onDrop = useCallback((accepted: File[]) => {
    accepted.forEach(file => mutation.mutate(file))
  }, [mutation])

  const { getRootProps, getInputProps, isDragActive } = useDropzone({
    onDrop,
    accept: { 'image/*': ['.jpg', '.jpeg', '.png', '.webp'] },
    maxSize: 20_000_000,
    multiple: true,
  })

  return (
    <div>
      <div
        {...getRootProps()}
        className={clsx(
          'border-2 border-dashed rounded-lg p-6 text-center cursor-pointer transition-colors',
          isDragActive
            ? 'border-green-400 bg-green-50'
            : 'border-gray-300 hover:border-green-400 hover:bg-gray-50',
        )}
      >
        <input {...getInputProps()} />
        <svg className="mx-auto h-8 w-8 text-gray-400 mb-2" fill="none" stroke="currentColor" viewBox="0 0 24 24">
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M4 16l4.586-4.586a2 2 0 012.828 0L16 16m-2-2l1.586-1.586a2 2 0 012.828 0L20 14m-6-6h.01M6 20h12a2 2 0 002-2V6a2 2 0 00-2-2H6a2 2 0 00-2 2v12a2 2 0 002 2z" />
        </svg>
        <p className="text-sm text-gray-600">
          {isDragActive ? 'Drop photos here' : 'Drop photos or click to upload'}
        </p>
        <p className="text-xs text-gray-400 mt-1">JPG, PNG, WEBP up to 20 MB</p>
      </div>
      {mutation.isPending && progress !== null && (
        <div className="mt-2">
          <div className="flex justify-between text-xs text-gray-500 mb-1">
            <span>Uploading...</span>
            <span>{progress}%</span>
          </div>
          <div className="h-1.5 bg-gray-200 rounded-full overflow-hidden">
            <div className="h-full bg-green-500 rounded-full transition-all" style={{ width: `${progress}%` }} />
          </div>
        </div>
      )}
      {mutation.isError && (
        <p className="mt-2 text-xs text-red-600">Upload failed. Please try again.</p>
      )}
    </div>
  )
}
