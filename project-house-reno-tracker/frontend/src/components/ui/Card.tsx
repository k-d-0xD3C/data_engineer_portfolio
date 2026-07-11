import { type HTMLAttributes } from 'react'
import { clsx } from 'clsx'

interface Props extends HTMLAttributes<HTMLDivElement> {
  padding?: boolean
}

export default function Card({ padding = true, className, children, ...props }: Props) {
  return (
    <div
      className={clsx('bg-white rounded-xl shadow-sm border border-gray-200', padding && 'p-5', className)}
      {...props}
    >
      {children}
    </div>
  )
}
