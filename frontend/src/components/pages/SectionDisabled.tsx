import { Link } from 'react-router-dom';
import { Lock, ArrowLeft } from 'lucide-react';
import type { Section } from '../../lib/sections';

export function SectionDisabled({ section }: { section: Section }) {
  return (
    <div className="max-w-2xl mx-auto p-8">
      <Link
        to="/"
        className="inline-flex items-center gap-1 text-sm text-gray-500 hover:text-gray-700 mb-6"
      >
        <ArrowLeft className="w-4 h-4" /> Back to home
      </Link>
      <div className="bg-white border border-gray-200 rounded-lg p-8 text-center">
        <div className="inline-flex items-center justify-center w-12 h-12 rounded-full bg-gray-100 text-gray-500 mb-4">
          <Lock className="w-5 h-5" />
        </div>
        <h1 className="text-xl font-semibold text-gray-900">{section.title} is paused</h1>
        <p className="mt-2 text-sm text-gray-600">
          This area is disabled while we focus on the Site Config models. Set{' '}
          <code className="px-1 py-0.5 bg-gray-100 rounded text-xs">
            VITE_FEATURE_{section.id === 'extraction' ? 'EXTRACTION' : 'DISCOVERY'}=true
          </code>{' '}
          in your frontend env and rebuild to re-enable it.
        </p>
      </div>
    </div>
  );
}
