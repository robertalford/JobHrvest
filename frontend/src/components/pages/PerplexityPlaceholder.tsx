import { Link } from 'react-router-dom';

type PerplexityPlaceholderProps = {
  title: string;
  description: string;
};

export function PerplexityPlaceholder({ title, description }: PerplexityPlaceholderProps) {
  return (
    <div className="max-w-4xl mx-auto p-8">
      <div className="mb-6">
        <Link to="/" className="text-sm font-medium text-brand hover:underline">
          Back to JobHarvest
        </Link>
      </div>

      <div className="bg-white border border-gray-200 rounded-xl p-8 shadow-sm">
        <div className="inline-flex items-center rounded-full bg-emerald-50 px-3 py-1 text-xs font-semibold uppercase tracking-wide text-emerald-700">
          Placeholder
        </div>
        <h1 className="mt-4 text-3xl font-semibold text-gray-900">{title}</h1>
        <p className="mt-3 max-w-2xl text-base leading-7 text-gray-600">{description}</p>

        <div className="mt-8 rounded-lg border border-dashed border-gray-300 bg-gray-50 p-6">
          <p className="text-sm text-gray-700">
            This page is ready as a placeholder route and can be expanded with the Perplexity workflow next.
          </p>
        </div>
      </div>
    </div>
  );
}
