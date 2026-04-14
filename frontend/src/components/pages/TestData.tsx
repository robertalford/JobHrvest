import { Beaker } from 'lucide-react';

export function TestData() {
  return (
    <div className="max-w-5xl mx-auto p-8">
      <div className="mb-6">
        <h1 className="text-2xl font-semibold text-gray-900 flex items-center gap-2">
          <Beaker className="w-6 h-6 text-brand" /> Test Data
        </h1>
        <p className="mt-1 text-sm text-gray-600">
          Gold holdout sets used to evaluate the site-config champion and challenger models —
          stratified by market, ATS, and JS-rendering requirement.
        </p>
      </div>
      <div className="bg-white border border-gray-200 rounded-lg p-8 text-center text-sm text-gray-500">
        Holdout set browser — coming next. Seed via{' '}
        <code className="px-1 py-0.5 bg-gray-100 rounded text-xs">
          backend/scripts/build_gold_holdout.py
        </code>
        .
      </div>
    </div>
  );
}
