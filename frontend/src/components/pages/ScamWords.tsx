import { AlertTriangle } from 'lucide-react';
import { WordFilterSection } from './settings-shared';

export function ScamWords() {
  return (
    <div className="p-6 space-y-6">
      <div className="flex items-center gap-3">
        <div className="w-10 h-10 rounded-xl bg-red-100 flex items-center justify-center">
          <AlertTriangle className="w-5 h-5 text-red-600" />
        </div>
        <div>
          <h1 className="text-2xl font-bold text-gray-900">Scam / Fraud Words</h1>
          <p className="text-sm text-gray-500">Words that indicate scam or fraudulent job listings</p>
        </div>
      </div>
      <WordFilterSection filterType="scam_word" title="Scam / Fraud Words" />
    </div>
  );
}
