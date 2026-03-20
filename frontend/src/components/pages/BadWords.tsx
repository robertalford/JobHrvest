import { Type } from 'lucide-react';
import { WordFilterSection } from './settings-shared';

export function BadWords() {
  return (
    <div className="p-6 space-y-6">
      <div className="flex items-center gap-3">
        <div className="w-10 h-10 rounded-xl bg-yellow-100 flex items-center justify-center">
          <Type className="w-5 h-5 text-yellow-600" />
        </div>
        <div>
          <h1 className="text-2xl font-bold text-gray-900">Bad Words</h1>
          <p className="text-sm text-gray-500">Words that indicate low-quality or suspicious job listings</p>
        </div>
      </div>
      <WordFilterSection filterType="bad_word" title="Bad Words" />
    </div>
  );
}
