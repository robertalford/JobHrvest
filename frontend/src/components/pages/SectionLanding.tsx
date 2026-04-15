import { Link } from 'react-router-dom';
import { ArrowRight, Lock, Sparkles } from 'lucide-react';
import { SECTIONS } from '../../lib/sections';

const EXTRA_CARDS = [
  {
    id: 'perplexity-v2',
    title: 'Perplexity v2',
    tagline: 'A copy of the existing functionality so we can preserve the current Perplexity experience.',
    path: '/perplexity-v2',
  },
  {
    id: 'perplexity-v3',
    title: 'Perplexity v3',
    tagline: 'A new iteration focused on improving the current Perplexity script and prompt with more features.',
    path: '/perplexity-v3',
  },
];

export function SectionLanding() {
  return (
    <div className="max-w-6xl mx-auto p-8">
      <div className="mb-8">
        <h1 className="text-2xl font-semibold text-gray-900">JobHarvest</h1>
        <p className="mt-1 text-sm text-gray-600">
          Choose a feature area to get started. Extraction and Domain Discovery are paused while we
          optimise the Site Config models.
        </p>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-3 gap-5">
        {SECTIONS.map((section) => {
          const Icon = section.icon;
          const disabled = !section.enabled;
          const body = (
            <div
              className={`h-full bg-white border rounded-lg p-6 flex flex-col gap-4 transition-shadow ${
                disabled
                  ? 'border-gray-200 opacity-70'
                  : 'border-gray-200 hover:shadow-md hover:border-brand/40'
              }`}
            >
              <div className="flex items-center justify-between">
                <div
                  className={`inline-flex items-center justify-center w-10 h-10 rounded-lg ${section.accentClass}`}
                >
                  <Icon className="w-5 h-5" />
                </div>
                {disabled && (
                  <span className="inline-flex items-center gap-1 text-[11px] font-medium uppercase tracking-wider text-gray-500">
                    <Lock className="w-3 h-3" /> Disabled
                  </span>
                )}
              </div>
              <div>
                <h2 className="text-lg font-semibold text-gray-900">{section.title}</h2>
                <p className="mt-1 text-sm text-gray-600 leading-relaxed">{section.tagline}</p>
              </div>
              <div className="mt-auto pt-2">
                <span
                  className={`inline-flex items-center gap-1 text-sm font-medium ${
                    disabled ? 'text-gray-400' : 'text-brand'
                  }`}
                >
                  {disabled ? 'Coming soon' : 'Open'} <ArrowRight className="w-4 h-4" />
                </span>
              </div>
            </div>
          );

          return disabled ? (
            <Link to={section.path} key={section.id} className="block">
              {body}
            </Link>
          ) : (
            <Link to={section.path} key={section.id} className="block">
              {body}
            </Link>
          );
        })}

        {EXTRA_CARDS.map((card) => (
          <Link to={card.path} key={card.id} className="block">
            <div className="h-full bg-white border border-gray-200 rounded-lg p-6 flex flex-col gap-4 transition-shadow hover:shadow-md hover:border-brand/40">
              <div className="flex items-center justify-between">
                <div className="inline-flex items-center justify-center w-10 h-10 rounded-lg bg-emerald-50 text-emerald-700">
                  <Sparkles className="w-5 h-5" />
                </div>
              </div>
              <div>
                <h2 className="text-lg font-semibold text-gray-900">{card.title}</h2>
                <p className="mt-1 text-sm text-gray-600 leading-relaxed">{card.tagline}</p>
              </div>
              <div className="mt-auto pt-2">
                <span className="inline-flex items-center gap-1 text-sm font-medium text-brand">
                  Open <ArrowRight className="w-4 h-4" />
                </span>
              </div>
            </div>
          </Link>
        ))}
      </div>
    </div>
  );
}
