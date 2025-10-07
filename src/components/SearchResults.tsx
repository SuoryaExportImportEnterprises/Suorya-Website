import { ArrowLeft } from "lucide-react";
import { Button } from "./ui/button";
import { ImageWithFallback } from "./figma/ImageWithFallback";

interface SearchResult {
  type: string;
  name: string;
  category?: string;
  description: string;
  imageUrl: string;
}

interface SearchResultsProps {
  query: string;
  results: SearchResult[];
  onBack: () => void;
  onResultClick: (result: SearchResult) => void;
}

export function SearchResults({
  query,
  results,
  onBack,
  onResultClick,
}: SearchResultsProps) {
  return (
    <div className="min-h-screen bg-white pt-20">
      {/* Back Button */}
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-4">
        <Button
          variant="ghost"
          onClick={onBack}
          className="flex items-center gap-2 text-stone-700 hover:text-orange-600"
        >
          <ArrowLeft className="w-4 h-4" />
          Back to Home
        </Button>
      </div>

      {/* Page Header */}
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
        <div className="mb-16 space-y-4">
          <p className="text-orange-600 tracking-widest uppercase text-sm">
            Search Results
          </p>
          <h1 className="text-4xl md:text-5xl text-stone-800">
            Results for "{query}"
          </h1>
          <p className="text-lg text-stone-600">
            Found {results.length} {results.length === 1 ? "result" : "results"}
          </p>
        </div>

        {/* Results Grid */}
        {results.length > 0 ? (
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-8 mb-16">
            {results.map((result, index) => (
              <button
                key={index}
                onClick={() => onResultClick(result)}
                className="group bg-white rounded-2xl overflow-hidden shadow-md hover:shadow-xl transition-all duration-300 hover:-translate-y-1 text-left"
              >
                <div className="aspect-[4/3] overflow-hidden bg-stone-100">
                  <ImageWithFallback
                    src={result.imageUrl}
                    alt={result.name}
                    className="w-full h-full object-cover group-hover:scale-105 transition-transform duration-500"
                  />
                </div>
                <div className="p-6">
                  <div className="flex items-center gap-2 mb-2">
                    <span className="text-xs px-2 py-1 bg-orange-100 text-orange-600 rounded-full">
                      {result.type}
                    </span>
                    {result.category && (
                      <span className="text-xs text-stone-500">
                        in {result.category}
                      </span>
                    )}
                  </div>
                  <h3 className="text-xl text-stone-800 mb-2 group-hover:text-orange-600 transition-colors duration-300">
                    {result.name}
                  </h3>
                  <p className="text-stone-600 text-sm leading-relaxed">
                    {result.description}
                  </p>
                </div>
              </button>
            ))}
          </div>
        ) : (
          <div className="text-center py-16">
            <div className="w-24 h-24 mx-auto mb-6 bg-stone-100 rounded-full flex items-center justify-center">
              <svg
                className="w-12 h-12 text-stone-400"
                fill="none"
                viewBox="0 0 24 24"
                stroke="currentColor"
              >
                <path
                  strokeLinecap="round"
                  strokeLinejoin="round"
                  strokeWidth={2}
                  d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z"
                />
              </svg>
            </div>
            <h2 className="text-2xl text-stone-800 mb-4">No results found</h2>
            <p className="text-stone-600 mb-8">
              We couldn't find any products matching "{query}". Try a different
              search term.
            </p>
            <Button
              onClick={onBack}
              className="bg-orange-600 text-white hover:bg-orange-700 px-8 py-3 rounded-full"
            >
              Return Home
            </Button>
          </div>
        )}
      </div>
    </div>
  );
}