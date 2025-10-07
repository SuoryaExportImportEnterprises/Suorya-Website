import { ArrowLeft } from "lucide-react";
import { Button } from "./ui/button";
import { ImageWithFallback } from "./figma/ImageWithFallback";

interface Subcategory {
  name: string;
  description: string;
  imageUrl: string;
}

interface CategoryLandingPageProps {
  categoryName: string;
  coverImageUrl: string;
  subcategories: Subcategory[];
  onBack: () => void;
  onSubcategoryClick: (subcategoryName: string) => void;
}

export function CategoryLandingPage({
  categoryName,
  coverImageUrl,
  subcategories,
  onBack,
  onSubcategoryClick,
}: CategoryLandingPageProps) {
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

      {/* Cover Section - Reduced height */}
      <div className="relative h-[300px] md:h-[400px] w-full overflow-hidden mb-20">
        <ImageWithFallback
          src={coverImageUrl}
          alt={categoryName}
          className="w-full h-full object-cover"
        />
        <div className="absolute inset-0 bg-gradient-to-t from-black/70 via-black/40 to-transparent"></div>
        <div className="absolute inset-0 flex items-center justify-center">
          <div className="text-center text-white space-y-4 px-4">
            <h1 className="text-5xl md:text-7xl lg:text-8xl tracking-wide">
              {categoryName}
            </h1>
            <p className="text-xl md:text-2xl max-w-3xl mx-auto text-white/90">
              Explore our premium collection
            </p>
          </div>
        </div>
      </div>

      {/* Subcategory Blocks */}
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 pb-20">
        <div className="text-center mb-12">
          <h2 className="text-3xl md:text-4xl text-stone-800 mb-4">
            Select a Category
          </h2>
          <div className="w-24 h-1 bg-orange-600 mx-auto"></div>
        </div>

        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4 gap-6">
          {subcategories.map((subcategory, index) => (
            <button
              key={index}
              onClick={() => onSubcategoryClick(subcategory.name)}
              className="group bg-white rounded-xl overflow-hidden shadow-md hover:shadow-2xl transition-all duration-300 hover:-translate-y-2 text-left"
            >
              <div className="aspect-[4/3] overflow-hidden bg-stone-100">
                <ImageWithFallback
                  src={subcategory.imageUrl}
                  alt={subcategory.name}
                  className="w-full h-full object-cover group-hover:scale-110 transition-transform duration-500"
                />
              </div>
              <div className="p-6">
                <h3 className="text-xl text-stone-800 mb-2 group-hover:text-orange-600 transition-colors duration-300">
                  {subcategory.name}
                </h3>
                <p className="text-stone-600 text-sm leading-relaxed">
                  {subcategory.description}
                </p>
              </div>
            </button>
          ))}
        </div>
      </div>
    </div>
  );
}