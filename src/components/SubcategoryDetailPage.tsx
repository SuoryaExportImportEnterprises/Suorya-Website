import { ArrowLeft } from "lucide-react";
import { Button } from "./ui/button";
import { ImageWithFallback } from "./figma/ImageWithFallback";

interface SubSubcategoryImage {
  name: string;
  imageUrl: string;
  description?: string;
}

interface SubSubcategory {
  name: string;
  images: SubSubcategoryImage[];
}

interface SubcategoryDetailPageProps {
  categoryName: string;
  subcategoryName: string;
  coverImageUrl: string;
  subSubcategories: SubSubcategory[];
  onBack: () => void;
}

export function SubcategoryDetailPage({
  categoryName,
  subcategoryName,
  coverImageUrl,
  subSubcategories,
  onBack,
}: SubcategoryDetailPageProps) {
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
          Back to {categoryName}
        </Button>
      </div>

      {/* Cover Section - Reduced height */}
      <div className="relative h-[280px] md:h-[350px] w-full overflow-hidden mb-16">
        <ImageWithFallback
          src={coverImageUrl}
          alt={subcategoryName}
          className="w-full h-full object-cover"
        />
        <div className="absolute inset-0 bg-gradient-to-t from-black/70 via-black/40 to-transparent"></div>
        <div className="absolute inset-0 flex items-center justify-center">
          <div className="text-center text-white space-y-3 px-4">
            <p className="text-orange-400 tracking-widest uppercase text-sm">
              {categoryName}
            </p>
            <h1 className="text-4xl md:text-6xl lg:text-7xl">
              {subcategoryName}
            </h1>
          </div>
        </div>
      </div>

      {/* Sub-Subcategory Sections */}
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 pb-20">
        {subSubcategories.map((subSubcat, index) => (
          <div key={index} className="mb-20 last:mb-0">
            {/* Sub-Subcategory Heading */}
            <div className="mb-8">
              <h2 className="text-3xl md:text-4xl text-stone-800 mb-3">
                {subSubcat.name}
              </h2>
              <div className="w-20 h-1 bg-orange-600"></div>
            </div>

            {/* Image Grid - Max 4 images per section, increased spacing */}
            <div className="grid grid-cols-2 md:grid-cols-4 gap-8">
              {subSubcat.images.slice(0, 4).map((image, imgIndex) => (
                <div
                  key={imgIndex}
                  className="bg-white rounded-lg overflow-hidden shadow-md hover:shadow-xl transition-shadow duration-300"
                >
                  <div className="aspect-square overflow-hidden bg-stone-100">
                    <ImageWithFallback
                      src={image.imageUrl}
                      alt={image.name}
                      className="w-full h-full object-cover"
                    />
                  </div>
                  <div className="p-3">
                    <h3 className="text-stone-800 mb-1 text-sm">
                      {image.name}
                    </h3>
                    {image.description && (
                      <p className="text-xs text-stone-600 line-clamp-2">
                        {image.description}
                      </p>
                    )}
                  </div>
                </div>
              ))}
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}