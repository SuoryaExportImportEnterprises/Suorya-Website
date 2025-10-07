import { ImageWithFallback } from "./figma/ImageWithFallback";

interface PortfolioItem {
  title: string;
  image: string;
}

const portfolioItems: PortfolioItem[] = [
  {
    title: "Boutique Displays",
    image:
      "https://images.unsplash.com/photo-1453834190665-46ff0a1fbd5a?crop=entropy&cs=tinysrgb&fit=max&fm=jpg&ixid=M3w3Nzg4Nzd8MHwxfHNlYXJjaHwxfHxlbGVnYW50JTIwZ2lmdCUyMHdyYXBwaW5nfGVufDF8fHx8MTc1OTI1MjQ2N3ww&ixlib=rb-4.1.0&q=80&w=1080&utm_source=figma&utm_medium=referral",
  },
  {
    title: "Floral Arrangements",
    image:
      "https://images.unsplash.com/photo-1742836531212-c55044926fbd?crop=entropy&cs=tinysrgb&fit=max&fm=jpg&ixid=M3w3Nzg4Nzd8MHwxfHNlYXJjaHwxfHxmbG9yYWwlMjBib3V0aXF1ZSUyMGRlc2lnbnxlbnwxfHx8fDE3NTkyNTI0Njl8MA&ixlib=rb-4.1.0&q=80&w=1080&utm_source=figma&utm_medium=referral",
  },
  {
    title: "Holiday Elegance",
    image:
      "https://images.unsplash.com/photo-1692884020981-ff239e22cd09?crop=entropy&cs=tinysrgb&fit=max&fm=jpg&ixid=M3w3Nzg4Nzd8MHwxfHNlYXJjaHwxfHxjaHJpc3RtYXMlMjBkZWNvcmF0aW9uJTIwZWxlZ2FudHxlbnwxfHx8fDE3NTkyNTI0Njh8MA&ixlib=rb-4.1.0&q=80&w=1080&utm_source=figma&utm_medium=referral",
  },
  {
    title: "Luxury Gift Wrapping",
    image:
      "https://images.unsplash.com/photo-1535551393484-1a1907f51759?crop=entropy&cs=tinysrgb&fit=max&fm=jpg&ixid=M3w3Nzg4Nzd8MHwxfHNlYXJjaHwxfHxsdXh1cnklMjByaWJib24lMjBjcmFmdHN8ZW58MXx8fHwxNzU5MjUyNDY3fDA&ixlib=rb-4.1.0&q=80&w=1080&utm_source=figma&utm_medium=referral",
  },
  {
    title: "Contemporary Wall Art",
    image:
      "https://images.unsplash.com/photo-1589223095536-e5a50ece9a48?crop=entropy&cs=tinysrgb&fit=max&fm=jpg&ixid=M3w3Nzg4Nzd8MHwxfHNlYXJjaHwxfHxmZXN0aXZlJTIwZ2FybGFuZCUyMGRlY29yfGVufDF8fHx8MTc1OTI1MjQ3MHww&ixlib=rb-4.1.0&q=80&w=1080&utm_source=figma&utm_medium=referral",
  },
  {
    title: "Event Décor",
    image:
      "https://images.unsplash.com/photo-1719936319627-78b5ca4a28fc?crop=entropy&cs=tinysrgb&fit=max&fm=jpg&ixid=M3w3Nzg4Nzd8MHwxfHNlYXJjaHwxfHxzYXRpbiUyMHJpYmJvbiUyMGJvd3xlbnwxfHx8fDE3NTkyNTI0Njl8MA&ixlib=rb-4.1.0&q=80&w=1080&utm_source=figma&utm_medium=referral",
  },
];

export function PortfolioGrid() {
  return (
    <section className="py-24 bg-stone-50">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
        {/* Section Header */}
        <div className="text-center mb-16 space-y-4">
          <p className="text-orange-600 tracking-widest uppercase text-sm">
            Inspiration Gallery
          </p>
          <h2 className="text-4xl md:text-5xl text-stone-800">
            See Our Creations in Action
          </h2>
          <p className="text-xl text-stone-600 max-w-2xl mx-auto">
            From boutique windows to special events, our ribbons and décor bring
            creative visions to life
          </p>
        </div>

        {/* Masonry Grid */}
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
          {portfolioItems.map((item, index) => (
            <div
              key={item.title}
              className={`group relative overflow-hidden rounded-2xl shadow-lg hover:shadow-2xl transition-all duration-500 ${
                index % 3 === 0 ? "md:row-span-2" : ""
              }`}
            >
              <div
                className={`relative overflow-hidden ${
                  index % 3 === 0 ? "aspect-[3/4]" : "aspect-square"
                }`}
              >
                <ImageWithFallback
                  src={item.image}
                  alt={item.title}
                  className="w-full h-full object-cover group-hover:scale-110 transition-transform duration-700"
                />
                <div className="absolute inset-0 bg-gradient-to-t from-stone-900/80 via-stone-900/20 to-transparent opacity-60 group-hover:opacity-80 transition-opacity duration-500"></div>

                <div className="absolute inset-0 flex items-end p-6">
                  <div className="transform translate-y-4 group-hover:translate-y-0 transition-transform duration-500">
                    <h3 className="text-2xl text-white mb-2">
                      {item.title}
                    </h3>
                    <div className="w-12 h-1 bg-orange-600 opacity-0 group-hover:opacity-100 transition-opacity duration-500"></div>
                  </div>
                </div>
              </div>
            </div>
          ))}
        </div>
      </div>
    </section>
  );
}