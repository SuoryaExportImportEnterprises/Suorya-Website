import { ImageWithFallback } from "./figma/ImageWithFallback";

interface StorylineSectionProps {
  title: string;
  subtitle: string;
  description: string;
  imageUrl: string;
  imageAlt: string;
  reverse?: boolean;
  bgColor?: string;
}

export function StorylineSection({
  title,
  subtitle,
  description,
  imageUrl,
  imageAlt,
  reverse = false,
  bgColor = "bg-white",
}: StorylineSectionProps) {
  return (
    <section className={`py-16 ${bgColor}`}>
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
        <div
          className={`grid lg:grid-cols-2 gap-12 items-center ${
            reverse ? "lg:grid-flow-dense" : ""
          }`}
        >
          {/* Image */}
          <div
            className={`relative h-[500px] rounded-2xl overflow-hidden shadow-xl ${
              reverse ? "lg:col-start-2" : ""
            }`}
          >
            <ImageWithFallback
              src={imageUrl}
              alt={imageAlt}
              className="w-full h-full object-cover"
            />
            <div className="absolute inset-0 bg-gradient-to-br from-orange-600/10 to-transparent"></div>
          </div>

          {/* Content */}
          <div
            className={`space-y-6 ${reverse ? "lg:col-start-1 lg:row-start-1" : ""}`}
          >
            <div className="space-y-3">
              <p className="text-orange-600 tracking-widest uppercase text-sm">
                {subtitle}
              </p>
              <h2 className="text-4xl md:text-5xl text-stone-800 leading-tight">
                {title}
              </h2>
            </div>

            <p className="text-lg text-stone-600 leading-relaxed">
              {description}
            </p>

            <div className="pt-4">
              <a
                href="#products"
                className="inline-flex items-center text-orange-600 hover:text-orange-700 transition-colors duration-300 group"
              >
                <span className="border-b-2 border-orange-600 group-hover:border-orange-700">
                  Explore Our Collections
                </span>
                <svg
                  className="w-5 h-5 ml-2 group-hover:translate-x-1 transition-transform duration-300"
                  fill="none"
                  stroke="currentColor"
                  viewBox="0 0 24 24"
                >
                  <path
                    strokeLinecap="round"
                    strokeLinejoin="round"
                    strokeWidth={2}
                    d="M17 8l4 4m0 0l-4 4m4-4H3"
                  />
                </svg>
              </a>
            </div>
          </div>
        </div>
      </div>
    </section>
  );
}