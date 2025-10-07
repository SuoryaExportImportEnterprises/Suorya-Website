import { ImageWithFallback } from "./figma/ImageWithFallback";

export function AboutUs() {
  return (
    <section id="about" className="py-16 bg-stone-50">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
        {/* Section Header */}
        <div className="text-center mb-16 space-y-4">
          <p className="text-orange-600 tracking-widest uppercase text-sm">
            The Heart Behind Suorya
          </p>
          <h2 className="text-4xl md:text-5xl text-stone-800">
            A Father - Daughter Journey
          </h2>
        </div>

        {/* Content Grid */}
        <div className="grid lg:grid-cols-2 gap-12 items-center">
          {/* Image */}
          <div className="relative h-[600px] rounded-2xl overflow-hidden shadow-2xl">
            <img
              src="/cover-images/craftsmanship-detail.jpg"
              alt="Suorya Craftsmanship"
              className="w-full h-full object-cover aspect-[16/10]"
            />
            <div className="absolute inset-0 bg-gradient-to-t from-stone-900/50 to-transparent"></div>
          </div>

          {/* Story Content */}
          <div className="space-y-8">
            <p className="text-lg text-stone-600 leading-relaxed">
              Suorya Exports is a story of legacy, creativity, and commitment - a
              father’s 30+ years of mastery in craftsmanship joined by his
              daughter’s fresh design perspective and collaborative spirit.
              Together, they’ve built more than a business; they’ve nurtured a
              partnership rooted in trust, innovation, and shared vision. With
              every ribbon, trim, and décor piece, their design and buying teams
              work hand-in-hand to ensure timeless elegance that remains on
              trend. Driven by an unwavering focus on quality, premium
              craftsmanship, and timely delivery, Suorya Exports continues to
              redefine what it means to create with purpose - celebrating a
              legacy that’s both enduring and ever-evolving.
            </p>

            <blockquote className="border-l-4 border-orange-600 pl-6 italic text-stone-700">
              "With more than 30 years of experience working with top
              international brands, we have followed the best export practices.
              Our products meet international quality standards. We are SEDEX
              certified. These certifications reflect our strong commitment
              towards ethical and responsible practices."
            </blockquote>
          </div>
        </div>
      </div>
    </section>
  );
}
