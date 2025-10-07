import { ImageWithFallback } from "./figma/ImageWithFallback";

interface CatalogItem {
  name: string;
  description: string;
  image: string;
}

interface CatalogCategory {
  category: string;
  items: CatalogItem[];
}

const catalogItems: CatalogCategory[] = [
  {
    category: "Ribbons",
    items: [
      {
        name: "Frayed Velvet",
        description:
          "Soft-touch velvet with delicate frayed edges for a vintage, romantic aesthetic",
        image:
          "https://images.unsplash.com/photo-1638006353284-eca071dd9238?crop=entropy&cs=tinysrgb&fit=max&fm=jpg&ixid=M3w3Nzg4Nzd8MHwxfHNlYXJjaHwxfHx2ZWx2ZXQlMjBmYWJyaWMlMjB0ZXh0dXJlfGVufDF8fHx8MTc1OTIxNTA2OHww&ixlib=rb-4.1.0&q=80&w=1080&utm_source=figma&utm_medium=referral",
      },
      {
        name: "Lurex Shimmer",
        description:
          "Metallic threading woven through premium fabric for an opulent finish",
        image:
          "https://images.unsplash.com/photo-1758606137571-79fb4dd7d5a4?crop=entropy&cs=tinysrgb&fit=max&fm=jpg&ixid=M3w3Nzg4Nzd8MHwxfHNlYXJjaHwxfHxsdXh1cnklMjB0ZXh0aWxlJTIwZGV0YWlsfGVufDF8fHx8MTc1OTI1MjQ3MHww&ixlib=rb-4.1.0&q=80&w=1080&utm_source=figma&utm_medium=referral",
      },
      {
        name: "Satin Classic",
        description:
          "Timeless smooth satin in a curated palette, perfect for any occasion",
        image:
          "https://images.unsplash.com/photo-1719936319627-78b5ca4a28fc?crop=entropy&cs=tinysrgb&fit=max&fm=jpg&ixid=M3w3Nzg4Nzd8MHwxfHNlYXJjaHwxfHxzYXRpbiUyMHJpYmJvbiUyMGJvd3xlbnwxfHx8fDE3NTkyNTI0Njl8MA&ixlib=rb-4.1.0&q=80&w=1080&utm_source=figma&utm_medium=referral",
      },
    ],
  },
  {
    category: "Trims & Details",
    items: [
      {
        name: "Braided Elegance",
        description:
          "Intricately woven braided trims that add dimension to any design",
        image:
          "https://images.unsplash.com/photo-1758789891883-2058dd8d5898?crop=entropy&cs=tinysrgb&fit=max&fm=jpg&ixid=M3w3Nzg4Nzd8MHwxfHNlYXJjaHwxfHxkZWNvcmF0aXZlJTIwdHJpbSUyMGRldGFpbHxlbnwxfHx8fDE3NTkyNTI0NzB8MA&ixlib=rb-4.1.0&q=80&w=1080&utm_source=figma&utm_medium=referral",
      },
      {
        name: "Ric Rac Classic",
        description:
          "Wavy, zigzag trim that brings playful sophistication to projects",
        image:
          "https://images.unsplash.com/photo-1535551393484-1a1907f51759?crop=entropy&cs=tinysrgb&fit=max&fm=jpg&ixid=M3w3Nzg4Nzd8MHwxfHNlYXJjaHwxfHxsdXh1cnklMjByaWJib24lMjBjcmFmdHN8ZW58MXx8fHwxNzU5MjUyNDY3fDA&ixlib=rb-4.1.0&q=80&w=1080&utm_source=figma&utm_medium=referral",
      },
    ],
  },
];

export function ProductCatalog() {
  return (
    <section id="products" className="py-8 bg-white">
      

        {/* CTA */}
        <div className="mt-10 text-center bg-stone-50 rounded-2xl p-5">
          <h3 className="text-3xl text-stone-800 mb-4">
            Looking for Something Specific?
          </h3>
          <p className="text-lg text-stone-600 mb-6 max-w-2xl mx-auto">
            Our full catalog includes hundreds of designs across all categories.
            Each piece is crafted with the same attention to detail and
            commitment to quality.
          </p>

          <a href="#contact" className="inline-flex items-center px-8 py-4 bg-orange-600 text-white rounded-full hover:bg-orange-700 transition-colors duration-300" > Request Full Catalog </a>
        </div>
    </section>
  );
}