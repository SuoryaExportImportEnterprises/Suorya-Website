import { useState } from "react";
import { Navigation } from "./components/Navigation";
import { Hero } from "./components/Hero";
import { AboutUs } from "./components/AboutUs";
import { StorylineSection } from "./components/StorylineSection";
import { VisionSection } from "./components/VisionSection";
import { ProductCatalog } from "./components/ProductCatalog";
import { PortfolioGrid } from "./components/PortfolioGrid";
import { ContactPage } from "./components/ContactPage";
import { CategoryLandingPage } from "./components/CategoryLandingPage";
import { SubcategoryDetailPage } from "./components/SubcategoryDetailPage";
import { ScrollableCategoryPage } from "./components/ScrollableCategoryPage";
import { SimpleCategoryPage } from "./components/SimpleCategoryPage";
import { SearchResults } from "./components/SearchResults";
import { Footer } from "./components/Footer";

const generateImages = (fileNames: string[], baseName: string, pathKey: string) => {
  return fileNames.map((fileName, i) => {

    const imageUrl = `/products/${pathKey.toLowerCase()}/${fileName}`;
    
    return {
      name: `${baseName} ${i + 1}`,
      imageUrl,
    };
  });
};

// --- START OF CATEGORY DATA ---
const categoryData = {
  ribbons: {
    type: "complex" as const,
    name: "Ribbons",
    coverImageUrl: "/cover-images/ribbon-cover/cover_tinsel.png", 
    subcategories: [
      {
        name: "VELVET RIBBONS",
        description: "Soft-touch velvet ribbons: Cotton, Polyester, Nylon, Finished Edge",
        imageUrl: "/cover-images/ribbon-cover/cover_VELVET -FINISH EDGE.jpg", 
        subSubcategories: [
          { 
            name: "Handfrayed Cotton Velvet", 
            images: generateImages(['cotton-velvet-1.jpg', 'cotton-velvet-2.jpg', 'cotton-velvet-3.jpg'], "Cotton Velvet", "ribbons/velvet/cotton") 
          },
          { 
            name: "Handfrayed Polyester Velvet", 
            images: generateImages(['poly-velvet-1.jpg', 'poly-velvet-2.jpg', 'poly-velvet-3.jpg'], "Polyester Velvet", "ribbons/velvet/polyester") 
          },
          { 
            name: "Handfrayed Nylon Velvet", 
            images: generateImages(['nylon1.jpg', 'nylon2.jpg', 'nylon3.jpg', 'nylon4.jpg'], "Nylon Velvet", "ribbon/velvet_ribbons/nylon") 
          },
          { 
            name: "Finished Edge", 
            images: generateImages(['finishedge1.jpg', 'finishedge2.jpg', 'finishededge3.jpg','finishededge4.jpg'], "Finished Edge Velvet", "ribbon/velvet_ribbons/finishedge") 
          },
        ],
      },
      {
        name: "SATIN RIBBONS",
        description: "Smooth poly satin ribbons - Single Sided & Double Sided",
        imageUrl: "/cover-images/ribbon-cover/cover_doublesided.jpg", 
        subSubcategories: [
          { 
            name: "Single Sided Poly Satin",
            images: generateImages(['singlesatin1.jpg'], "Single Sided Satin", "ribbon/satinribbon/single") 
          },
          { 
            name: "Double Sided Poly Satin",
            images: generateImages(['doubleside1.jpg', 'doublesided2.jpg', 'doublesided3.jpg'], "Double Sided Satin", "ribbon/satinribbon/double") 
          },
          // { 
          //   name: "Show sizes",
          //   images: generateImages(['satin-size-1.jpg', 'satin-size-2.jpg', 'satin-size-3.jpg'], "Satin Size Options", "ribbons/satin/sizes") 
          // },
        ],
      },
      // {
      //   name: "GROSSGRAIN RIBBONS",
      //   description: "Poly grossgrain ribbons in various widths",
      //   imageUrl: "/cover-images/grossgrain-sub.jpg", 
      //   subSubcategories: [
      //     { 
      //       name: "Poly grossgrain", 
      //       images: generateImages(['grossgrain-1.jpg', 'grossgrain-2.jpg', 'grossgrain-3.jpg'], "Poly Grossgrain", "ribbons/grossgrain") 
      //     },
      //   ],
      // },
      {
        name: "PRINTED RIBBONS",
        description: "Printed designs on Satin and Grossgrain",
        imageUrl: "/cover-images/ribbon-cover/cover_ongrossgrain.png",
        subSubcategories: [
          { 
            name: "on Satin",
            images: generateImages(['satin1.jpg', 'satin2.jpg'], "Printed on Satin", "ribbon/printed/on satin") 
          },
          { 
            name: "on Grossgrain",
            images: generateImages(['grossgrain1.jpg', 'grossgrain2.jpg'], "Printed on Grossgrain", "ribbon/printed/on grossgrain") 
          },
        ],
      },
      {
        name: "BRAIDED",
        description: "Classic braided ribbon designs",
        imageUrl: "/cover-images/ribbon-cover/braided cover.png",
        subSubcategories: [
          { 
            name: "Braided", 
            images: generateImages(['braided1.png', 'braided2.png', 'braided3.png'], "Braided Ribbon", "/ribbon/Braided") 
          },
        ],
      },
      {
        name: "TINSEL",
        description: "Festive tinsel ribbons for celebrations",
        imageUrl: "/cover-images/ribbon-cover/cover_tinsel.png",
        subSubcategories: [
          { 
            name: "Tinsel", 
            images: generateImages(['tinsel.jpg'], "Tinsel Ribbon", "ribbon/tinsel") 
          },
        ],
      },
      
      {
        name: "DIE CUT",
        description: "Intricately cut ribbons with unique patterns",
        imageUrl: "/cover-images/ribbon-cover/Cover_DieCut.jpg", 
        subSubcategories: [
          { 
            name: "Die Cut", 
            images: generateImages(['DIE CUT1.jpg', 'Diecut2.jpg', 'DIE CUT3.jpg', 'DIE CUT4.jpg'], "Die Cut Ribbon", "ribbon/diecut") 
          },
        ],
      },
      {
        name: "PEARL RIBBONS",
        description: "Elegant pearl-embellished ribbons",
        imageUrl: "/cover-images/ribbon-cover/Cover_Pearl.png",
        subSubcategories: [
          { 
            name: "Pearl Ribbons", 
            images: generateImages(['pearl1.png', 'pearl2.png', 'pearl3.png'], "Pearl Ribbon", "ribbon/pearlribbons") 
          },
        ],
      },
      {
        name: "EMBROIDERED RIBBONS",
        description: "Hand-embroidered ribbons - Floral, Sequins, Embellished, Mirror work",
        imageUrl: "/cover-images/ribbon-cover/sequin cover.png",
        subSubcategories: [
          { 
            name: "Floral", 
            images: generateImages(['floral-1.jpg', 'floral-2.jpg', 'floral-3.jpg'], "Floral Embroidered", "ribbons/embroidered/floral") 
          },
          { 
            name: "Sequins", 
            images: generateImages(['sequin1.png', 'sequin2.png', 'sequin3.png', 'sequin4.png'], "Sequin Embroidered", "ribbon/embroidered/sequins") 
          },
          { 
            name: "Embellished", 
            images: generateImages(['embellished-1.jpg', 'embellished-2.jpg', 'embellished-3.jpg'], "Embellished Ribbon", "ribbons/embroidered/embellished") 
          },
          { 
            name: "Mirror", 
            images: generateImages(['mirror-1.jpg', 'mirror-2.jpg', 'mirror-3.jpg'], "Mirror Work Ribbon", "ribbons/embroidered/mirror") 
          },
        ],
      },
      {
        name: "JACQUARD",
        description: "Woven jacquard ribbons with intricate patterns",
        imageUrl: "/cover-images/ribbon-cover/jacquardcover.png",
        subSubcategories: [
          { 
            name: "Jacquard", 
            images: generateImages(['jacquard1.png', 'jacquard2.png', 'jacquard3.png', 'jacquard4.png', 'jacquard5.png'], "Jacquard Ribbon", "ribbon/jacquard") 
          },
        ],
      },
      {
        name: "JUTE",
        description: "Natural jute ribbons for rustic charm",
        imageUrl: "/cover-images/ribbon-cover/cover_jute.jpg",
        subSubcategories: [
          { 
            name: "Jute", 
            images: generateImages(['jute1.jpg', 'jute2.jpg', 'jute3.jpg', 'jute4.jpg'], "Jute Ribbon", "ribbon/jute") 
          },
        ],
      },
      {
        name: "SHEER RIBBONS",
        description: "Delicate sheer ribbons - Tulle/Net, Organza, Chiffon",
        imageUrl: "/cover-images/sheer-sub.jpg", 
        subSubcategories: [
          { 
            name: "Tulle/Net", 
            images: generateImages(['tulle-1.jpg', 'tulle-2.jpg', 'tulle-3.jpg'], "Tulle/Net Ribbon", "ribbons/sheer/tulle") 
          },
          { 
            name: "Organza", 
            images: generateImages(['organza-1.jpg', 'organza-2.jpg', 'organza-3.jpg'], "Organza Ribbon", "ribbons/sheer/organza") 
          },
          { 
            name: "Chiffon", 
            images: generateImages(['chiffon-1.jpg', 'chiffon-2.jpg', 'chiffon-3.jpg'], "Chiffon Ribbon", "ribbons/sheer/chiffon") 
          },
        ],
      },
      {
        name: "WIRED RIBBONS",
        description: "Ribbons with wire edges for easy shaping",
        imageUrl: "/cover-images/wired-sub.jpg", 
        subSubcategories: [
          { 
            name: "Wired Ribbons", 
            images: generateImages(['wired-1.jpg', 'wired-2.jpg', 'wired-3.jpg'], "Wired Ribbon", "ribbons/wired") 
          },
        ],
      },
      {
        name: "ABSTRACT POLY WOVEN",
        description: "Modern abstract woven patterns",
        imageUrl: "/cover-images/abstract-sub.jpg",
        subSubcategories: [
          { 
            name: "Abstract Poly Woven", 
            images: generateImages(['abstract-1.jpg', 'abstract-2.jpg', 'abstract-3.jpg'], "Abstract Poly Woven", "ribbons/abstract") 
          },
        ],
      },
    ],
  },
  trims: {
    type: "scrollable" as const,
    name: "Trims",
    coverImageUrl: "/cover-images/cover_ricrac.jpg",
    subcategories: [
      {
        name: "POMS",
        description: "Playful pom-pom trims that add texture and fun",
        images: generateImages(['pom1.jpeg', 'pom2.jpeg', 'pom3.jpeg'], "Pom Trim", "trims/poms"),
      },
      {
        name: "RIC RAC",
        description: "Zigzag decorative trims for vintage charm",
        images: generateImages(['ricrac1.jpg', 'ricrac2.jpg', 'ricrac3.jpg', 'ricrac4.jpg'], "Ric Rac Trim", "trims/ricrac"),
      },
      {
        name: "FRINGE",
        description: "Decorative fringe trims in various styles",
        images: generateImages(['f1.webp', 'f2.jpg', 'f3.jpeg'], "Fringe Trim", "trims/fringe"),
      },
      {
        name: "TASSELS",
        description: "Elegant tassels for finishing touches",
        images: generateImages(['tassel1.jpeg', 'tassel2.webp', 'tassel3.webp'], "Tassel Trim", "trims/tassels"),
      },
      {
        name: "LACE",
        description: "Delicate lace trims for elegant designs",
        images: generateImages(['lace1.jpg', 'lace2.jpg', 'lace3.avif'], "Lace Trim", "trims/laces"),
      },
      {
        name: "CORDS",
        description: "Decorative cords for elegant finishing touches",
        images: generateImages(['cord1.png', 'cord2.png', 'cord3.png', 'cord4.png'], "Cord Trim", "trims/cords"),
      },
    ],
  },
  bows: {
    type: "simple" as const,
    name: "Bows",
    coverImageUrl: "/cover-images/bow-coverimage.webp",
    images: generateImages(['clipon1.avif', 'clipon2.webp', 'clipon3.jpg'], "Christmas Bow", "bows/clipons"),
  },
  spool: {
    type: "simple" as const,
    name: "Spools",
    coverImageUrl: "/cover-images/spool cover.jpeg",
    images: generateImages(['paperspool-1.jpg', 'woodenspool-2.png'], "Ribbon Spool", "spools"),
  },
  "tree-toppers": {
    type: "simple" as const,
    name: "Tree Toppers",
    coverImageUrl: "/cover-images/treetop_final.webp",
    images: generateImages(['top1.avif', 'top2.jpg', 'top3.jpg'], "Tree Topper", "treetoppers"),
  },
  "tree-skirts": {
    type: "simple" as const,
    name: "Tree Skirts",
    coverImageUrl: "/cover-images/treeskirtscover.jpeg",
    images: generateImages(['skirt1.jpeg', 'skirt2.jpeg', 'skirt3.jpg'], "Tree Skirt", "treeskirts"),
  },
};
// --- END OF CATEGORY DATA ---

type PageState =
  | { type: "home" }
  | { type: "contact" }
  | { type: "category-landing"; categoryKey: string }
  | { type: "subcategory-detail"; categoryKey: string; subcategoryName: string }
  | { type: "scrollable-category"; categoryKey: string }
  | { type: "simple-category"; categoryKey: string }
  | { type: "search"; query: string; results: any[] };

export default function App() {
  const [currentPage, setCurrentPage] = useState<PageState>({
    type: "home",
  });

  const scrollToTop = () => {
    window.scrollTo({ top: 0, behavior: "smooth" });
  };

  const handleNavigate = (page: string) => {
    if (page === "home") {
      setCurrentPage({ type: "home" });
    } else if (page === "contact") {
      setCurrentPage({ type: "contact" });
    } else if (categoryData[page as keyof typeof categoryData]) {
      const category = categoryData[page as keyof typeof categoryData];
      
      if (category.type === "complex") {
        setCurrentPage({ type: "category-landing", categoryKey: page });
      } else if (category.type === "scrollable") {
        setCurrentPage({ type: "scrollable-category", categoryKey: page });
      } else if (category.type === "simple") {
        setCurrentPage({ type: "simple-category", categoryKey: page });
      }
    }
    scrollToTop();
  };

  const handleSearch = (query: string) => {
    const results: any[] = [];

    Object.entries(categoryData).forEach(([key, category]) => {
      if (category.name.toLowerCase().includes(query.toLowerCase())) {
        results.push({
          type: "category",
          name: category.name,
          description: `Explore our ${category.name.toLowerCase()} collection`,
          imageUrl: category.coverImageUrl,
        });
      }
    });

    setCurrentPage({ type: "search", query, results });
    scrollToTop();
  };

  // Render based on current page type
  if (currentPage.type === "contact") {
    return (
      <div className="min-h-screen">
        <Navigation
          onNavigate={handleNavigate}
          onSearch={handleSearch}
          currentPage="contact"
        />
        <ContactPage onBack={() => handleNavigate("home")} />
        <Footer onNavigate={handleNavigate} />
      </div>
    );
  }

  if (currentPage.type === "category-landing") {
    const category = categoryData[currentPage.categoryKey as keyof typeof categoryData];
    if (category.type !== "complex") return null;

    return (
      <div className="min-h-screen">
        <Navigation
          onNavigate={handleNavigate}
          onSearch={handleSearch}
          currentPage="category"
        />
        <CategoryLandingPage
          categoryName={category.name}
          coverImageUrl={category.coverImageUrl}
          subcategories={category.subcategories.map((sub) => ({
            name: sub.name,
            description: sub.description,
            imageUrl: sub.imageUrl,
          }))}
          onBack={() => handleNavigate("home")}
          onSubcategoryClick={(subcategoryName) => {
            setCurrentPage({
              type: "subcategory-detail",
              categoryKey: currentPage.categoryKey,
              subcategoryName,
            });
            scrollToTop();
          }}
        />
        <Footer onNavigate={handleNavigate} />
      </div>
    );
  }

  if (currentPage.type === "subcategory-detail") {
    const category = categoryData[currentPage.categoryKey as keyof typeof categoryData];
    if (category.type !== "complex") return null;

    const subcategory = category.subcategories.find(
      (sub) => sub.name === currentPage.subcategoryName
    );
    if (!subcategory) return null;

    return (
      <div className="min-h-screen">
        <Navigation
          onNavigate={handleNavigate}
          onSearch={handleSearch}
          currentPage="subcategory"
        />
        <SubcategoryDetailPage
          categoryName={category.name}
          subcategoryName={subcategory.name}
          coverImageUrl={subcategory.imageUrl}
          subSubcategories={subcategory.subSubcategories}
          onBack={() =>
            setCurrentPage({
              type: "category-landing",
              categoryKey: currentPage.categoryKey,
            })
          }
        />
        <Footer onNavigate={handleNavigate} />
      </div>
    );
  }

  if (currentPage.type === "scrollable-category") {
    const category = categoryData[currentPage.categoryKey as keyof typeof categoryData];
    if (category.type !== "scrollable") return null;

    return (
      <div className="min-h-screen">
        <Navigation
          onNavigate={handleNavigate}
          onSearch={handleSearch}
          currentPage="category"
        />
        <ScrollableCategoryPage
          categoryName={category.name}
          coverImageUrl={category.coverImageUrl}
          subcategories={category.subcategories}
          onBack={() => handleNavigate("home")}
        />
        <Footer onNavigate={handleNavigate} />
      </div>
    );
  }

  if (currentPage.type === "simple-category") {
    const category = categoryData[currentPage.categoryKey as keyof typeof categoryData];
    if (category.type !== "simple") return null;

    return (
      <div className="min-h-screen">
        <Navigation
          onNavigate={handleNavigate}
          onSearch={handleSearch}
          currentPage="category"
        />
        <SimpleCategoryPage
          categoryName={category.name}
          coverImageUrl={category.coverImageUrl}
          images={category.images}
          onBack={() => handleNavigate("home")}
        />
        <Footer onNavigate={handleNavigate} />
      </div>
    );
  }

  if (currentPage.type === "search") {
    return (
      <div className="min-h-screen">
        <Navigation
          onNavigate={handleNavigate}
          onSearch={handleSearch}
          currentPage="search"
        />
        <SearchResults
          query={currentPage.query}
          results={currentPage.results}
          onBack={() => handleNavigate("home")}
          onResultClick={(result) => {
            if (result.type === "category") {
              const categoryKey = Object.keys(categoryData).find(
                (key) => categoryData[key as keyof typeof categoryData].name === result.name
              );
              if (categoryKey) handleNavigate(categoryKey);
            }
          }}
        />
        <Footer onNavigate={handleNavigate} />
      </div>
    );
  }

  // Home page
  return (
    <div className="min-h-screen">
      <Navigation onNavigate={handleNavigate} onSearch={handleSearch} currentPage="home" />
      <Hero />
      <AboutUs />

      <StorylineSection
        title="Heritage & Expertise"
        subtitle="Our Foundation"
        description="With over three decades of experience, Suorya Exports stands as a symbol of trust, craftsmanship, and innovation. Our heritage is built on a foundation of quality - each ribbon, trim, and décor piece crafted with meticulous attention to detail and a promise of perfection. We take pride in being timeless yet on trend, preserving traditional artistry while embracing modern design directions that define global décor aesthetics."
        imageUrl="/home/heritage.jpg" 
        imageAlt="Elegant craftsmanship"
        bgColor="bg-white"
      />

      <StorylineSection
        title="Creativity & Collaboration"
        subtitle="A Partnership of Vision"
        description="At Suorya Exports, creativity thrives through collaboration. Our in-house design team works hand-in-hand with our global buying partners to bring visionary ideas to life. Together, we blend artistic intuition with commercial insight ensuring that every creation is not only beautiful but also purposeful. It’s this harmony between design and partnership that allows us to deliver truly exceptional products, season after season."
        imageUrl="/home/creative.png"
        imageAlt="Creative collaboration"
        reverse={true}
        bgColor="bg-stone-50"
      />

      <StorylineSection
        title="Beyond Wrapping"
        subtitle="Where Our Ribbons Live"
        description="We’re not just about wrapping - we’re about redefining it. Our collections go beyond ribbons and décor; they capture emotion, artistry, and the joy of celebration. Every product we create is an invitation to experience elegance in its most expressive form. Because at Suorya Exports, we believe that beauty lies not just in how something looks, but in how it makes you feel."
        imageUrl="/home/beyond2.jpg"
        imageAlt="Festive decoration"
        bgColor="bg-white"
      />

      <VisionSection />
      <PortfolioGrid />
      <ProductCatalog />
      
      <Footer onNavigate={handleNavigate} />
    </div>
  );
}
