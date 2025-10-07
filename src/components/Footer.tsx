interface FooterProps {
  onNavigate: (page: string) => void;
}

export function Footer({ onNavigate }: FooterProps) {
  const handleNavigate = (page: string) => {
    if (onNavigate) {
      onNavigate(page);
    }
  };

  return (
    <footer className="bg-stone-900 text-stone-300 py-12">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
        <div className="grid md:grid-cols-4 gap-8 mb-8">
          {/* Brand */}
          <div className="space-y-4">
            <button
              onClick={() => handleNavigate("home")}
              className="text-2xl text-white tracking-wider hover:text-orange-400 transition-colors duration-300"
            >
              Suorya
            </button>
            <p className="text-sm leading-relaxed">
              Crafting premium ribbons and décor with excellence for over two
              decades.
            </p>
          </div>

          {/* Quick Links */}
          <div>
            <h4 className="text-white mb-4">Quick Links</h4>
            <ul className="space-y-2 text-sm">
              <li>
                <button
                  onClick={() => handleNavigate("home")}
                  className="hover:text-orange-400 transition-colors duration-300 text-left"
                >
                  Home
                </button>
              </li>
              <li>
                <button
                  onClick={() => handleNavigate("ribbons")}
                  className="hover:text-orange-400 transition-colors duration-300 text-left"
                >
                  Products
                </button>
              </li>
              <li>
                <button
                  onClick={() => handleNavigate("contact")}
                  className="hover:text-orange-400 transition-colors duration-300 text-left"
                >
                  Contact
                </button>
              </li>
            </ul>
          </div>

          {/* Categories */}
          <div>
            <h4 className="text-white mb-4">Categories</h4>
            <ul className="space-y-2 text-sm">
              <li>
                <button
                  onClick={() => handleNavigate("ribbons")}
                  className="hover:text-orange-400 transition-colors duration-300 text-left"
                >
                  Ribbons
                </button>
              </li>
              <li>
                <button
                  onClick={() => handleNavigate("trims")}
                  className="hover:text-orange-400 transition-colors duration-300 text-left"
                >
                  Trims
                </button>
              </li>
              <li>
                <button
                  onClick={() => handleNavigate("bows")}
                  className="hover:text-orange-400 transition-colors duration-300 text-left"
                >
                  Bows
                </button>
              </li>
              <li>
                <button
                  onClick={() => handleNavigate("tree-toppers")}
                  className="hover:text-orange-400 transition-colors duration-300 text-left"
                >
                  Tree Toppers
                </button>
              </li>
              <li>
                <button
                  onClick={() => handleNavigate("tree-skirts")}
                  className="hover:text-orange-400 transition-colors duration-300 text-left"
                >
                  Tree Skirts
                </button>
              </li>
              <li>
                <button
                  onClick={() => handleNavigate("spool")}
                  className="hover:text-orange-400 transition-colors duration-300 text-left"
                >
                  Spool
                </button>
              </li>
            </ul>
          </div>

          {/* Newsletter */}
          <div>
            <h4 className="text-white mb-4">Stay Updated</h4>
            <p className="text-sm mb-4">
              Subscribe for new collection updates and design inspiration
            </p>
            <div className="flex gap-2">
              <input
                type="email"
                placeholder="Your email"
                className="flex-1 px-3 py-2 bg-stone-800 border border-stone-700 rounded-lg focus:outline-none focus:ring-2 focus:ring-orange-600 text-sm"
              />
              <button className="px-4 py-2 bg-orange-600 text-white rounded-lg hover:bg-orange-700 transition-colors duration-300 text-sm">
                Subscribe
              </button>
            </div>
          </div>
        </div>

        {/* Bottom Bar */}
        <div className="border-t border-stone-800 pt-8 flex flex-col md:flex-row justify-between items-center text-sm">
          <p>
            &copy; {new Date().getFullYear()} Suorya. All rights reserved.
          </p>
          <div className="flex gap-6 mt-4 md:mt-0">
            <a
              href="#privacy"
              className="hover:text-orange-400 transition-colors duration-300"
            >
              Privacy Policy
            </a>
            <a
              href="#terms"
              className="hover:text-orange-400 transition-colors duration-300"
            >
              Terms of Service
            </a>
          </div>
        </div>
      </div>
    </footer>
  );
}