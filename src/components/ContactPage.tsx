import { Mail, MapPin, Phone, ArrowLeft } from "lucide-react";
import { useState } from "react";
import { Button } from "./ui/button";
import { Link } from "react-router-dom";

interface ContactPageProps {
  onBack: () => void;
}

interface FormData {
  name: string;
  email: string;
  phone: string;
  inquiry: string;
  message: string;
}

export function ContactPage({ onBack }: ContactPageProps) {
  const [formData, setFormData] = useState<FormData>({
    name: "",
    email: "",
    phone: "",
    inquiry: "General Inquiry",
    message: "",
  });

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();

    // Email addresses to send the form to
    const emailAddresses = [
      "email1@example.com", // Replace with actual email
      "email2@example.com", // Replace with actual email
      "email3@example.com", // Replace with actual email
    ];

    // Create mailto link with all recipients
    const subject = encodeURIComponent(`Suorya Contact Form - ${formData.inquiry}`);
    const body = encodeURIComponent(
      `Name: ${formData.name}\nEmail: ${formData.email}\nPhone: ${formData.phone}\nInquiry Type: ${formData.inquiry}\n\nMessage:\n${formData.message}`
    );

    // Open mailto for all recipients
    window.location.href = `mailto:${emailAddresses.join(",")}?subject=${subject}&body=${body}`;

    // Reset form
    setFormData({
      name: "",
      email: "",
      phone: "",
      inquiry: "General Inquiry",
      message: "",
    });
  };

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
        <div className="text-center mb-16 space-y-4">
          <p className="text-orange-600 tracking-widest uppercase text-sm">
            Get In Touch
          </p>
          <h1 className="text-4xl md:text-5xl text-stone-800">Contact Us</h1>
          <p className="text-lg text-stone-600 max-w-2xl mx-auto">
            We'd love to hear from you. Reach out for inquiries, wholesale
            orders, or any questions about our products.
          </p>
        </div>

        <div className="grid lg:grid-cols-2 gap-12 mb-16">
          {/* Contact Information */}
          <div className="space-y-8">
            <div>
              <h2 className="text-2xl text-stone-800 mb-6">
                Visit Our Design Studio
              </h2>
              <div className="space-y-6">
                <div className="flex items-start space-x-4">
                  <div className="w-12 h-12 bg-orange-100 rounded-full flex items-center justify-center flex-shrink-0">
                    <MapPin className="w-5 h-5 text-orange-600" />
                  </div>
                  <div>
                    <h3 className="text-lg text-stone-800 mb-1">Location</h3>
                    <p className="text-stone-600">
                      Visit our studio to explore our complete collection
                      <br />
                      <span className="text-sm">By appointment only</span>
                    </p>
                  </div>
                </div>

                <div className="flex items-start space-x-4">
                  <div className="w-12 h-12 bg-orange-100 rounded-full flex items-center justify-center flex-shrink-0">
                    <Mail className="w-5 h-5 text-orange-600" />
                  </div>
                  <div>
                    <h3 className="text-lg text-stone-800 mb-1">Email Us</h3>
                    <a
                      href="mailto:hello@suorya.com"
                      className="text-orange-600 hover:text-orange-700 transition-colors duration-300"
                    >
                      hello@suorya.com
                    </a>
                    <p className="text-sm text-stone-600 mt-1">
                      We respond within 24 hours
                    </p>
                  </div>
                </div>

                <div className="flex items-start space-x-4">
                  <div className="w-12 h-12 bg-orange-100 rounded-full flex items-center justify-center flex-shrink-0">
                    <Phone className="w-5 h-5 text-orange-600" />
                  </div>
                  <div>
                    <h3 className="text-lg text-stone-800 mb-1">Call Us</h3>
                    <a
                      href="tel:+1234567890"
                      className="text-orange-600 hover:text-orange-700 transition-colors duration-300"
                    >
                      +1 (234) 567-8900
                    </a>
                    <p className="text-sm text-stone-600 mt-1">
                      Mon-Fri, 9am-6pm EST
                    </p>
                  </div>
                </div>
              </div>
            </div>

            <div className="pt-8 border-t border-stone-200">
              <h3 className="text-lg text-stone-800 mb-3">
                For Trade & Wholesale Inquiries
              </h3>
              <p className="text-stone-600">
                We work with boutiques, florists, event planners, and interior
                designers worldwide. Contact us for wholesale pricing and custom
                orders.
              </p>
            </div>
          </div>

          {/* Contact Form */}
          <div className="bg-stone-50 rounded-2xl p-8 lg:p-12">
            <h2 className="text-2xl text-stone-800 mb-6">Send Us a Message</h2>

            <form onSubmit={handleSubmit} className="space-y-6">
              <div>
                <label htmlFor="name" className="block text-stone-700 mb-2">
                  Name *
                </label>
                <input
                  type="text"
                  id="name"
                  required
                  value={formData.name}
                  onChange={(e) =>
                    setFormData({ ...formData, name: e.target.value })
                  }
                  className="w-full px-4 py-3 bg-white border border-stone-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-orange-600 focus:border-transparent transition-all duration-300"
                  placeholder="Your name"
                />
              </div>

              <div>
                <label htmlFor="email" className="block text-stone-700 mb-2">
                  Email *
                </label>
                <input
                  type="email"
                  id="email"
                  required
                  value={formData.email}
                  onChange={(e) =>
                    setFormData({ ...formData, email: e.target.value })
                  }
                  className="w-full px-4 py-3 bg-white border border-stone-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-orange-600 focus:border-transparent transition-all duration-300"
                  placeholder="your@email.com"
                />
              </div>

              <div>
                <label htmlFor="phone" className="block text-stone-700 mb-2">
                  Phone (Optional)
                </label>
                <input
                  type="tel"
                  id="phone"
                  value={formData.phone}
                  onChange={(e) =>
                    setFormData({ ...formData, phone: e.target.value })
                  }
                  className="w-full px-4 py-3 bg-white border border-stone-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-orange-600 focus:border-transparent transition-all duration-300"
                  placeholder="+1 (234) 567-8900"
                />
              </div>

              <div>
                <label htmlFor="inquiry" className="block text-stone-700 mb-2">
                  Type of Inquiry
                </label>
                <select
                  id="inquiry"
                  value={formData.inquiry}
                  onChange={(e) =>
                    setFormData({ ...formData, inquiry: e.target.value })
                  }
                  className="w-full px-4 py-3 bg-white border border-stone-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-orange-600 focus:border-transparent transition-all duration-300"
                >
                  <option>General Inquiry</option>
                  <option>Wholesale/Trade</option>
                  <option>Custom Order</option>
                  <option>Event/Wedding</option>
                  <option>Product Information</option>
                </select>
              </div>

              <div>
                <label htmlFor="message" className="block text-stone-700 mb-2">
                  Message *
                </label>
                <textarea
                  id="message"
                  rows={5}
                  required
                  value={formData.message}
                  onChange={(e) =>
                    setFormData({ ...formData, message: e.target.value })
                  }
                  className="w-full px-4 py-3 bg-white border border-stone-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-orange-600 focus:border-transparent transition-all duration-300 resize-none"
                  placeholder="Tell us about your project..."
                ></textarea>
              </div>

              <button
                type="submit"
                className="w-full px-8 py-4 bg-orange-600 text-white rounded-full hover:bg-orange-700 transition-colors duration-300 shadow-lg hover:shadow-xl"
              >
                Send Message
              </button>
            </form>
          </div>
        </div>

        
        <div className="mb-16">
          <h2 className="text-2xl text-stone-800 mb-6 text-center">
            Find Our Studio
          </h2>
          <div className="w-full h-[450px] rounded-2xl overflow-hidden shadow-lg border border-stone-200">
            
            <iframe src="https://www.google.com/maps/embed?pb=!1m18!1m12!1m3!1d3502.3390108307194!2d77.38283687475115!3d28.619599984669087!2m3!1f0!2f0!3f0!3m2!1i1024!2i768!4f13.1!3m3!1m2!1s0x390ceff39a35ee69%3A0x954d9b1c21dcc7b4!2sSuorya%20Exports%20Imports%20Enterprises!5e0!3m2!1sen!2sin!4v1759751159815!5m2!1sen!2sin" width="100%" height="600"  loading="lazy" ></iframe>

          </div>
          
        </div>
      </div>
    </div>
  );
}