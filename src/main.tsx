
  // import { createRoot } from "react-dom/client";
  // //@ts-ignore
  // import App from "./App.tsx";
  // //@ts-ignore
  // import "./index.css";

  // createRoot(document.getElementById("root")!).render(<App />);
  





import React from "react";
import { createRoot } from "react-dom/client";
import { BrowserRouter } from "react-router-dom";
// @ts-ignore
import App from "./App.tsx";
// @ts-ignore
import "./index.css";

createRoot(document.getElementById("root")!).render(
  <React.StrictMode>
    <BrowserRouter>
      <App />
    </BrowserRouter>
  </React.StrictMode>
);
