/** @type {import('tailwindcss').Config} */
export default {
  content: ["./index.html", "./src/**/*.{js,ts,jsx,tsx}"],
  theme: {
    extend: {
      colors: {
        brand: {
          DEFAULT: "#0e8136",
          light: "#17a84a",
          dark: "#0a6128",
        },
      },
    },
  },
  plugins: [],
}

