/** @type {import('tailwindcss').Config} */
export default {
  content: ['./index.html', './src/**/*.{js,jsx}'],
  theme: {
    extend: {
      colors: {
        'telethon-blue':      '#0057A8',
        'telethon-blue-dark': '#003D7A',
        'telethon-red':       '#D81E1E',
        'telethon-lightblue': '#E8F0FA',
      },
      fontFamily: {
        sans: ['Outfit', 'DM Sans', 'system-ui', 'sans-serif'],
      },
      borderRadius: {
        '2xl': '1rem',
        '3xl': '1.5rem',
      },
      boxShadow: {
        'card': '0 1px 3px rgba(0,0,0,0.06), 0 1px 2px rgba(0,0,0,0.04)',
        'card-lg': '0 4px 20px rgba(0,0,0,0.06)',
      },
    },
  },
  plugins: [],
}
