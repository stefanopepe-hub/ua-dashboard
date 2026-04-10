/** @type {import('tailwindcss').Config} */
export default {
  content: ['./index.html','./src/**/*.{js,jsx}'],
  theme: {
    extend: {
      colors: {
        'telethon-blue':      '#0057A8',
        'telethon-red':       '#D81E1E',
        'telethon-lightblue': '#E8F0FA',
      },
    },
  },
  plugins: [],
}
