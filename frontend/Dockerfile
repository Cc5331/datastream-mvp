FROM node:20-alpine
WORKDIR /app
COPY package.json ./
COPY serve.js ./
COPY public ./public
ENV PORT=3000 BACKEND_HOST=backend BACKEND_PORT=8080
EXPOSE 3000
CMD ["node", "serve.js"]
