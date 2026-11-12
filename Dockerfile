FROM node:22-alpine
WORKDIR /app
COPY package*.json ./
RUN npm ci --only=production || npm install --omit=dev
COPY tsconfig.json ./
COPY src ./src
RUN npm install -D typescript @types/node && npm run build && npm prune --omit=dev
EXPOSE 3000
CMD ["node", "dist/main.js"]
