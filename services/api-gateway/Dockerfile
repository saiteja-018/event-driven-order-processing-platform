FROM node:20-alpine
WORKDIR /usr/src/app
COPY services/api-gateway/package*.json ./
RUN npm install
COPY services/api-gateway/tsconfig.json ./
COPY services/api-gateway/src ./src
RUN npm run build
CMD ["node", "dist/index.js"]
