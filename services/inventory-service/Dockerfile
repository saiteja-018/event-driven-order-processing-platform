FROM node:20-alpine
WORKDIR /usr/src/app
COPY services/inventory-service/package*.json ./
RUN npm install
COPY services/inventory-service/tsconfig.json ./
COPY services/inventory-service/src ./src
RUN npm run build
COPY shared/schemas ./shared/schemas
CMD ["node", "dist/index.js"]
