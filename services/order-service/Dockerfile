FROM node:20-alpine
WORKDIR /usr/src/app
COPY services/order-service/package*.json ./
RUN npm install
COPY services/order-service/tsconfig.json ./
COPY services/order-service/src ./src
RUN npm run build
COPY shared/schemas ./shared/schemas
CMD ["node", "dist/index.js"]
