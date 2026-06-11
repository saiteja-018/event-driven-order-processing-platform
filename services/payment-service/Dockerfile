FROM node:20-alpine
WORKDIR /usr/src/app
COPY services/payment-service/package*.json ./
RUN npm install
COPY services/payment-service/tsconfig.json ./
COPY services/payment-service/src ./src
RUN npm run build
CMD ["node", "dist/index.js"]
