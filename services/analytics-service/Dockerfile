FROM node:20-alpine
WORKDIR /usr/src/app
COPY services/analytics-service/package*.json ./
RUN npm install
COPY services/analytics-service/tsconfig.json ./
COPY services/analytics-service/src ./src
RUN npm run build
CMD ["node", "dist/index.js"]
