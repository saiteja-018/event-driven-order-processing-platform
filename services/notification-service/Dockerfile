FROM node:20-alpine
WORKDIR /usr/src/app
COPY services/notification-service/package*.json ./
RUN npm install
COPY services/notification-service/tsconfig.json ./
COPY services/notification-service/src ./src
RUN npm run build
CMD ["node", "dist/index.js"]
