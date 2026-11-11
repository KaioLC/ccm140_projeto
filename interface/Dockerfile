FROM node:22-alpine

WORKDIR /app

COPY interface/package.json ./
RUN npm install

COPY contracts/chat.proto ./
COPY interface/server.ts ./

EXPOSE 8080

CMD ["npx", "tsx", "server.ts"]
