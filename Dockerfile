FROM node:22-alpine

WORKDIR /app

COPY package*.json ./
RUN npm ci --omit=dev && apk add --no-cache wget

COPY src/ ./src/
# Data dir for Zalo credentials/session — mount as volume in production
RUN mkdir -p /data && ln -sf /data /root/.zalo-agent-cli

EXPOSE 3000

ENV NODE_ENV=production
ENV ZALO_JSON_MODE=1

CMD ["node", "src/index.js", "serve", "--port", "3000", "--host", "0.0.0.0"]
