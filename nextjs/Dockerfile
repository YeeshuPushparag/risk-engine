# Build stage
FROM node:22-alpine AS builder
WORKDIR /app

COPY package*.json ./
RUN npm ci          # better than npm install for Docker

COPY . .
RUN npm run build

# Run stage
FROM node:22-alpine
WORKDIR /app
ENV NODE_ENV=production

# copy build artifacts
COPY --from=builder /app/.next ./.next
COPY --from=builder /app/public ./public
COPY --from=builder /app/package*.json ./
COPY --from=builder /app/node_modules ./node_modules

EXPOSE 3000
CMD ["npm","run","start"]
