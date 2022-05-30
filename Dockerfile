FROM node:18-alpine AS builder

WORKDIR /build

COPY ./package.json ./package-lock.json ./index.ts ./tsconfig.json /build/
RUN CI=true npm install

FROM node:18-alpine

WORKDIR /app

# COPY --from=builder /build ./
COPY --from=builder /build/index.ts /build/package.json /build/package-lock.json /app/
RUN CI=true npm install --omit=dev && npm cache clean --force

ENV PORT=3100

EXPOSE $PORT
CMD ["npx", "ts-node", "-T", "index.ts"]