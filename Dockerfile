FROM oven/bun:1 AS build
WORKDIR /app
COPY package.json ./
RUN bun install
COPY . .
RUN bun run build

FROM oven/bun:1-slim
WORKDIR /app
ENV NODE_ENV=production
COPY --from=build /app/package.json ./package.json
COPY --from=build /app/node_modules ./node_modules
COPY --from=build /app/dist ./dist
COPY --from=build /app/mikro-orm.config.ts ./mikro-orm.config.ts
CMD ["bun", "dist/src/main.js"]
