FROM node:22-alpine
WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci --omit=dev --ignore-scripts && npm cache clean --force
COPY --chown=node:node server.mjs kinds.json ./
ENV NODE_ENV=production
ENV PORT=8787
USER node
EXPOSE 8787
CMD ["node", "server.mjs"]
