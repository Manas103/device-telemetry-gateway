# Container shape for the React dashboard (web/deliverydashboard), built as
# static assets and served by a tiny static file server. Authored, not
# built or run in this session (see README).
FROM node:20-bookworm AS build
WORKDIR /src
COPY web/deliverydashboard/package.json ./
RUN npm install
COPY web/deliverydashboard/ ./
RUN npm run build

FROM nginx:1.27-alpine
COPY --from=build /src/dist /usr/share/nginx/html
EXPOSE 80
