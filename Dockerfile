FROM clux/muslrust:stable AS chef
USER root
RUN cargo install cargo-chef
WORKDIR /app
FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json


FROM chef AS server_builder
COPY --from=planner /app/recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json
COPY . .
RUN cargo build --release --bin server --features FRTB_CRR2 --target x86_64-unknown-linux-musl


FROM node:16 as frontend_builder
WORKDIR /frontend
RUN apt-get update && apt-get install -y nodejs
COPY frontend/package.json .
COPY frontend/package-lock.json .
RUN npm install
COPY frontend/ .
RUN npm run build


FROM alpine AS runtime
RUN addgroup -S ultima && adduser -S ultima -G ultima
COPY --from=server_builder /app/target/x86_64-unknown-linux-musl/release/server /usr/local/bin/
COPY --from=frontend_builder /frontend/dist /var/frontend
ENV STATIC_FILES_DIR /var/frontend
USER ultima
EXPOSE 8000
CMD ["/usr/local/bin/server"]
