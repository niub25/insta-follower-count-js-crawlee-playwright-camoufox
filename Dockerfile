# Apify base image with Camoufox (stealthy Firefox fork) pre-installed
FROM apify/actor-node-playwright-camoufox:24

COPY package*.json ./

RUN npm --quiet set progress=false \
    && npm install --only=prod

COPY . ./

CMD npm start --silent
