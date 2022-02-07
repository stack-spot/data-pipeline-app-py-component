FROM python:3.9-slim

WORKDIR /src

USER root

COPY plugin/ plugin/ 
COPY requirements.txt .
COPY setup.py .
COPY setup.cfg .

ENV PYTHONPATH=/src

RUN \
  apt-get update && \
  apt-get install gnupg wget -y && \
  echo "deb https://deb.nodesource.com/node_14.x buster main" > /etc/apt/sources.list.d/nodesource.list && \
  wget -qO- https://deb.nodesource.com/gpgkey/nodesource.gpg.key | apt-key add - && \
  apt-get update && \
  apt-get install -yqq nodejs && \
  pip install -r requirements.txt && \
  python setup.py install

RUN addgroup --gid 1001 --system orange && \
  adduser --no-create-home --shell /bin/false --disabled-password --uid 1001 --system --group orange

RUN chown -R orange:orange /src
RUN chown -R orange:orange /usr/local/lib/python3.9/site-packages/plugin-*.egg/
USER orange

ENTRYPOINT ["data-pipeline"]
