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
  apt-get upgrade -y && \
  apt-get install curl -y && \
  curl -fsSL https://deb.nodesource.com/setup_16.x -o nodesource_setup.sh | sed -e 's/sudo//g' | bash && \
  apt-get update && \
  apt-get install -yqq nodejs && \
  pip install -r requirements.txt && \
  python setup.py install

RUN addgroup --gid 1001 --system stk && \
  adduser --no-create-home --shell /bin/false --disabled-password --uid 1001 --system --group stk

RUN chown -R stk:stk /src
RUN chown -R stk:stk /usr/local/lib/python3.9/site-packages/plugin-*.egg/
USER stk

ENTRYPOINT ["data-pipeline"]
