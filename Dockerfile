FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt .
COPY packages/ ./packages/
RUN pip install --no-index --find-links=packages/ -r requirements.txt



COPY . .

CMD ["bash"]
