FROM python:3.12-slim@sha256:7026274c107626d7e940e0e5d6730481a4600ae95d5ca7eb532dd4180313fea9

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["python", "main.py"]
