FROM python:3.10-slim
WORKDIR /app
COPY serving/requirements.txt /app/
RUN pip install -r requirements.txt
COPY serving/ /app/
EXPOSE 8000
CMD ["uvicorn","app:app","--host","0.0.0.0","--port","8000"]