FROM python:3.12.2-slim-bullseye

WORKDIR /app

COPY app /app

RUN pip3 install -r requirements.txt

EXPOSE 8501

ENTRYPOINT ["streamlit", "run", "streamlit_app.py", "--server.port=8501", "--server.address=0.0.0.0"]