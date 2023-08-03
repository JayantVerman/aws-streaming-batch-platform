docker run --rm -p 8501:8501 \
  -e DB_URL="postgresql://platform:platform@host.docker.internal:5432/warehouse" \
  -v "$PWD/viz/streamlit_app:/app" \
  python:3.11-slim \
  sh -c "pip install -q --no-cache-dir 'streamlit==1.29.0' 'pandas==2.0.3' 'SQLAlchemy==2.0.17' 'psycopg2-binary==2.9.6' && streamlit run /app/app.py"