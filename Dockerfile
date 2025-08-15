FROM python:3.11-slim

# Install system deps
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl supervisor \
 && rm -rf /var/lib/apt/lists/*

# Install ngrok
RUN curl -sSL https://bin.equinox.io/c/4VmDzA7iaHb/ngrok-stable-linux-amd64.tgz \
 | tar -zx -C /usr/local/bin \
 && chmod +x /usr/local/bin/ngrok

# Set workdir
WORKDIR /app

# Install Python deps
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy app code
COPY . .

# Copy supervisord config
COPY supervisord.conf /etc/supervisor/conf.d/supervisord.conf

EXPOSE 8000 

# Add start script
COPY start.sh /start.sh
RUN chmod +x /start.sh

# Run start script instead of supervisord directly
CMD ["/start.sh"]
