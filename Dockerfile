FROM python:3.10-slim
# Copy local code to the container image.
ENV APP_HOME /app
WORKDIR $APP_HOME
COPY . ./

RUN apt update -y && apt install -y \
    libgl1-mesa-glx \
    libglib2.0-dev \
    ffmpeg  \
    libsm6  \
    libxext6 

# Install production dependencies.
RUN pip install --upgrade pip && pip install -r requirements.txt

CMD python job.py