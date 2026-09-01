# Analysis + offline-training image.
#
# Python rather than Rust for the statistics: every routine in graze_analysis/stats.py replaces a
# hand-rolled version that produced a wrong answer, and statsmodels/scipy provide reference
# implementations to check against. The same image later hosts the stage 4 similarity job and the
# stage 6 two-tower training, so this is one image rather than three.
FROM --platform=linux/amd64 python:3.12-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential \
    && rm -rf /var/lib/apt/lists/*

# Install from the LOCK, not requirements.txt: the lock carries transitive pins too, so a rebuild
# resolves to the same set the statistics were validated against rather than whatever is current.
COPY requirements.txt requirements.lock.txt ./
RUN pip install --no-cache-dir -r requirements.lock.txt

COPY graze_analysis ./graze_analysis
COPY experiments ./experiments
COPY tests ./tests

ENV PYTHONUNBUFFERED=1
ENTRYPOINT ["python", "-m", "graze_analysis.runner"]
