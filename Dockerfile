# fake-tickets v2 — one image, two roles:  `faketickets backend` | `faketickets frontend`
#
#   podman build -t fake-tickets:2.0.0 .            (build context = repo root)
#   podman build --target test -t fake-tickets:test .   (runs the unit test-suite in the image)
#
# Base images follow the homelab convention (UBI 9). Build stage compiles the venv
# with the full python-312 image; runtime is ubi-minimal + the python3.12 RPM.

ARG BUILD_IMAGE=registry.access.redhat.com/ubi9/python-312:latest
ARG RUNTIME_IMAGE=registry.access.redhat.com/ubi9/ubi-minimal:latest

# --------------------------------------------------------------------------- build
FROM ${BUILD_IMAGE} AS build
USER 0
WORKDIR /src
RUN /usr/bin/python3.12 -m venv /opt/venv \
 && /opt/venv/bin/pip install --no-cache-dir --upgrade pip setuptools wheel
# dependency layer first (better cache reuse)
COPY pyproject.toml README.md ./
RUN mkdir faketickets && touch faketickets/__init__.py \
 && /opt/venv/bin/pip install --no-cache-dir . dumb-init==1.2.5.post1 \
 && /opt/venv/bin/pip uninstall -y fake-tickets
COPY faketickets ./faketickets
# rm build/: setuptools would otherwise reuse the placeholder package from the dependency layer
RUN rm -rf build *.egg-info \
 && /opt/venv/bin/pip install --no-cache-dir --no-deps . \
 && rm -rf build *.egg-info \
 && cd / && /opt/venv/bin/faketickets workloads > /dev/null \
 && /opt/venv/bin/python -c "import faketickets.backend.app, faketickets.frontend.app, cassandra.io.libevreactor"

# --------------------------------------------------------------------------- test (optional target)
FROM build AS test
COPY tests ./tests
# run against the *installed* package (not /src) so packaging mistakes fail the build
RUN /opt/venv/bin/pip install --no-cache-dir pytest \
 && mkdir /t && cp -r tests /t/ && rm -rf /src/faketickets \
 && cd /t && /opt/venv/bin/python -m pytest -q -p no:cacheprovider tests

# --------------------------------------------------------------------------- runtime
FROM ${RUNTIME_IMAGE} AS runtime
ARG VERSION=2.0.0
LABEL org.opencontainers.image.title="fake-tickets" \
      org.opencontainers.image.description="Kubernetes-friendly Cassandra / DSE / Astra data simulator" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.licenses="Proprietary"
RUN microdnf install -y --nodocs --setopt=install_weak_deps=0 python3.12 shadow-utils \
 && microdnf clean all \
 && useradd --uid 10001 --gid 0 --home-dir /data --no-create-home --shell /sbin/nologin faketickets \
 && mkdir -p /data && chown 10001:0 /data && chmod 0770 /data \
 && microdnf remove -y shadow-utils && microdnf clean all
COPY --from=build /opt/venv /opt/venv
ENV PATH=/opt/venv/bin:$PATH \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    FT_DATA_DIR=/data \
    TMPDIR=/tmp
USER 10001
WORKDIR /data
VOLUME ["/data"]
EXPOSE 8443 8444
# dumb-init is PID 1 and reaps orphans. --single-child: SIGTERM goes to the server only,
# which then stops its worker processes itself (so runs are recorded as "interrupted").
ENTRYPOINT ["/opt/venv/bin/dumb-init", "--single-child", "--", "faketickets"]
CMD ["backend"]
