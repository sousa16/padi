# dida-trade dev/runtime image.
#
# maven:3.9-eclipse-temurin-22 is multi-arch, so this pulls arm64 on Apple
# Silicon and amd64 on Linux and runs native on both -- no emulation. The
# protoc and grpc-java plugin binaries are selected at build time by
# os-maven-plugin via ${os.detected.classifier} (src/contract/pom.xml), which
# is why one pom now works on every platform.
FROM maven:3.9-eclipse-temurin-22

# procps: the replica launcher tracks background pids.
RUN apt-get update \
 && apt-get install -y --no-install-recommends procps \
 && rm -rf /var/lib/apt/lists/*

# Source is bind-mounted here at runtime; ~/.m2 is a named volume, kept
# separate from the host repository so host-built and container-built
# platform-specific protoc binaries never mix.
WORKDIR /work/src

CMD ["bash"]
