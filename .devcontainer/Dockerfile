# See here for image contents: https://github.com/microsoft/vscode-dev-containers/tree/v0.234.0/containers/python-3/.devcontainer/base.Dockerfile

# [Choice] Python version (use -bullseye variants on local arm64/Apple Silicon): 3, 3.10, 3.9, 3.8, 3.7, 3.6, 3-bullseye, 3.10-bullseye, 3.9-bullseye, 3.8-bullseye, 3.7-bullseye, 3.6-bullseye, 3-buster, 3.10-buster, 3.9-buster, 3.8-buster, 3.7-buster, 3.6-buster
ARG VARIANT="3.10-bullseye"
FROM mcr.microsoft.com/vscode/devcontainers/python:0-${VARIANT}

# [Choice] Node.js version: none, lts/*, 16, 14, 12, 10
ARG NODE_VERSION="none"
RUN if [ "${NODE_VERSION}" != "none" ]; then su vscode -c "umask 0002 && . /usr/local/share/nvm/nvm.sh && nvm install ${NODE_VERSION} 2>&1"; fi

# [Optional] If your pip requirements rarely change, uncomment this section to add them to the image.
# COPY requirements.txt /tmp/pip-tmp/
# RUN pip3 --disable-pip-version-check --no-cache-dir install -r /tmp/pip-tmp/requirements.txt \
#    && rm -rf /tmp/pip-tmp

# [Optional] Uncomment this section to install additional OS packages.
# RUN apt-get update && export DEBIAN_FRONTEND=noninteractive \
#     && apt-get -y install --no-install-recommends <your-package-list-here>

# [Optional] Uncomment this line to install global node packages.
# RUN su vscode -c "source /usr/local/share/nvm/nvm.sh && npm install -g <your-package-here>" 2>&1

# Pyspark braucht Java 8+ JVM
# das ist so mit die "kleinste"
ENV JAVA_HOME=/opt/java/openjdk-8
COPY --from=openjdk:8u332-jre /usr/local/openjdk-8 $JAVA_HOME
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Diese Java Abhängigkeiten sind so groß dass ich die ins Image mit reinpacke.
# Normalerweise werden alle Abhängigkeiten in der requirements.txt
# runtergeladen nachdem der Container gestartet ist.
# Man müsste also jedes mal beim Neustart des Containers wieder 200MB+
# Pyspark runterladen. -> Lieber 1x im Image und dann hat man es jedesmal
RUN pip3 install findspark
RUN pip3 install pyspark

# Diese Einstellung verhindert dass das komplette Datenset runtergeladen wird
ENV TEST_ENVIRONMENT=TRUE

# PySpark beim geladen werden dazu überreden auch GraphFrames mitzuladen
# geht leider nicht über pip weil sowohl java als auch python packages installiert werden
# nur direkt über spark
ENV PYSPARK_SUBMIT_ARGS='--packages graphframes:graphframes:0.8.2-spark3.2-s_2.12 pyspark-shell'