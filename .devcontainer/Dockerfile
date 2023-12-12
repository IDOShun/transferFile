FROM --platform=linux/arm64/v8 debian:bullseye-slim

ARG USERNAME=vscode
ARG GROUPNAME=developer
ARG UID=1000
ARG GID=1000

RUN apt-get update \
&& apt-get install python3 -y \
&& apt-get install python3-pip -y
RUN groupadd -g $GID $GROUPNAME && \
    useradd -m -s /bin/bash -u $UID -g $GID $USERNAME

USER $USERNAME

WORKDIR /home/vscode