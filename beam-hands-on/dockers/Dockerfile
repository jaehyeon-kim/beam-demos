FROM jupyter/scipy-notebook:python-3.10

ARG BEAM_VERSION
ENV BEAM_VERSION=${PYTHON_VERSION:-2.53.0}

USER root

## install utils - graphviz, gcloud, azure cli, awscli 
RUN apt-get update \
  && apt-get install -y apt-transport-https ca-certificates gnupg curl sudo \
  && curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | sudo gpg --dearmor -o /usr/share/keyrings/cloud.google.gpg \
  && echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" | sudo tee -a /etc/apt/sources.list.d/google-cloud-sdk.list \
  && apt-get update \
  && apt-get install -y graphviz google-cloud-cli \
  && curl -sL https://aka.ms/InstallAzureCLIDeb | bash \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/* \
  && curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip" \
  && unzip -qq awscliv2.zip \
  && ./aws/install \
  && rm -rf aws*

USER ${NB_UID}

## install apache-beam from pip - mamba doesn't support extra requirements
# upgrade pyopenssl to fix the following error due to dependency conflicts
#       AttributeError: module 'lib' has no attribute 'X509_V_FLAG_CB_ISSUER_CHECK'
# see
#   ERROR: pip's dependency resolver does not currently take into account all the packages that are installed. 
#   This behaviour is the source of the following dependency conflicts.
#   pyopenssl 22.1.0 requires cryptography<39,>=38.0.0, but you have cryptography 42.0.2 which is incompatible.
RUN pip install "apache-beam[gcp,aws,azure,test,docs,interactive]==${BEAM_VERSION}" \
  && pip install pyopenssl --upgrade
