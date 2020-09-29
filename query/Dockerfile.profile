FROM {{ service_base_image.image }}

RUN wget http://insightfullogic.com/honest-profiler.zip && \
  unzip honest-profiler.zip -d /honest-profiler && \
  wget https://download2.gluonhq.com/openjfx/11.0.2/openjfx-11.0.2_linux-x64_bin-sdk.zip && \
  unzip openjfx-11.0.2_linux-x64_bin-sdk.zip -d /openjfx

COPY query/hail.jar /

EXPOSE 5000
