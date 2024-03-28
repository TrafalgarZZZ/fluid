FROM alpine:3.18 AS builder

ENV OSSFS_VERSION 1.91.1
RUN sed -i 's/dl-cdn.alpinelinux.org/mirrors.aliyun.com/' /etc/apk/repositories
RUN apk --update add fuse alpine-sdk automake autoconf libxml2-dev fuse-dev curl-dev pkgconf
RUN wget -qO- https://github.com/aliyun/ossfs/archive/v$OSSFS_VERSION.tar.gz |tar xz
RUN cd ossfs-1.91.1 \
  && ./autogen.sh \
  && ./configure --prefix=/usr \
  && make \
  && make install

FROM fluidcloudnative/dynamic-mount:base
RUN sed -i 's/dl-cdn.alpinelinux.org/mirrors.aliyun.com/' /etc/apk/repositories
RUN apk --update add jq curl libxml2
COPY --from=builder /usr/bin/ossfs /usr/bin/ossfs
COPY mount.sh /opt/mount.sh
RUN chmod u+x /opt/mount.sh
