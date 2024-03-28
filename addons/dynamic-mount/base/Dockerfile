FROM alpine:3.18
RUN sed -i 's/dl-cdn.alpinelinux.org/mirrors.aliyun.com/' /etc/apk/repositories
RUN apk --update add fuse python3 inotify-tools bash coreutils tini supervisor && rm -rf /var/cache/apk/* 

COPY inotify-fluid-config.ini /tmp/inotify-fluid-config.ini
RUN cat /tmp/inotify-fluid-config.ini >> /etc/supervisord.conf && rm /tmp/inotify-fluid-config.ini

COPY reconcile_mount_program_settings.py mount-helper.sh inotify.sh prestop.sh entrypoint.sh /usr/local/bin/
RUN chmod u+x /usr/local/bin/mount-helper.sh /usr/local/bin/inotify.sh /usr/local/bin/prestop.sh /usr/local/bin/entrypoint.sh

ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]