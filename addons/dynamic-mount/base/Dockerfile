FROM debian:bullseye-slim@sha256:a165446a88794db4fec31e35e9441433f9552ae048fb1ed26df352d2b537cb96

RUN apt update && apt install -y python3 fuse tini supervisor inotify-tools jq && rm -rf /var/cache/apt/* && ln -s /usr/bin/python3 /usr/local/bin/python
COPY inotify-fluid-config.ini /tmp/inotify-fluid-config.ini
RUN cat /tmp/inotify-fluid-config.ini >> /etc/supervisor/supervisord.conf && rm /tmp/inotify-fluid-config.ini

COPY reconcile_mount_program_settings.py mount-helper.sh inotify.sh prestop.sh entrypoint.sh /usr/local/bin/
RUN chmod u+x /usr/local/bin/mount-helper.sh /usr/local/bin/inotify.sh /usr/local/bin/prestop.sh /usr/local/bin/entrypoint.sh

ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]