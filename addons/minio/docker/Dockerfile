FROM cloudposse/goofys

RUN apk add python3 bash

ADD ./fluid_config_init.py /
ADD ./entrypoint.sh /usr/local/bin

RUN chmod u+x /usr/local/bin/entrypoint.sh

CMD [ "/usr/local/bin/entrypoint.sh" ]