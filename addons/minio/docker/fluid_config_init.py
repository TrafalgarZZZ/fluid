import json

with open("/etc/fluid/config.json", "r") as f:
    lines = f.readlines()

rawStr = lines[0]
print(rawStr)


script = """
#!/bin/sh
set -ex
export AWS_ACCESS_KEY_ID=$akId
export AWS_SECRET_ACCESS_KEY=$akSecret

mkdir -p $targetPath

exec goofys -f --endpoint "$url" "$bucket" $targetPath
"""

obj = json.loads(rawStr)

with open("mount-minio.sh", "w") as f:
    f.write("targetPath=\"%s\"\n" % obj['targetPath'])
    f.write("url=\"%s\"\n" % obj['mounts'][0]['options']['minio-url'])
    if obj['mounts'][0]['mountPoint'].startswith("minio://"):
      f.write("bucket=\"%s\"\n" % obj['mounts'][0]['mountPoint'][len("minio://"):])
    else:
      f.write("bucket=\"%s\"\n" % obj['mounts'][0]['mountPoint'])
    f.write("akId=\"%s\"\n" % obj['mounts'][0]['options']['minio-access-key'])
    f.write("akSecret=\"%s\"\n" % obj['mounts'][0]['options']['minio-access-secret'])

    f.write(script)
